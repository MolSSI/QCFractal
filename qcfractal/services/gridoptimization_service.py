"""
Wraps geometric procedures
"""

import json
from typing import Dict, Set

import numpy as np

from ..extras import get_information
from ..interface.models import GridOptimizationRecord, Molecule
from .service_util import BaseService, expand_ndimensional_grid

__all__ = ["GridOptimizationService"]


class GridOptimizationService(BaseService):

    # Index info
    service: str = "gridoptimization"
    program: str = "qcfractal"
    procedure: str = "gridoptimization"

    # Program info
    optimization_program: str

    # Output
    output: GridOptimizationRecord

    # Temporaries
    grid_optimizations: Dict[str, str] = {}
    seeds: Set[tuple] = set()
    complete: Set[tuple] = set()
    dimensions: tuple
    iteration: int
    starting_grid: tuple
    final_energies = {}

    # Task helpers
    task_map: Dict[str, str] = {}

    # Templates
    constraint_template: str
    optimization_template: str
    # keyword_template: KeywordSet
    starting_molecule: Molecule

    @classmethod
    def initialize_from_api(cls, storage_socket, logger, service_input, tag=None, priority=None):

        # Build the record
        output = GridOptimizationRecord(
            **service_input.dict(exclude={"initial_molecule"}),
            initial_molecule=service_input.initial_molecule.id,
            starting_molecule=service_input.initial_molecule.id,
            provenance={
                "creator": "qcfractal",
                "version": get_information("version"),
                "routine": "qcfractal.services.gridoptimization",
            },
            final_energy_dict={},
            grid_optimizations={},
            starting_grid=[0],
        )

        meta = {"output": output}

        # Build dihedral template
        constraint_template = []
        for scan in output.keywords.scans:
            tmp = {"type": scan.type, "indices": scan.indices}
            constraint_template.append(tmp)

        meta["constraint_template"] = json.dumps(constraint_template)

        # Build optimization template
        opt_template = {
            "meta": {"procedure": "optimization", "qc_spec": output.qc_spec.dict(), "tag": meta.pop("tag", None)}
        }
        opt_template["meta"].update(output.optimization_spec.dict())
        meta["optimization_template"] = json.dumps(opt_template)

        # Move around geometric data
        meta["optimization_program"] = output.optimization_spec.program
        meta["hash_index"] = output.hash_index

        # Hard coded data, # TODO
        meta["dimensions"] = output.get_scan_dimensions()

        meta["starting_molecule"] = service_input.initial_molecule
        if output.keywords.preoptimization:
            meta["iteration"] = -2
            meta["starting_grid"] = (0 for x in meta["dimensions"])
        else:
            meta["iteration"] = 0
            meta["starting_grid"] = GridOptimizationService._calculate_starting_grid(
                output.keywords.scans, service_input.initial_molecule
            )

        meta["task_tag"] = tag
        meta["task_priority"] = priority
        return cls(**meta, storage_socket=storage_socket, logger=logger)

    @staticmethod
    def _calculate_starting_grid(scans, molecule):
        starting_grid = []
        for scan in scans:

            # Find closest index
            if scan.step_type == "absolute":
                m = molecule.measure(scan.indices)
            elif scan.step_type == "relative":
                m = 0
            else:
                raise KeyError("'step_type' of '{}' not understood.".format(scan.step_type))

            idx = np.abs(np.array(scan.steps) - m).argmin()
            starting_grid.append(int(idx))

        return tuple(starting_grid)

    def iterate(self):

        self.status = "RUNNING"

        # Special pre-optimization iteration
        if self.iteration == -2:
            packet = json.loads(self.optimization_template)
            packet["data"] = [self.output.initial_molecule]

            self.task_manager.submit_tasks("optimization", {"initial_opt": packet})
            self.grid_optimizations[self.output.serialize_key("preoptimization")] = self.task_manager.required_tasks[
                "initial_opt"
            ]

            self.update_output()  # normally handled by submit_optimization_tasks
            self.iteration = -1
            return False

        elif self.iteration == -1:
            if self.task_manager.done() is False:
                return False

            complete_tasks = self.task_manager.get_tasks()

            self.starting_molecule = self.storage_socket.get_molecules(
                id=[complete_tasks["initial_opt"]["final_molecule"]]
            )["data"][0]
            self.starting_grid = self._calculate_starting_grid(self.output.keywords.scans, self.starting_molecule)

            self.submit_optimization_tasks({self.output.serialize_key(self.starting_grid): self.starting_molecule.id})
            self.iteration = 1

            return False

        # Special start iteration
        elif self.iteration == 0:

            self.submit_optimization_tasks({self.output.serialize_key(self.starting_grid): self.starting_molecule.id})
            self.iteration = 1

            return False

        # Check if tasks are done
        if self.task_manager.done() is False:
            return False

        # Obtain complete tasks and figure out future tasks
        complete_tasks = self.task_manager.get_tasks()
        for k, v in complete_tasks.items():
            self.final_energies[k] = v["energies"][-1]
            self.grid_optimizations[k] = v["id"]

        # Build out nthe new set of seeds
        complete_seeds = set(tuple(json.loads(k)) for k in complete_tasks.keys())
        self.complete |= complete_seeds
        self.seeds = complete_seeds
        # print("Complete", self.complete)

        # Compute new points
        new_points_list = expand_ndimensional_grid(self.dimensions, self.seeds, self.complete)
        # print(new_points_list)

        # grid = np.zeros(self.dimensions, dtype=np.int)
        # for x in self.complete:
        #     grid[x] = 1
        # print(grid)

        next_tasks = {}
        for new_points in new_points_list:
            old = self.output.serialize_key(new_points[0])
            new = self.output.serialize_key(new_points[1])

            next_tasks[new] = complete_tasks[old]["final_molecule"]

        # All done
        if len(next_tasks) == 0:
            self.status = "COMPLETE"
            self.update_output()
            return True

        self.submit_optimization_tasks(next_tasks)

        return False

    def submit_optimization_tasks(self, task_dict):

        new_tasks = {}

        for key, mol in task_dict.items():

            # Update molecule
            packet = json.loads(self.optimization_template)

            # Construct constraints
            constraints = json.loads(self.constraint_template)

            scan_indices = self.output.deserialize_key(key)
            for con_num, scan in enumerate(self.output.keywords.scans):
                idx = scan_indices[con_num]
                if scan.step_type == "absolute":
                    constraints[con_num]["value"] = scan.steps[idx]
                else:
                    constraints[con_num]["value"] = scan.steps[idx] + self.starting_molecule.measure(scan.indices)

            packet["meta"]["keywords"].setdefault("constraints", {})
            packet["meta"]["keywords"]["constraints"].setdefault("set", [])
            packet["meta"]["keywords"]["constraints"]["set"].extend(constraints)

            # Build new molecule
            packet["data"] = [mol]

            new_tasks[key] = packet

        self.task_manager.submit_tasks("optimization", new_tasks)
        self.grid_optimizations.update(self.task_manager.required_tasks)

        self.update_output()

    def update_output(self):
        """
        Finishes adding data to the GridOptimizationRecord object
        """

        self.output = self.output.copy(
            update={
                "status": self.status,
                "starting_molecule": self.starting_molecule.id,
                "starting_grid": self.starting_grid,
                "grid_optimizations": self.grid_optimizations,
                "final_energy_dict": self.final_energies,
            }
        )

        return True
