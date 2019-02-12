"""
Wraps geometric procedures
"""

import copy
import json
from typing import Dict, Set

import numpy as np

from qcfractal.extras import get_information
from qcfractal.interface.models.common_models import json_encoders
from qcfractal.interface.models.gridoptimization import GridOptimization
from .service_util import BaseService, TaskManager, expand_ndimensional_grid

__all__ = ["GridOptimizationService"]


class GridOptimizationService(BaseService):

    # Index info
    status: str = "READY"
    service: str = "gridoptimization"
    program: str = "qcfractal"
    procedure: str = "gridoptimization"

    # Output
    output: GridOptimization

    # Temporaries
    grid_optimizations: Dict[str, str] = {}
    seeds: Set[tuple] = set()
    complete: Set[tuple] = set()
    dimensions: tuple
    iteration: int = 0
    starting_grid: tuple
    final_energies = {}

    # Task helpers
    task_map: Dict[str, str] = {}
    task_manager: TaskManager = TaskManager()

    # Templates
    constraint_template: str
    optimization_template: str
    molecule_template: str

    class Config:
        json_encoders = json_encoders

    @classmethod
    def initialize_from_api(cls, storage_socket, service_input):

        # Build the results object
        input_dict = service_input.dict()
        input_dict["initial_molecule"] = input_dict["initial_molecule"]["id"]

        output = GridOptimization(
            **input_dict,
            provenance={
                "creator": "qcfractal",
                "version": get_information("version"),
                "routine": "qcfractal.services.gridoptimization"
            },
            final_energy_dict={},
            grid_optimizations={},
            starting_grid=[0])

        meta = {"output": output}

        # Remove identity info from molecule template
        molecule_template = copy.deepcopy(service_input.initial_molecule.json_dict())
        molecule_template.pop("id", None)
        molecule_template.pop("identifiers", None)
        meta["molecule_template"] = json.dumps(molecule_template)

        # Build dihedral template
        constraint_template = []
        for scan in output.gridoptimization_meta.scans:
            tmp = {"type": scan.type, "indices": scan.indices}
            constraint_template.append(tmp)

        meta["constraint_template"] = json.dumps(constraint_template)

        # Build optimization template
        meta["optimization_template"] = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": {"program": output.optimization_meta.program, "values": output.optimization_meta.dict()},
                "program": output.optimization_meta.program,
                "qc_meta": output.qc_meta.dict(),
                "tag": meta.pop("tag", None)
            },
        })

        # Move around geometric data
        meta["optimization_program"] = output.optimization_meta.program

        meta["hash_index"] = output.hash_index

        # Hard coded data, # TODO
        meta["dimensions"] = output.get_scan_dimensions()

        if output.gridoptimization_meta.starting_grid == "zero":
            meta["iteration"] = 0
            meta["starting_grid"] = (0 for x in meta["dimensions"])
        elif output.gridoptimization_meta.starting_grid == "relative":
            meta["iteration"] = 0
            starting_grid = []
            for scan in output.gridoptimization_meta.scans:

                # Find closest index
                m = service_input.initial_molecule.measure(scan.indices)
                idx = np.abs(np.array(scan.steps) - m).argmin()
                starting_grid.append(int(idx))

            meta["starting_grid"] = tuple(starting_grid)

        else:
            raise KeyError(
                "Unknown starting_grid configuration {}.".format(output.gridoptimization_meta.starting_grid))

        return cls(**meta, storage_socket=storage_socket)

    def iterate(self):

        self.status = "RUNNING"

        if self.iteration == 0:

            self.submit_optimization_tasks({
                self.output.serialize_key(self.starting_grid): self.output.initial_molecule
            })
            self.iteration = 1

            return False

        # Check if tasks are done
        if self.task_manager.done(self.storage_socket) is False:
            return False

        # Obtain complete tasks and figure out future tasks
        complete_tasks = self.task_manager.get_tasks(self.storage_socket)
        for k, v in complete_tasks.items():
            self.final_energies[k] = v["energies"][-1]

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
            return self.finalize()

        self.submit_optimization_tasks(next_tasks)

        return False

    def submit_optimization_tasks(self, task_dict):

        new_tasks = {}

        for key, mol in task_dict.items():

            # Update molecule
            packet = json.loads(self.optimization_template)

            # Construct constraints
            constraints = json.loads(self.constraint_template)
            grid_values = self.output.get_scan_value(key)
            for con_num, k in enumerate(grid_values):
                constraints[con_num]["value"] = k
            packet["meta"]["keywords"]["values"]["constraints"] = {"set": constraints}

            # Build new molecule
            packet["data"] = [mol]

            new_tasks[key] = packet

        self.task_manager.submit_tasks(self.storage_socket, "optimization", new_tasks)

    def finalize(self):
        """
        Finishes adding data to the GridOptimization object
        """

        self.output.Config.allow_mutation = True
        self.output.success = True
        self.output.status = "COMPLETE"

        self.output.starting_grid = self.starting_grid
        self.output.grid_optimizations = self.grid_optimizations
        self.output.final_energy_dict = self.final_energies

        self.output.Config.allow_mutation = False
        return self.output
