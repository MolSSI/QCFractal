"""
Wraps geometric procedures
"""

import copy
import json

import numpy as np
from typing import Any, Dict, List, Set, Tuple

from qcfractal.interface.models.gridoptimization import GridOptimization
from qcfractal.interface.models.common_models import json_encoders
from qcfractal import procedures
from qcfractal.extras import get_information

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
    seeds: Set[Tuple]
    complete: Set[Tuple] = set()
    dimensions: Tuple

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
    def initialize_from_api(cls, storage_socket, meta, molecule):

        # Validate input
        output = GridOptimization(
            **meta,
            initial_molecule=molecule["id"],
            provenance={
                "creator": "QCFractal",
                "version": get_information("version"),
                "routine": "qcfractal.services.gridoptimization"
            },
            final_energy_dict={},
            grid_optimizations={})

        meta = {"output": output}

        # Remove identity info from molecule template
        molecule_template = copy.deepcopy(molecule)
        del molecule_template["id"]
        del molecule_template["identifiers"]
        meta["molecule_template"] = json.dumps(molecule_template)

        # Build dihedral template
        constraint_template = []
        for scan in output.gridoptimization_meta.scans:
            tmp = {"type": scan.type, "indices": 0}
            constraint_template.append(tmp)

        meta["constraint_template"] = json.dumps(constraint_template)

        # Build optimization template
        meta["optimization_template"] = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": output.optimization_meta.dict(),
                "program": output.optimization_meta.program,
                "qc_meta": output.qc_meta.dict(),
                "tag": meta.pop("tag", None)
            },
        })

        # Move around geometric data
        meta["optimization_program"] = output.optimization_meta.program

        meta["hash_index"] = output.get_hash_index()
        meta["seeds"] = set([(0, 0)])
        meta["dimensions"] = (2, 2)

        return cls(**meta, storage_socket=storage_socket)

    def iterate(self):

        self.status = "RUNNING"

        # Check if tasks are done
        if self.task_manager.done(self.storage_socket) is False:
            return False

        complete_tasks = self.task_manager.get_tasks(self.storage_socket)

        # Populate task results

        new_tasks = expand_ndimensional_grid(self.dimensions, self.seeds, self.complete)

        print(new_tasks)
        raise Exception()

        # Create new tasks from the current state
        next_tasks = td_api.next_jobs_from_state(self.gridoptimization_state, verbose=True)

        # All done
        if len(next_tasks) == 0:
            return self.finalize()

        self.submit_optimization_tasks(next_tasks)

        return False

    def submit_optimization_tasks(self, task_dict):

        procedure_parser = procedures.get_procedure_parser("optimization", self.storage_socket)

        new_tasks = {}
        task_map = {}

        for key, geoms in task_dict.items():

            # Update molecule
            packet = json.loads(self.optimization_template)

            # Construct constraints
            constraints = json.loads(self.constraint_template)
            grid_id = td_api.grid_id_from_string(key)
            for con_num, k in enumerate(grid_id):
                constraints[con_num]["value"] = k
            packet["meta"]["keywords"]["constraints"] = {"set": constraints}

            # Build new molecule
            mol = json.loads(self.molecule_template)
            mol["geometry"] = geom
            packet["data"] = [mol]

            task_key = "{}-{}".format(key, num)
            new_tasks[task_key] = packet

            task_map[key].append(task_key)

        self.task_manager.submit_tasks(self.storage_socket, "optimization", new_tasks)
        self.task_map = task_map

    def finalize(self):
        """
        Finishes adding data to the GridOptimization object
        """

        self.output.Config.allow_mutation = True
        self.output.success = True
        self.output.status = "COMPLETE"

        self.output.Config.allow_mutation = False
        return self.output
