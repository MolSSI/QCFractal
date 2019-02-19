"""
Wraps geometric procedures
"""

import copy
import json
from typing import Any, Dict, List

import numpy as np

from ..interface.models.common_models import json_encoders
from ..interface.models.torsiondrive import TorsionDrive
from .service_util import BaseService, TaskManager

try:
    import torsiondrive
    from torsiondrive import td_api
except ImportError:
    td_api = None



__all__ = ["TorsionDriveService"]


def _check_td():
    if td_api is None:
        raise ImportError("Unable to find TorsionDrive which must be installed to use the TorsionDriveService")


class TorsionDriveService(BaseService):

    # Index info
    status: str = "READY"
    service: str = "torsiondrive"
    program: str = "torsiondrive"
    procedure: str = "torsiondrive"

    # Output
    output: TorsionDrive

    # Temporaries
    torsiondrive_state: Dict[str, Any]
    optimization_history: Dict[str, List[str]] = {}

    # Task helpers
    task_map: Dict[str, List[str]] = {}
    task_manager: TaskManager = TaskManager()

    # Templates
    dihedral_template: str
    optimization_template: str
    molecule_template: str

    class Config:
        json_encoders = json_encoders

    @classmethod
    def initialize_from_api(cls, storage_socket, service_input):
        _check_td()

        # Build the results object
        input_dict = service_input.dict()
        input_dict["initial_molecule"] = [x["id"] for x in input_dict["initial_molecule"]]

        # Validate input
        output = TorsionDrive(
            **input_dict,
            provenance={
                "creator": "torsiondrive",
                "version": torsiondrive.__version__,
                "routine": "torsiondrive.td_api"
            },
            final_energy_dict={},
            minimum_positions={},
            optimization_history={})

        meta = {"output": output}

        # Remove identity info from molecule template
        molecule_template = copy.deepcopy(service_input.initial_molecule[0].json_dict())
        molecule_template.pop("id", None)
        molecule_template.pop("identifiers", None)
        meta["molecule_template"] = json.dumps(molecule_template)

        # Initiate torsiondrive meta
        meta["torsiondrive_state"] = td_api.create_initial_state(
            dihedrals=output.keywords.dihedrals,
            grid_spacing=output.keywords.grid_spacing,
            elements=molecule_template["symbols"],
            init_coords=[x.geometry for x in service_input.initial_molecule])

        # Build dihedral template
        dihedral_template = []
        for idx in output.keywords.dihedrals:
            tmp = {"type": "dihedral", "indices": idx}
            dihedral_template.append(tmp)

        meta["dihedral_template"] = json.dumps(dihedral_template)

        # Build optimization template
        meta["optimization_template"] = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": {
                    "program": output.optimization_spec.program,
                    "values": output.optimization_spec.keywords
                },
                "program": output.optimization_spec.program,
                "qc_spec": output.qc_spec.dict(),
                "tag": meta.pop("tag", None)
            },
        })

        # Move around geometric data
        meta["optimization_program"] = output.optimization_spec.program

        meta["hash_index"] = output.get_hash_index()

        return cls(**meta, storage_socket=storage_socket)

    def iterate(self):

        self.status = "RUNNING"

        # Check if tasks are done
        print("Done", self.task_manager.done(self.storage_socket))
        if self.task_manager.done(self.storage_socket) is False:
            return False

        complete_tasks = self.task_manager.get_tasks(self.storage_socket)

        # Populate task results
        task_results = {}
        for key, task_ids in self.task_map.items():
            task_results[key] = []

            # Check for history key
            if key not in self.optimization_history:
                self.optimization_history[key] = []

            for task_id in task_ids:
                # Cycle through all tasks for this entry
                ret = complete_tasks[task_id]

                # Lookup molecules
                mol_keys = self.storage_socket.get_molecules(
                    [ret["initial_molecule"], ret["final_molecule"]], index="id")["data"]

                task_results[key].append((mol_keys[0]["geometry"], mol_keys[1]["geometry"], ret["energies"][-1]))

                # Update history
                self.optimization_history[key].append(ret["id"])

        td_api.update_state(self.torsiondrive_state, task_results)

        # Create new tasks from the current state
        next_tasks = td_api.next_jobs_from_state(self.torsiondrive_state, verbose=True)

        # All done
        if len(next_tasks) == 0:
            return self.finalize()

        self.submit_optimization_tasks(next_tasks)

        return False

    def submit_optimization_tasks(self, task_dict):

        new_tasks = {}
        task_map = {}

        for key, geoms in task_dict.items():
            task_map[key] = []
            for num, geom in enumerate(geoms):

                # Update molecule
                packet = json.loads(self.optimization_template)

                # Construct constraints
                constraints = json.loads(self.dihedral_template)
                grid_id = td_api.grid_id_from_string(key)
                for con_num, k in enumerate(grid_id):
                    constraints[con_num]["value"] = k
                packet["meta"]["keywords"]["values"]["constraints"] = {"set": constraints}

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
        Finishes adding data to the TorsionDrive object
        """

        self.output.Config.allow_mutation = True
        self.output.success = True
        self.output.status = "COMPLETE"

        # # Get lowest energies and positions
        for k, v in self.torsiondrive_state["grid_status"].items():
            min_pos = int(np.argmin([x[2] for x in v]))
            key = json.dumps(td_api.grid_id_from_string(k))
            self.output.minimum_positions[key] = min_pos
            self.output.final_energy_dict[key] = v[min_pos][2]

        self.output.optimization_history = {
            json.dumps(td_api.grid_id_from_string(k)): v
            for k, v in self.optimization_history.items()
        }

        self.output.Config.allow_mutation = False
        return self.output
