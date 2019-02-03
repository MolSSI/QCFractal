"""
Wraps geometric procedures
"""

import copy
import json

import numpy as np
from typing import Any, Dict, List

try:
    import torsiondrive
    from torsiondrive import td_api
except ImportError:
    td_api = None

from qcfractal import procedures
from qcfractal.interface.models.torsiondrive import TorsionDrive
from qcfractal.interface.models.common_models import json_encoders

from .service_util import BaseService

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
    required_tasks: List[str] = []
    task_map: Dict[str, List[str]] = {}
    optimization_history: Dict[str, List[str]] = {}

    # Templates
    dihedral_template: str
    optimization_template: str
    molecule_template: str

    class Config:
        json_encoders = json_encoders

    @classmethod
    def initialize_from_api(cls, storage_socket, meta, molecule):
        _check_td()

        # Validate input
        output = TorsionDrive(
            **meta,
            initial_molecule=molecule["id"],
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
        molecule_template = copy.deepcopy(molecule)
        del molecule_template["id"]
        del molecule_template["identifiers"]
        meta["molecule_template"] = json.dumps(molecule_template)

        # Initiate torsiondrive meta
        meta["torsiondrive_state"] = td_api.create_initial_state(
            dihedrals=output.torsiondrive_meta.dihedrals,
            grid_spacing=output.torsiondrive_meta.grid_spacing,
            elements=molecule_template["symbols"],
            init_coords=[molecule_template["geometry"]])

        # Build dihedral template
        dihedral_template = []
        for idx in output.torsiondrive_meta.dihedrals:
            tmp = {"type": "dihedral", "indices": idx}
            dihedral_template.append(tmp)

        meta["dihedral_template"] = json.dumps(dihedral_template)

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

        return cls(**meta, storage_socket=storage_socket)

    def iterate(self):

        self.status = "RUNNING"

        # Create the query payload, fetching the completed required tasks and output location
        task_query = self.storage_socket.get_queue(
            id=self.required_tasks, status=["COMPLETE", "ERROR"], projection={"base_result": True,
                                                                              "status": True})
        # If all tasks are not complete, return a False
        if len(task_query["data"]) != len(self.required_tasks):
            return False

        if "ERROR" in set(x["status"] for x in task_query["data"]):
            raise KeyError("All tasks did not execute successfully.")

        # Create a lookup table for task ID mapping to result from that task in the procedure table
        inv_task_lookup = {
            x["id"]: self.storage_socket.get_procedures_by_id(id=x["base_result"]["id"])["data"][0]
            for x in task_query["data"]
        }

        # Populate task results
        task_results = {}
        for key, task_ids in self.task_map.items():
            task_results[key] = []

            # Check for history key
            if key not in self.optimization_history:
                self.optimization_history[key] = []

            for task_id in task_ids:
                # Cycle through all tasks for this entry
                ret = inv_task_lookup[task_id]

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

        procedure_parser = procedures.get_procedure_parser("optimization", self.storage_socket)

        full_tasks = []
        task_map = {}
        submitted_hash_id_remap = []  # Tracking variable for exondary pass
        for key, geoms in task_dict.items():
            task_map[key] = []
            for num, geom in enumerate(geoms):

                # Update molecule
                packet = json.loads(self.optimization_template)

                # Construct constraints
                constraints = json.loads(self.dihedral_template)
                grid_id = td_api.grid_id_from_string(key)
                if len(grid_id) == 1:
                    constraints[0]["value"] = grid_id[0]
                else:
                    for con_num, k in enumerate(grid_id):
                        constraints[con_num]["value"] = k
                packet["meta"]["keywords"]["constraints"] = {"set": constraints}

                # Build new molecule
                mol = json.loads(self.molecule_template)
                mol["geometry"] = geom
                packet["data"] = [mol]

                # Turn packet into a full task, if there are duplicates, get the ID
                tasks, completed, errors = procedure_parser.parse_input(packet, duplicate_id="id")

                if len(completed):
                    # Job is already complete
                    task_map[key].append(completed[0]["task_id"])
                else:
                    # Remember the full tasks map to update task_map later
                    submitted_hash_id_remap.append((key, num))
                    # Create a placeholder entry at that index for now, we'll update them all
                    # with known task ID's after we submit them
                    task_map[key].append(None)
                    # Add task to "list to submit"
                    full_tasks.append(tasks[0])

        # Add tasks to Nanny
        ret = self.storage_socket.queue_submit(full_tasks)
        if len(ret["meta"]["duplicates"]):
            raise RuntimeError("It appears that one of the tasks you submitted is already in the queue, but was "
                               "not there when the tasks were populated.\n"
                               "This should only happen if someone else submitted a similar or exact task "
                               "was submitted at the same time.\n"
                               "This is a corner case we have not solved yet. Please open a ticket with QCFractal"
                               "describing the conditions which yielded this message.")

        # Create data for next round
        # Update task map based on task IDs
        for (key, list_index), returned_id in zip(submitted_hash_id_remap, ret['data']):
            task_map[key][list_index] = returned_id
        self.task_map = task_map
        self.required_tasks = list({x for v in task_map.values() for x in v})

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
