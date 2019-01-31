"""
Wraps geometric procedures
"""

import copy
import json

import numpy as np

try:
    import torsiondrive
    from torsiondrive import td_api
except ImportError:
    td_api = None

from qcfractal import procedures
from qcfractal.interface import schema
from qcfractal.interface.models.torsiondrive import TorsionDriveInput

from typing import Any, Dict
from pydantic import BaseModel

__all__ = ["TorsionDriveService"]


def _check_td():
    if td_api is None:
        raise ImportError("Unable to find TorsionDrive which must be installed to use the TorsionDriveService")



class TorsionDriveService(BaseModel):

    storage_socket: Any

    # Index info
    id: str = None
    hash_index: str
    success: bool= False
    status: str = "READY"
    service: str = "torsiondrive"
    program: str = "torsiondrive"
    procedure: str = "torsiondrive"

    molecule_template: Any
    torsiondrive_state: Dict[str, Any]
    torsiondrive_meta: Dict[str, Any]
    required_tasks: Any
    remaining_tasks: Any
    optimization_history: Any = {}
    task_map: Any = {}
    optimization_template: str
    queue_keys: Any= None


    # def __init__(self, storage_socket, data):
    #     _check_td()
    #     super().__init__(**data)

    #     # # Server interaction
    #     self.storage_socket = storage_socket
    #     # self.data = data


    @classmethod
    def initialize_from_api(cls, storage_socket, meta, molecule):
        _check_td()

        tdinput = TorsionDriveInput(**meta, initial_molecule=molecule)

        meta = copy.deepcopy(meta)
        # Remove identity info from template
        molecule_template = copy.deepcopy(molecule)
        del molecule_template["id"]
        del molecule_template["identifiers"]
        meta["molecule_template"] = molecule_template

        # Initiate torsiondrive meta
        meta["torsiondrive_state"] = td_api.create_initial_state(
            dihedrals=tdinput.torsiondrive_meta.dihedrals,
            grid_spacing=tdinput.torsiondrive_meta.grid_spacing,
            elements=tdinput.initial_molecule.symbols,
            init_coords=[tdinput.initial_molecule.geometry.ravel().tolist()])

        # Save initial molecule and add hash
        meta["required_tasks"] = False
        meta["remaining_tasks"] = False

        dihedral_template = []
        for idx in meta["torsiondrive_meta"]["dihedrals"]:
            tmp = {"type": "dihedral", "indices": idx}
            dihedral_template.append(tmp)

        meta["torsiondrive_meta"]["dihedral_template"] = dihedral_template


        meta["optimization_template"] = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": tdinput.optimization_meta.dict(),
                "program": tdinput.optimization_meta.program,
                "qc_meta": tdinput.qc_meta.dict(),
                "tag": meta.pop("tag", None)
            },
        })

        # Move around geometric data
        meta["optimization_program"] = meta["optimization_meta"].pop("program")

        meta["hash_index"] = tdinput.get_hash_index()
        meta["provenance"] = {"creator": "torsiondrive",
                              "version": torsiondrive.__version__,
                              "route": "torsiondrive.td_api"}

        return cls(**meta, storage_socket=storage_socket)

    def dict(self, include=None, exclude=None, by_alias=False):
        # return self.data
        return super().dict(exclude={"storage_socket"})

    def get_json(self):
        return json.loads(self.json())

    def iterate(self):

        self.status = "RUNNING"
        if self.remaining_tasks is not False:

            # Create the query payload, fetching the completed required tasks and output location
            task_query = self.storage_socket.get_queue(
                id=self.required_tasks,
                status=["COMPLETE", "ERROR"],
                projection={"base_result": True,
                            "status": True})
            # If all tasks are not complete, return a False
            if len(task_query["data"]) != len(self.required_tasks):
                return False

            if "ERROR" in set(x["status"] for x in task_query["data"]):
                raise KeyError("All tasks did not execute successfully.")

            # Create a lookup table for task ID mapping to result from that task in the procedure table
            inv_task_lookup = {
                x["id"]: self.storage_socket.get_procedures_by_id(
                    id=x["base_result"]["id"]
                )["data"][0]
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

            # print("\nTorsionDrive State Updated:")
            # print(json.dumps(self.torsiondrive_state, indent=2))

        # Figure out if we are still waiting on tasks

        # Create new tasks from the current state
        next_tasks = td_api.next_jobs_from_state(self.torsiondrive_state, verbose=True)
        # print("\n\nNext Jobs:\n" + str(next_tasks))

        # All done
        if len(next_tasks) == 0:
            return self.finalize()

        self.submit_optimization_tasks(next_tasks)

        return False
        # if len(next_tasks) == 0:
        #     return self.finalize()

        # step 5

        # Save torsiondrive state

    def submit_optimization_tasks(self, task_dict):

        # Prepare optimization
        initial_molecule = json.dumps(self.molecule_template)
        meta_packet = self.optimization_template

        # hook_template = json.dumps({
        #     "document": ("service_queue", self.id),
        #     "updates": [["inc", "remaining_tasks", -1]]
        # })

        procedure_parser = procedures.get_procedure_parser("optimization", self.storage_socket)

        full_tasks = []
        task_map = {}
        submitted_hash_id_remap = []  # Tracking variable for exondary pass
        for key, geoms in task_dict.items():
            task_map[key] = []
            for num, geom in enumerate(geoms):

                # Update molecule
                packet = json.loads(meta_packet)

                # Construct constraints
                constraints = copy.deepcopy(self.torsiondrive_meta["dihedral_template"])
                grid_id = td_api.grid_id_from_string(key)
                if len(grid_id) == 1:
                    constraints[0]["value"] = grid_id[0]
                else:
                    for con_num, k in enumerate(grid_id):
                        constraints[con_num]["value"] = k
                packet["meta"]["keywords"]["constraints"] = {"set": constraints}

                mol = json.loads(initial_molecule)
                mol["geometry"] = geom
                packet["data"] = [mol]

                # Turn packet into a full task, if there are duplicates, get the ID
                tasks, completed, errors = procedure_parser.parse_input(packet, duplicate_id="id")

                if len(completed):
                    # Job is already complete
                    task_map[key].append(completed[0]["task_id"])
                else:
                    # Create a hook which will update the complete tasks uid
                    # hook = json.loads(hook_template)
                    # tasks[0]["hooks"].append(hook)
                    # Remember the full tasks map to update task_map later
                    submitted_hash_id_remap.append((key, num))
                    # Create a placeholder entry at that index for now, we'll update them all
                    # with known task ID's after we submit them
                    task_map[key].append(None)
                    # Add task to "list to submit"
                    full_tasks.append(tasks[0])

        # Add tasks to Nanny
        ret = self.storage_socket.queue_submit(full_tasks)
        self.queue_keys = ret["data"]
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

        # TODO edit remaining tasks to reflect duplicates
        self.remaining_tasks = len(self.required_tasks)

    def finalize(self):
        # Add finalize state
        # Parse remaining procedures
        # Create a map of "tasks" so that procedures does not have to followed

        data = self.dict()
        data["success"] = True
        data["status"] = "COMPLETE"

        data["final_energies"] = {}
        data["minimum_positions"] = {}

        # # Get lowest energies and positions
        for k, v in self.torsiondrive_state["grid_status"].items():
            min_pos = int(np.argmin([x[2] for x in v]))
            key = json.dumps(td_api.grid_id_from_string(k))
            data["minimum_positions"][key] = min_pos
            data["final_energies"][key] = v[min_pos][2]

        data["optimization_history"] = {
            json.dumps(td_api.grid_id_from_string(k)): v
            for k, v in data["optimization_history"].items()
        }

        # print(self.optimization_history"])
        # print(self.minimum_positions"])
        # print(self.final_energies"])

        # Pop temporaries
        del data["task_map"]
        del data["remaining_tasks"]
        del data["molecule_template"]
        del data["queue_keys"]
        del data["torsiondrive_state"]
        del data["required_tasks"]

        return data
