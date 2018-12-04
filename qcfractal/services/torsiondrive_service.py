"""
Wraps geometric procedures
"""

import copy
import json

import numpy as np

try:
    from torsiondrive import td_api
except ImportError:
    td_api = None

from qcfractal import procedures
from qcfractal.interface import schema

__all__ = ["TorsionDriveService"]


def _check_td():
    if td_api is None:
        raise ImportError("Unable to find TorsionDrive which must be installed to use the TorsionDriveService")


class TorsionDriveService:
    def __init__(self, storage_socket, data):
        _check_td()

        # Server interaction
        self.storage_socket = storage_socket

        # Unpack data
        self.data = data

    @classmethod
    def initialize_from_api(cls, storage_socket, meta, molecule):
        _check_td()

        # Grab initial molecule
        meta["initial_molecule"] = molecule["id"]

        # Copy initial intial input and build out a torsiondrive_state
        meta = copy.deepcopy(meta)

        # Remove identity info from template
        molecule_template = copy.deepcopy(molecule)
        del molecule_template["id"]
        del molecule_template["identifiers"]

        # Initiate torsiondrive meta
        meta["torsiondrive_state"] = td_api.create_initial_state(
            dihedrals=meta["torsiondrive_meta"]["dihedrals"],
            grid_spacing=meta["torsiondrive_meta"]["grid_spacing"],
            elements=molecule_template["symbols"],
            init_coords=[molecule_template["geometry"]])

        # Save initial molecule and add hash
        meta["status"] = "READY"
        meta["required_tasks"] = False
        meta["remaining_tasks"] = False
        meta["molecule_template"] = molecule_template
        meta["optimization_history"] = {}

        dihedral_template = []
        for idx in meta["torsiondrive_meta"]["dihedrals"]:
            tmp = {"type": "dihedral", "indices": idx}
            dihedral_template.append(tmp)

        meta["torsiondrive_meta"]["dihedral_template"] = dihedral_template

        # Move around geometric data
        meta["optimization_program"] = meta["optimization_meta"].pop("program")

        # Temporary hash index
        single_keys = copy.deepcopy(meta["qc_meta"])
        single_keys["molecule_id"] = meta["initial_molecule"]
        keys = {
            "type": "torsiondrive",
            "program": "torsiondrive",
            "keywords": meta["torsiondrive_meta"],
            "optimization_keys": {
                "procedure": meta["optimization_program"],
                "keywords": meta["optimization_meta"],
            },
            "single_keys": schema.format_result_indices(single_keys)
        }

        meta["success"] = False
        meta["procedure"] = "torsiondrive"
        meta["program"] = "torsiondrive"
        meta["hash_index"] = procedures.procedures_util.hash_procedure_keys(keys)
        meta["hash_keys"] = keys
        meta["tag"] = meta.pop("tag", None)

        return cls(storage_socket, meta)

    def get_json(self):
        return self.data

    def iterate(self):

        self.data["status"] = "RUNNING"
        if self.data["remaining_tasks"] is not False:

            # Create the query payload, fetching the completed required tasks and output location
            task_query = self.storage_socket.get_queue(
                {
                    "id": self.data["required_tasks"],
                    "status": ["COMPLETE", "ERROR"]
                },
                projection={"base_result": True,
                            "status": True})
            # If all tasks are not complete, return a False
            if len(task_query["data"]) != len(self.data["required_tasks"]):
                return False

            if "ERROR" in set(x["status"] for x in task_query["data"]):
                raise KeyError("All tasks did not execute successfully.")

            # Create a lookup table for task ID mapping to result from that task in the procedure table
            inv_task_lookup = {
                x["id"]: self.storage_socket.get_procedures({
                    "id": x["base_result"]["_ref"].id
                })["data"][0]
                for x in task_query["data"]
            }

            # Populate task results
            task_results = {}
            for key, task_ids in self.data["task_map"].items():
                task_results[key] = []

                # Check for history key
                if key not in self.data["optimization_history"]:
                    self.data["optimization_history"][key] = []

                for task_id in task_ids:
                    # Cycle through all tasks for this entry
                    ret = inv_task_lookup[task_id]

                    # Lookup molecules
                    mol_keys = self.storage_socket.get_molecules(
                        [ret["initial_molecule"], ret["final_molecule"]], index="id")["data"]

                    task_results[key].append((mol_keys[0]["geometry"], mol_keys[1]["geometry"], ret["energies"][-1]))

                    # Update history
                    self.data["optimization_history"][key].append(ret["id"])

            td_api.update_state(self.data["torsiondrive_state"], task_results)

            # print("\nTorsionDrive State Updated:")
            # print(json.dumps(self.data["torsiondrive_state"], indent=2))

        # Figure out if we are still waiting on tasks

        # Create new tasks from the current state
        next_tasks = td_api.next_jobs_from_state(self.data["torsiondrive_state"], verbose=True)
        # print("\n\nNext Jobs:\n" + str(next_tasks))

        # All done
        if len(next_tasks) == 0:
            self.finalize()
            return self.data

        self.submit_optimization_tasks(next_tasks)

        return False
        # if len(next_tasks) == 0:
        #     return self.finalize()

        # step 5

        # Save torsiondrive state

    def submit_optimization_tasks(self, task_dict):

        # Prepare optimization
        initial_molecule = json.dumps(self.data["molecule_template"])
        meta_packet = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": self.data["optimization_meta"],
                "program": self.data["optimization_program"],
                "qc_meta": self.data["qc_meta"],
                "tag": self.data["tag"]
            },
        })

        hook_template = json.dumps({
            "document": ("service_queue", self.data["id"]),
            "updates": [["inc", "remaining_tasks", -1]]
        })

        full_tasks = []
        task_map = {}
        submitted_hash_id_remap = []  # Tracking variable for exondary pass
        for key, geoms in task_dict.items():
            task_map[key] = []
            for num, geom in enumerate(geoms):

                # Update molecule
                packet = json.loads(meta_packet)

                # Construct constraints
                constraints = copy.deepcopy(self.data["torsiondrive_meta"]["dihedral_template"])
                if not isinstance(key, (tuple, list)):
                    constraints[0]["value"] = key
                else:
                    for con_num, k in enumerate(key):
                        constraints[con_num]["value"] = k
                packet["meta"]["keywords"]["constraints"] = {"set": constraints}

                mol = json.loads(initial_molecule)
                mol["geometry"] = geom
                packet["data"] = [mol]

                # Turn packet into a full task, if there are duplicates, get the ID
                tasks, complete, errors = procedures.get_procedure_input_parser("optimization")(
                    self.storage_socket, packet, duplicate_id="id")

                if len(complete):
                    # Job is already complete
                    queue_id = self.storage_socket.get_procedures({"id": complete[0]})["data"][0]["queue_id"]
                    task_map[key].append(queue_id)
                else:
                    # Create a hook which will update the complete tasks uid
                    hook = json.loads(hook_template)
                    tasks[0]["hooks"].append(hook)
                    # Remember the full tasks map to update task_map later
                    submitted_hash_id_remap.append((key, num))
                    # Create a placeholder entry at that index for now, we'll update them all
                    # with known task ID's after we submit them
                    task_map[key].append(None)
                    # Add task to "list to submit"
                    full_tasks.append(tasks[0])

        # Add tasks to Nanny
        ret = self.storage_socket.queue_submit(full_tasks)
        self.data["queue_keys"] = ret["data"]
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
        self.data["task_map"] = task_map
        self.data["required_tasks"] = list({x for v in task_map.values() for x in v})

        # TODO edit remaining tasks to reflect duplicates
        self.data["remaining_tasks"] = len(self.data["required_tasks"])

    def finalize(self):
        # Add finalize state
        # Parse remaining procedures
        # Create a map of "tasks" so that procedures does not have to followed
        self.data["success"] = True

        self.data["final_energies"] = {}
        self.data["minimum_positions"] = {}

        # # Get lowest energies and positions
        for k, v in self.data["torsiondrive_state"]["grid_status"].items():
            min_pos = int(np.argmin([x[2] for x in v]))
            key = json.dumps(td_api.grid_id_from_string(k))
            self.data["minimum_positions"][key] = min_pos
            self.data["final_energies"][key] = v[min_pos][2]

        self.data["optimization_history"] = {
            json.dumps(td_api.grid_id_from_string(k)): v
            for k, v in self.data["optimization_history"].items()
        }

        # print(self.data["optimization_history"])
        # print(self.data["minimum_positions"])
        # print(self.data["final_energies"])

        # Pop temporaries
        del self.data["task_map"]
        del self.data["remaining_tasks"]
        del self.data["molecule_template"]
        del self.data["queue_keys"]
        del self.data["torsiondrive_state"]
        del self.data["status"]
        del self.data["required_tasks"]

        return self.data
