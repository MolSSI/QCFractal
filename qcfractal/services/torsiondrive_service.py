"""
Wraps geometric procedures
"""

import copy
import json
import numpy as np

from torsiondrive import td_api

from qcfractal import procedures
from qcfractal.interface import schema

__all__ = ["TorsionDriveService"]


class TorsionDriveService:
    def __init__(self, storage_socket, queue_socket, data):

        # Server interaction
        self.storage_socket = storage_socket
        self.queue_socket = queue_socket

        # Unpack data
        self.data = data

    @classmethod
    def initialize_from_api(cls, storage_socket, queue_socket, meta, molecule):

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
        meta["required_jobs"] = False
        meta["remaining_jobs"] = False
        meta["molecule_template"] = molecule_template
        meta["optimization_history"] = {}

        dihedral_template = []
        for idx in meta["torsiondrive_meta"]["dihedrals"]:
            tmp = ('dihedral', ) + tuple(str(z + 1) for z in idx)
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
        meta["tag"] = None

        return cls(storage_socket, queue_socket, meta)

    def get_json(self):
        return self.data

    def iterate(self):

        self.data["status"] = "RUNNING"
        # print("\nTorsionDrive State:")
        # print(json.dumps(self.data["torsiondrive_state"], indent=2))
        # print("Iterate")
        #if (self.data["remaining_jobs"] > 0):
            # print("Iterate: not yet done", self.data["remaining_jobs"])
            # print("Complete jobs", self.data["complete_jobs"])
        #    return False
        # if self.data["success"] is True:
        #     return False


        # print(self.data["remaining_jobs"])

        # Required jobs is false on first iteration
        # next_iter = (self.data["remaining_jobs"] is not False) and (self.data["remaining_jobs"] == 0):
        # print("ID {} : REMAINING JOBS {}".format(self.data["hash_index"], self.data["queue_keys"]))
        # print("rem job", self.data["remaining_jobs"])
        if self.data["remaining_jobs"] is not False:

            # Create the query payload, fetching the completed required jobs and output location
            payload = self.storage_socket.get_queue({"id": self.data["required_jobs"], "status": "COMPLETE"},
                                                    projection={"result_location": True, "status": True})
            # If all jobs are not complete, return a False
            if len(payload["data"]) != len(self.data["required_jobs"]):
                return False

            job_query = payload["data"]
            # Create a lookup table for job ID mapping to result from that job in the procedure table
            inv_job_lookup = {v["id"]: self.storage_socket.locator(v["result_location"])["data"][0]
                              for v in job_query}

            # Populate job results
            job_results = {}
            for key, job_ids in self.data["job_map"].items():
                job_results[key] = []

                # Check for history key
                if key not in self.data["optimization_history"]:
                    self.data["optimization_history"][key] = []

                for job_id in job_ids:
                    # Cycle through all jobs for this entry
                    ret = inv_job_lookup[job_id]

                    # Lookup molecules
                    mol_keys = self.storage_socket.get_molecules(
                        [ret["initial_molecule"], ret["final_molecule"]], index="id")["data"]

                    job_results[key].append((mol_keys[0]["geometry"], mol_keys[1]["geometry"], ret["energies"][-1]))

                    # Update history
                    self.data["optimization_history"][key].append(ret["id"])

            td_api.update_state(self.data["torsiondrive_state"], job_results)

            # print("\nTorsionDrive State Updated:")
            # print(json.dumps(self.data["torsiondrive_state"], indent=2))

        # Figure out if we are still waiting on jobs

        # Create new jobs from the current state
        next_jobs = td_api.next_jobs_from_state(self.data["torsiondrive_state"], verbose=True)
        # print("\n\nNext Jobs:\n" + str(next_jobs))

        # All done
        if len(next_jobs) == 0:
            self.finalize()
            return self.data

        self.submit_optimization_tasks(next_jobs)

        return False
        # if len(next_jobs) == 0:
        #     return self.finalize()

        # step 5

        # Save torsiondrive state

    def submit_optimization_tasks(self, job_dict):

        # Prepare optimization
        initial_molecule = json.dumps(self.data["molecule_template"])
        meta_packet = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": self.data["optimization_meta"],
                "program": self.data["optimization_program"],
                "qc_meta": self.data["qc_meta"]
            },
        })

        hook_template = json.dumps({
            "document": ("service_queue", self.data["id"]),
            "updates": [["inc", "remaining_jobs", -1]]
        })

        full_tasks = []
        job_map = {}
        submitted_hash_id_remap = []  # Tracking variable for exondary pass
        for key, geoms in job_dict.items():
            job_map[key] = []
            for num, geom in enumerate(geoms):

                # Update molecule
                packet = json.loads(meta_packet)

                # Construct constraints
                containts = [
                    tuple(x) + (str(y), )
                    for x, y in zip(self.data["torsiondrive_meta"]["dihedral_template"],
                                    td_api.grid_id_from_string(key))
                ]
                packet["meta"]["keywords"]["constraints"] = {"set": containts}

                mol = json.loads(initial_molecule)
                mol["geometry"] = geom
                packet["data"] = [mol]

                # Turn packet into a full task, if there are duplicates, get the ID
                tasks, complete, errors = procedures.get_procedure_input_parser("optimization")(
                    self.storage_socket, packet, duplicate_id="id")

                if len(complete):
                    # Job is already complete
                    queue_id = self.storage_socket.get_procedures({"id": complete[0]})["data"][0]["queue_id"]
                    job_map[key].append(queue_id)
                else:
                    # Create a hook which will update the complete jobs uid
                    hook = json.loads(hook_template)
                    tasks[0]["hooks"].append(hook)
                    # Remember the full tasks map to update job_map later
                    submitted_hash_id_remap.append((key, num))
                    # Create a placeholder entry at that index for now, we'll update them all
                    # with known task ID's after we submit them
                    job_map[key].append(None)
                    # Add task to "list to submit"
                    full_tasks.append(tasks[0])

        # Add tasks to Nanny
        ret = self.queue_socket.submit_tasks(full_tasks)
        self.data["queue_keys"] = ret["data"]
        if len(ret["meta"]["duplicates"]):
            raise RuntimeError("It appears that one of the jobs you submitted is already in the queue, but was "
                               "not there when the jobs were populated.\n"
                               "This should only happen if someone else submitted a similar or exact job "
                               "was submitted at the same time.\n"
                               "This is a corner case we have not solved yet. Please open a ticket with QCFractal"
                               "describing the conditions which yielded this message.")

        # Create data for next round
        # Update job map based on task IDs
        for (key, list_index), returned_id in zip(submitted_hash_id_remap, ret['data']):
            job_map[key][list_index] = returned_id
        self.data["job_map"] = job_map
        self.data["required_jobs"] = list({x for v in job_map.values() for x in v})

        # TODO edit remaining jobs to reflect duplicates
        self.data["remaining_jobs"] = len(self.data["required_jobs"])

    def finalize(self):
        # Add finalize state
        # Parse remaining procedures
        # Create a map of "jobs" so that procedures does not have to followed
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
        del self.data["job_map"]
        del self.data["remaining_jobs"]
        del self.data["molecule_template"]
        del self.data["queue_keys"]
        del self.data["torsiondrive_state"]
        del self.data["status"]
        del self.data["required_jobs"]

        return self.data

