"""
Wraps geometric procedures
"""

import copy
import json
import uuid
import numpy as np

from torsiondrive import td_api

from qcfractal import procedures
from qcfractal.interface import schema

__all__ = ["TorsionDriveService"]


class TorsionDriveService:
    def __init__(self, db_socket, queue_socket, data):

        # Server interaction
        self.db_socket = db_socket
        self.queue_socket = queue_socket

        # Unpack data
        self.data = data

    @classmethod
    def initialize_from_api(cls, db_socket, queue_socket, meta, molecule):

        # Grab initial molecule
        meta["initial_molecule"] = molecule["id"]

        # Copy initial intial input and build out a torsiondrive_state
        meta = copy.deepcopy(meta)

        # Remove identity info from template
        molecule_template = copy.deepcopy(molecule)
        del molecule_template["id"]
        del molecule_template["identifiers"]

        # Iniate torsiondrive meta
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
        meta["hash_index"] = procedures.procedures_util.hash_procedure_keys(keys),
        meta["hash_keys"] = keys
        meta["tag"] = None

        return cls(db_socket, queue_socket, meta)

    def get_json(self):
        return self.data

    def iterate(self):

        self.data["status"] = "RUNNING"
        # print("\nTorsionDrive State:")
        # print(json.dumps(self.data["torsiondrive_state"], indent=2))
        # print("Iterate")
        if (self.data["remaining_jobs"] > 0):
            # print("Iterate: not yet done", self.data["remaining_jobs"])
            # print("Complete jobs", self.data["complete_jobs"])
            return False

        # print(self.data["remaining_jobs"])

        # Required jobs is false on first iteration
        if (self.data["remaining_jobs"] is not False) and (self.data["remaining_jobs"] == 0):

            # Query the jobs
            job_query = self.db_socket.get_procedures(list(self.data["complete_jobs"].values()), by_id=True)

            # Figure out the structure
            job_results = {k: [None] * v for k, v in self.data["update_structure"].items()}
            job_ids = {k: [None] * v for k, v in self.data["update_structure"].items()}

            inv_job_lookup = {v: k for k, v in self.data["complete_jobs"].items()}

            for ret in job_query["data"]:
                job_uid = inv_job_lookup[ret["id"]]
                value, pos = self.data["job_map"][job_uid]
                mol_keys = self.db_socket.get_molecules(
                    [ret["initial_molecule"], ret["final_molecule"]], index="id")["data"]

                job_results[value][int(pos)] = (mol_keys[0]["geometry"], mol_keys[1]["geometry"], ret["energies"][-1])
                job_ids[value][int(pos)] = ret["id"]

            # Update the complete_jobs in order
            for k, v in job_ids.items():
                if k not in self.data["optimization_history"]:
                    self.data["optimization_history"][k] = []
                self.data["optimization_history"][k].extend(v)

            td_api.update_state(self.data["torsiondrive_state"], job_results)

            # print("\nTorsionDrive State Updated:")
            # print(json.dumps(self.data["torsiondrive_state"], indent=2))

        # Figure out if we are still waiting on jobs

        # Create new jobs from the current state
        next_jobs = td_api.next_jobs_from_state(self.data["torsiondrive_state"], verbose=True)

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

        # Build out all of the new molecules in a flat dictionary
        flat_map = {}
        initial_molecule = json.dumps(self.data["molecule_template"])
        for v, k in job_dict.items():
            for num, geom in enumerate(k):
                mol = json.loads(initial_molecule)
                mol["geometry"] = geom
                flat_map[(v, str(num))] = mol

        # Add new molecules
        self.db_socket.add_molecules(flat_map)

        # Prepare optimization
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
            "updates": [["inc", "remaining_jobs", -1], ["set", "complete_jobs", "$task_id"]]
        })

        job_map = {}
        full_tasks = []
        for key, mol in flat_map.items():
            packet = json.loads(meta_packet)

            # Construct constraints
            containts = [
                tuple(x) + (str(y), )
                for x, y in zip(self.data["torsiondrive_meta"]["dihedral_template"], td_api.grid_id_from_string(key[0]))
            ]
            packet["meta"]["keywords"]["constraints"] = {"set": containts}
            packet["data"] = [mol]

            # Turn packet into a full task
            task, errors = procedures.get_procedure_input_parser("optimization")(self.db_socket, packet)

            uid = str(uuid.uuid4())
            hook = json.loads(hook_template)
            hook["updates"][-1][1] = "complete_jobs." + uid

            task[0]["hooks"].append(hook)
            full_tasks.append(task[0])
            job_map[uid] = key

        # Create data for next round
        self.data["update_structure"] = {k: len(v) for k, v in job_dict.items()}
        self.data["job_map"] = job_map
        self.data["remaining_jobs"] = len(job_map)
        self.data["complete_jobs"] = {}
        # print(json.dumps(required_jobs, indent=2))

        # Add tasks to Nanny
        ret = self.queue_socket.submit_tasks(full_tasks)
        self.data["queue_keys"] = [x[1] for x in ret["data"]]

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

        # print(self.data["optimization_history"])
        # print(self.data["minimum_positions"])
        # print(self.data["final_energies"])

        # Pop temporaries
        del self.data["update_structure"]
        del self.data["job_map"]
        del self.data["remaining_jobs"]
        del self.data["complete_jobs"]
        del self.data["molecule_template"]
        del self.data["queue_keys"]
        del self.data["torsiondrive_state"]
        del self.data["status"]

        return self.data

