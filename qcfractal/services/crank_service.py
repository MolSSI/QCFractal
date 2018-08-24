"""
Wraps geometric procedures
"""

import copy
import json
import uuid

from crank import crankAPI

from .. import procedures

__all__ = ["CrankService"]


class CrankService:
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

        # Copy initial intial input and build out a crank_state
        meta = copy.deepcopy(meta)

        # Remove identity info from template
        molecule_template = copy.deepcopy(molecule)
        del molecule_template["id"]
        del molecule_template["molecule_hash"]

        # Iniate crank meta
        meta["crank_state"] = crankAPI.create_initial_state(
            dihedrals=meta["crank_meta"]["dihedrals"],
            grid_spacing=meta["crank_meta"]["grid_spacing"],
            elements=molecule_template["symbols"],
            init_coords=[molecule_template["geometry"]])

        # Save initial molecule and add hash
        meta["state"] = "READY"
        meta["required_jobs"] = False
        meta["remaining_jobs"] = False
        meta["molecule_template"] = molecule_template
        meta["complete_jobs"] = []

        dihedral_template = []
        for idx in meta["crank_meta"]["dihedrals"]:
            tmp = ('dihedral', ) + tuple(str(z + 1) for z in idx)
            dihedral_template.append(tmp)
        meta["crank_meta"]["dihedral_template"] = dihedral_template

        return cls(db_socket, queue_socket, meta)

    def get_json(self):
        return self.data

    def iterate(self):

        self.data["state"] = "RUNNING"
        # print("\nCrank State:")
        # print(json.dumps(self.data["crank_state"], indent=2))
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

            inv_job_lookup = {v : k for k, v in self.data["complete_jobs"].items()}

            for ret in job_query["data"]:
                job_uid = inv_job_lookup[ret["id"]]
                value, pos = self.data["job_map"][job_uid]
                mol_keys = self.db_socket.get_molecules(
                    [ret["initial_molecule"], ret["final_molecule"]], index="id")["data"]
                job_results[value][int(pos)] = (mol_keys[0]["geometry"], mol_keys[1]["geometry"], ret["energies"][-1])

            # print("Job Results:", json.dumps(job_results, indent=2))

            crankAPI.update_state(self.data["crank_state"], job_results)

            # print("\nCrank State Updated:")
            # print(json.dumps(self.data["crank_state"], indent=2))

        # Figure out if we are still waiting on jobs

        # Create new jobs from the current state
        next_jobs = crankAPI.next_jobs_from_state(self.data["crank_state"], verbose=True)

        # All done
        if len(next_jobs) == 0:
            self.finalize()
            return True

        self.submit_geometric_tasks(next_jobs)

        return False
        # if len(next_jobs) == 0:
        #     return self.finalize()

        # step 5

        # Save crank state

    def submit_geometric_tasks(self, job_dict):

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
                "keywords": self.data["geometric_meta"],
                "program": "geometric",
                "qc_meta": self.data["qc_meta"]
            },
        })

        hook_template = json.dumps({
            "document": ("services", self.data["id"]),
            "updates": [["inc", "remaining_jobs", -1], ["set", "complete_jobs", "$task_id"]]
        })

        job_map = {}
        full_tasks = []
        for key, mol in flat_map.items():
            packet = json.loads(meta_packet)

            # Construct constraints
            containts = [
                tuple(x) + (str(y), )
                for x, y in zip(self.data["crank_meta"]["dihedral_template"], crankAPI.grid_id_from_string(key[0]))
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
        self.data["state"] = "FINISHED"
        final_energies = crankAPI.collect_lowest_energies(self.data["crank_state"])
        self.data["final_energies"] = {json.dumps(k): v for k, v in final_energies.items()}

        # Pop temporaries
        del self.data["update_structure"]
        del self.data["job_map"]
        del self.data["remaining_jobs"]
        del self.data["complete_jobs"]
        del self.data["molecule_template"]
        del self.data["queue_keys"]


