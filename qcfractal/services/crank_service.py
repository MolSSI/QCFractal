"""
Wraps geometric procedures
"""

import copy
import json
import uuid

from crank import crankAPI

from .. import procedures

__all__ = ["Crank"]

class Crank:
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
        molecule = copy.deepcopy(molecule)
        meta["crank_state"] = crankAPI.create_initial_state(
            dihedrals=meta["crank_meta"]["dihedrals"],
            grid_spacing=meta["crank_meta"]["grid_spacing"],
            elements=molecule["symbols"],
            init_coords=[molecule["geometry"]])

        # Save initial molecule and add hash
        meta["state"] = "READY"
        meta["required_jobs"] = False
        meta["molecule_template"] = molecule

        dihedral_template = []
        for idx in meta["crank_meta"]["dihedrals"]:
            tmp =  ('dihedral', ) + tuple(str(z+1) for z in idx)
            dihedral_template.append(tmp)
        meta["crank_meta"]["dihedral_template"] = dihedral_template

        return cls(db_socket, queue_socket, meta)

    def get_json(self):
        return self.data

    def iterate(self):

        self.data["state"] = "RUNNING"
        # print("\nCrank State:")
        # print(json.dumps(self.data["crank_state"], indent=2))

        # Required jobs is false on first iteration
        if self.data["required_jobs"] is not False:

            nquery = len(self.data["required_jobs"])
            job_results = {k: [None] * v for k, v in self.data["update_structure"].items()}

            job_query = self.db_socket.get_procedures([{"crank_uuid": uid} for key, uid in self.data["required_jobs"]])

            # We are not yet done
            # print(self.data["required_jobs"])
            # print(job_query)
            if job_query["meta"]["n_found"] != nquery:
                return False

            lookup = {x[1]: x[0] for x in self.data["required_jobs"]}
            for ret in job_query["data"]:
                value, pos = lookup[ret["crank_uuid"]]
                mol_keys = self.db_socket.get_molecules([ret["initial_molecule"], ret["final_molecule"]], index="id")["data"]
                job_results[value][int(pos)] = (mol_keys[0]["geometry"], mol_keys[1]["geometry"], ret["energies"][-1])
                # print(value, pos, ret["energies"][-1])

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

        # Add molecules and grab hashes
        self.db_socket.add_molecules(flat_map)

        # Check if everything was successful

        # Prepare optimization runs
        meta_packet = json.dumps({
            "meta": {
                "procedure": "optimization",
                "keywords": self.data["geometric_meta"],
                "program": "geometric",
                "qc_meta": self.data["qc_meta"]
            },
        })

        required_jobs = []
        full_tasks = {}
        for key, mol in flat_map.items():
            uid = str(uuid.uuid4())
            packet = json.loads(meta_packet)

            containts = [
                tuple(x) + (str(y), )
                for x, y in zip(self.data["crank_meta"]["dihedral_template"], crankAPI.grid_id_from_string(key[0]))
            ]
            packet["meta"]["keywords"]["constraints"] = {"set": containts}
            packet["data"] = [mol]

            tasks, errors = procedures.get_procedure_input_parser("optimization")(self.db_socket, packet)

            # Unpack 1 element dict and add
            [(task_key, task)] = tasks.items()
            task[1]["crank_uuid"] = uid

            full_tasks[task_key + (uid, )] = task
            required_jobs.append((key, uid))

        # Create data for next round
        self.data["update_structure"] = {k: len(v) for k, v in job_dict.items()}
        self.data["required_jobs"] = required_jobs
        # print(json.dumps(required_jobs, indent=2))

        # Add tasks to Nanny
        self.queue_socket.submit_tasks(full_tasks)


    def finalize(self):
        # Add finalize state
        # Parse remaining procedures
        # Create a map of "jobs" so that procedures does not have to followed
        self.data["state"] = "FINISHED"
        #print("Crank Scan Finished")
        #print(json.dumps(self.data, indent=2))
        return crankAPI.collect_lowest_energies(self.data["crank_state"])
