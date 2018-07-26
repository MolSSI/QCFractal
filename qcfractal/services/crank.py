"""
Wraps geometric procedures
"""

import copy
import collections
import json

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

        return cls(db_socket, queue_socket, meta)

    def get_json(self):
        return self.data

    def iterate(self):

        self.data["state"] = "RUNNING"
        print("\nCrank State:")
        print(json.dumps(self.data["crank_state"], indent=2))

        # Required jobs is false on first iteration
        if self.data["required_jobs"] is not False:
            query = copy.deepcopy(self.data["required_jobs"])
            query["initial_molecule"] = {"$in": self.data["required_jobs"]["initial_molecules"]}
            del query["initial_molecules"]
            print("\nQuery")
            print(query)
            ret = self.db_socket.get_procedures([query])
            print("\nReturned")
            print(ret)

            print('\n------\n')

            if len(self.data["molecule_map"]) > ret["meta"]["n_found"]:
                return False

            print(self.data["molecule_map"])
            sizing = collections.defaultdict([])
            skeleton

            # inv_molecule_map = {v : k for k, v in self.data["molecule_map"]}
            # for result in ret["data"]:

            return False
            # crankAPI.update_state(self.crank_state, job_results)


        # Figure out if we are still waiting on jobs

        # Create new jobs from the current state
        next_jobs = crankAPI.next_jobs_from_state(self.data["crank_state"], verbose=True)

        self.submit_geometric_tasks(next_jobs)

        # step 4
        # job_results = collections.defaultdict(list)
        # for grid_id_str, job_geo_list in next_jobs.items():
        #     for job_geo in job_geo_list:
        #         dihedral_values = crankAPI.grid_id_from_string(grid_id_str)

        #         # Run geometric
        #         geometric_input_dict = self.make_geomeTRIC_input(dihedral_values, job_geo)
        #         geometric_output_dict = geometric.run_json.geometric_run_json(geometric_input_dict)

        #         # Pull out relevevant data
        #         final_geo = geometric_output_dict['final_molecule']['molecule']['geometry']
        #         final_energy = geometric_output_dict['final_molecule']['properties']['return_energy']

        #         # Note: the results should be appended in the same order as in the inputs
        #         # It's not a problem here when running serial for loop
        #         job_results[grid_id_str].append((job_geo, final_geo, final_energy))
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
                flat_map[(v, num)] = mol

        # Add molecules and grab hashes
        ret = self.db_socket.add_molecules(flat_map)

        # Check if everything was successful

        # Prepare optimization runs
        packet = {
            "meta": {
                "procedure": "optimization",
                "options": "none",
                "program": "geometric",
                "qc_meta": self.data["qc_meta"]
            },
            "data": list(ret["data"].values()),
        }
        print("\nPacket input")
        print(json.dumps(packet, indent=2))
        full_tasks, errors = procedures.get_procedure_input_parser("optimization")(self.db_socket, packet)

        # Create data for next round
        self.data["molecule_map"] = {v: k for k, v in ret["data"].items()}
        self.data["required_jobs"] = packet["meta"]
        self.data["required_jobs"]["initial_molecules"] = list(ret["data"].values())

        # Add tasks to Nanny
        submitted = self.queue_socket.submit_tasks(full_tasks)


    def finalize():
        # Add finalize state
        # Parse remaining procedures
        # Create a map of "jobs" so that procedures does not have to followed
        self.data["state"] = "FINISHED"
        print("Crank Scan Finished")
        return crankAPI.collect_lowest_energies(self.crank_state)
