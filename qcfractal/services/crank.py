"""
Wraps geometric procedures
"""

import copy

from crank import crankAPI


class Crank:
    def __init__(self, db_socket, queue_socket, data):

        # Server interaction
        self.db_socket = db_socket
        self.queue_socket = queue_socket

        # Unpack data
        self.data = data

    @classmethod
    def initialize_from_api(cls, db_socket, queue_socket, meta, molecule):

        # Copy initial intial input and build out a crank_state
        meta = copy.deepcopy(meta)
        molecule = copy.deepcopy(molecule)
        meta["crank_state"] = crankAPI.create_initial_state(
            dihedrals=meta["crank_meta"]["dihedrals"],
            grid_spacing=meta["crank_meta"]["grid_spacing"],
            elements=molecule["symbols"],
            init_coords=molecule["geometry"])

        # Save initial molecule and add hash
        meta["initial_molecule"] = self.db_socket.add_molecules({"ret": molecule})["data"]["ret"]
        meta["state"] = "READY"



        return cls(db_socket, queue_socket, meta)

    def iterate():
        next_jobs = crankAPI.next_jobs_from_state(self.crank_state, verbose=True)

        # step 3
        if len(next_jobs) == 0:
            print("Crank Scan Finished")
            return crankAPI.collect_lowest_energies(self.crank_state)

        # step 4
        job_results = collections.defaultdict(list)
        for grid_id_str, job_geo_list in next_jobs.items():
            for job_geo in job_geo_list:
                dihedral_values = crankAPI.grid_id_from_string(grid_id_str)

                # Run geometric
                geometric_input_dict = self.make_geomeTRIC_input(dihedral_values, job_geo)
                geometric_output_dict = geometric.run_json.geometric_run_json(geometric_input_dict)

                # Pull out relevevant data
                final_geo = geometric_output_dict['final_molecule']['molecule']['geometry']
                final_energy = geometric_output_dict['final_molecule']['properties']['return_energy']

                # Note: the results should be appended in the same order as in the inputs
                # It's not a problem here when running serial for loop
                job_results[grid_id_str].append((job_geo, final_geo, final_energy))

        # step 5
        crankAPI.update_state(self.crank_state, job_results)

        # Save crank state

    def submit


    def finalize():
        # Add finalize state
        # Parse remaining procedures
        # Create a map of "jobs" so that procedures does not have to followed
        pass
