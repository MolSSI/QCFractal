"""
Queue backend abstraction manager.
"""

import logging
import traceback
import collections

from ..web_handlers import APIHandler
from .. import procedures
from .. import services


class TaskQueue(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):
        """Summary
        """
        self.authenticate("compute")

        # Grab objects
        storage = self.objects["storage_socket"]

        # Format tasks
        func = procedures.get_procedure_input_parser(self.json["meta"]["procedure"])
        full_tasks, complete_jobs, errors = func(storage, self.json)

        # Add tasks to queue
        ret = storage.queue_submit(full_tasks)
        self.logger.info("QUEUE: Added {} tasks.".format(ret["meta"]["n_inserted"]))

        ret["data"] = {"submitted": ret["data"], "completed": list(complete_jobs), "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["db_socket"].set_project(header["project"])
    #     queue_manager = self.objects["queue_manager"]
    #     ret = {}
    #     ret["queue"] = list(queue_manager.queue)
    #     ret["error"] = queue_manager.errors
    #     self.write(ret)


class ServiceQueue(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):
        """Summary
        """
        self.authenticate("compute")

        # Grab objects
        storage = self.objects["storage_socket"]

        # Figure out initial molecules
        errors = []
        ordered_mol_dict = {x: mol for x, mol in enumerate(self.json["data"])}
        mol_query = storage.mixed_molecule_get(ordered_mol_dict)

        # Build out services
        submitted_services = []
        for idx, mol in mol_query["data"].items():
            tmp = services.initializer(self.json["meta"]["service"], storage, self.json["meta"], mol)
            submitted_services.append(tmp)

        # Figure out complete services
        service_hashes = [x.data["hash_index"] for x in submitted_services]
        found_hashes = storage.get_procedures({"hash_index": service_hashes}, projection={"hash_index": True})
        found_hashes = set(x["hash_index"] for x in found_hashes["data"])

        new_services = []
        complete_jobs = []
        for x in submitted_services:
            hash_index = x.data["hash_index"]

            if hash_index in found_hashes:
                complete_jobs.append(hash_index)
            else:
                new_services.append(x)

        # Add services to database
        ret = storage.add_services([service.get_json() for service in new_services])
        self.logger.info("QUEUE: Added {} services.\n".format(ret["meta"]["n_inserted"]))

        ret["data"] = {"submitted": ret["data"], "completed": list(complete_jobs), "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        # Return anything of interest
        # meta["success"] = True
        # meta["n_inserted"] = len(submitted)
        # meta["errors"] = []  # TODO
        # ret = {"meta": meta, "data": submitted}

        self.write(ret)
