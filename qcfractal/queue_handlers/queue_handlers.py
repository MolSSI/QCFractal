"""
Queue backend abstraction manager.
"""

import logging
import traceback
import collections

from ..web_handlers import APIHandler
from .. import procedures
from .. import services


class TaskQueueHandler(APIHandler):
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
        self.logger.info("TaskQueue: Added {} tasks.".format(ret["meta"]["n_inserted"]))

        ret["data"] = {"submitted": ret["data"], "completed": list(complete_jobs), "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["db_socket"].set_project(header["project"])
    #     queue_nanny = self.objects["queue_nanny"]
    #     ret = {}
    #     ret["queue"] = list(queue_nanny.queue)
    #     ret["error"] = queue_nanny.errors
    #     self.write(ret)


class ServiceQueueHandler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):
        """Posts new services to the service queue
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
        self.logger.info("ServiceQueue: Added {} services.\n".format(ret["meta"]["n_inserted"]))

        ret["data"] = {"submitted": ret["data"], "completed": list(complete_jobs), "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        # Return anything of interest
        # meta["success"] = True
        # meta["n_inserted"] = len(submitted)
        # meta["errors"] = []  # TODO
        # ret = {"meta": meta, "data": submitted}

        self.write(ret)


class QueueAPIHandler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    Manages the external
    """

    @staticmethod
    def insert_complete_tasks(storage_socket, results, logger):
        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(list)
        error_data = []

        task_success = 0
        task_failures = 0
        for key, (result, parser, hooks) in results.items():
            try:

                # Successful job
                if result["success"] is True:
                    result["queue_id"] = key
                    new_results[parser].append((result, hooks))
                    task_success += 1

                # Failed job
                else:
                    if "error" in result:
                        error = result["error"]
                    else:
                        error = "No error supplied"

                    logger.info("Computation key did not complete successfully:\n\t{}\n"
                                "Because: {}".format(str(key), error))

                    error_data.append((key, error))
                    task_failures += 1
            except Exception as e:
                msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                logger.info("update: ERROR\n{}".format(msg))
                error_data.append((key, msg))
                task_failures += 1

        logger.info("QueueManager: Added {} successful tasks, {} failed.".format(task_success, task_failures))

        # Run output parsers
        completed = []
        hooks = []
        for k, v in new_results.items():
            com, err, hks = procedures.get_procedure_output_parser(k)(storage_socket, v)
            completed.extend(com)
            error_data.extend(err)
            hooks.extend(hks)

        # Handle hooks and complete jobs
        storage_socket.handle_hooks(hooks)
        storage_socket.queue_mark_complete(completed)
        storage_socket.queue_mark_error(error_data)
        return (len(completed), len(error_data))

    def get(self):
        # Add new jobs to queue
        self.authenticate("queue")

        new_jobs = self.storage_socket.queue_get_next(n=open_slots)
        self.queue_adapter.submit_tasks(new_jobs)

    def post(self):
        """Summary
        """
        self.authenticate("queue")

    def update(self):
        """
        """
        self.authenticate("queue")
        return
