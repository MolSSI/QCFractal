"""
Queue backend abstraction manager.
"""

import collections
import traceback

from .. import procedures
from .. import services
from ..web_handlers import APIHandler


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
        full_tasks, complete_tasks, errors = func(storage, self.json)

        # Add tasks to queue
        ret = storage.queue_submit(full_tasks)
        self.logger.info("TaskQueue: Added {} tasks.".format(ret["meta"]["n_inserted"]))

        ret["data"] = [x for x in ret["data"] if x is not None]
        ret["data"] = {"submitted": ret["data"], "completed": list(complete_tasks), "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        self.write(ret)

    def get(self):
        """Posts new services to the service queue
        """
        self.authenticate("read")

        # Grab objects
        storage = self.objects["storage_socket"]

        projection = self.json["meta"].get("projection", None)
        if projection is None:
            projection = {x: True for x in ["status", "error_message", "tag"]}

        ret = storage.get_queue(self.json["data"], projection=projection)

        self.write(ret)


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
        complete_tasks = []
        for x in submitted_services:
            hash_index = x.data["hash_index"]

            if hash_index in found_hashes:
                complete_tasks.append(hash_index)
            else:
                new_services.append(x)

        # Add services to database
        ret = storage.add_services([service.get_json() for service in new_services])
        self.logger.info("ServiceQueue: Added {} services.\n".format(ret["meta"]["n_inserted"]))

        ret["data"] = {"submitted": ret["data"], "completed": list(complete_tasks), "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        self.write(ret)

    def get(self):
        """Posts new services to the service queue
        """
        self.authenticate("read")

        # Grab objects
        storage = self.objects["storage_socket"]

        projection = {x: True for x in ["status", "error_message", "tag"]}
        ret = storage.get_services(self.json["data"], projection=projection)

        self.write(ret)


class QueueManagerHandler(APIHandler):
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
        task_totals = len(results.items())
        for key, (result, parser, hooks) in results.items():
            try:

                # Successful task
                if result["success"] is True:
                    result["queue_id"] = key
                    new_results[parser].append((result, hooks))
                    task_success += 1

                # Failed task
                else:
                    if "error" in result:
                        logger.warning(
                            "Found old-style error field, please change to 'error_message'. Will be deprecated")
                        error = result["error"]
                        result["error_message"] = error
                    elif "error_message" in result:
                        error = result["error_message"]
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

        if task_totals:
            logger.info("QueueManager: Found {} complete tasks ({} successful, {} failed).".format(
                task_totals, task_success, task_failures))

        # Run output parsers
        completed = []
        hooks = []
        for k, v in new_results.items():
            com, err, hks = procedures.get_procedure_output_parser(k)(storage_socket, v)
            completed.extend(com)
            error_data.extend(err)
            hooks.extend(hks)

        # Handle hooks and complete tasks
        storage_socket.handle_hooks(hooks)
        storage_socket.queue_mark_complete(completed)
        storage_socket.queue_mark_error(error_data)
        return len(completed), len(error_data)

    def get(self):
        """Pulls new tasks from the Servers queue
        """
        self.authenticate("queue")

        # Grab objects
        storage = self.objects["storage_socket"]

        # Figure out metadata and kwargs
        name = self.json["meta"]["name"]
        tag = self.json["meta"].get("tag", None)
        kwargs = {
            "limit": self.json["meta"].get("limit", 100),
            "tag": tag,
        } # yapf: disable

        # Grab new tasks and write out
        new_tasks = storage.queue_get_next(**kwargs)
        self.write({"meta": {"n_found": len(new_tasks), "success": True}, "data": new_tasks})
        self.logger.info("QueueManager: Served {} tasks.".format(len(new_tasks)))

        # Update manager logs
        storage.manager_update(name, tag=tag, submitted=len(new_tasks))

    def post(self):
        """Posts complete tasks to the Servers queue
        """
        self.authenticate("queue")

        # Grab objects
        storage = self.objects["storage_socket"]

        ret = self.insert_complete_tasks(storage, self.json["data"], self.logger)
        self.write({"meta": {"n_inserted": ret[0]}, "data": True})
        self.logger.info("QueueManager: Aquired {} complete tasks.".format(len(self.json["data"])))

        # Update manager logs
        name = self.json["meta"]["name"]
        tag = self.json["meta"].get("tag", None)
        storage.manager_update(name, tag=tag, completed=len(self.json["data"]))

    def put(self):
        """
        """
        self.authenticate("queue")

        storage = self.objects["storage_socket"]

        storage.queue_reset_status(self.json["data"])
        self.write({"meta": {}, "data": True})

        # Update manager logs
        name = self.json["meta"]["name"]
        storage.manager_update(name, returned=len(self.json["data"]))
        self.logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(
            name, len(self.json["data"])))
