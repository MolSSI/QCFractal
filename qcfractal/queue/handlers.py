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
        procedure_parser = procedures.get_procedure_parser(self.json["meta"]["procedure"], storage)
        full_tasks, complete_tasks, errors = procedure_parser.parse_input(self.json)

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
            projection = {x: True for x in ["status", "error", "tag"]}

        ret = storage.get_queue(**self.json["data"], projection=projection)

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
        mol_query = storage.get_add_molecules_mixed(self.json["data"])

        # Build out services
        submitted_services = []
        for idx, mol in mol_query["data"].items():
            tmp = services.initializer(self.json["meta"]["service"], storage, self.json["meta"], mol)
            submitted_services.append(tmp)

        # Figure out complete services
        service_hashes = [x.hash_index for x in submitted_services]
        found_hashes = storage.get_procedures_by_id(hash_index=service_hashes, projection={"hash_index": True})
        found_hashes = set(x["hash_index"] for x in found_hashes["data"])

        new_services = []
        complete_tasks = []
        for x in submitted_services:
            hash_index = x.hash_index

            if hash_index in found_hashes:
                complete_tasks.append(hash_index)
            else:
                new_services.append(x)

        # Add services to database
        ret = storage.add_services([service.json_dict() for service in new_services])
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

    def _get_name_from_metadata(self, meta):
        """
        Form the canonical name string.
        """
        return meta["cluster"] + "-" + meta["hostname"] + "-" + meta["uuid"]

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
                    result["task_id"] = key
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
                                "Because: {}".format(str(key), error["error_message"]))

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
        for k, v in new_results.items():  # todo: can be merged? do they have diff k?
            procedure_parser = procedures.get_procedure_parser(k,storage_socket)
            com, err, hks = procedure_parser.parse_output(v)
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
        name = self._get_name_from_metadata(self.json["meta"])
        queue_tags = {
            "limit": self.json["data"].get("limit", 100),
            "tag": self.json["meta"]["tag"],
        } # yapf: disable

        # Grab new tasks and write out
        new_tasks = storage.queue_get_next(name, **queue_tags)
        self.write({"meta": {"n_found": len(new_tasks), "success": True}, "data": new_tasks})
        self.logger.info("QueueManager: Served {} tasks.".format(len(new_tasks)))

        # Update manager logs
        storage.manager_update(name, submitted=len(new_tasks), **self.json["meta"])

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
        name = self._get_name_from_metadata(self.json["meta"])
        storage.manager_update(name, completed=len(self.json["data"]), **self.json["meta"])

    def put(self):
        """
        Various manager manipulation operations
        """
        self.authenticate("queue")

        storage = self.objects["storage_socket"]
        ret = True

        name = self._get_name_from_metadata(self.json["meta"])
        if self.json["data"]["operation"] == "startup":
            name = self._get_name_from_metadata(self.json["meta"])
            storage.manager_update(name, status="ACTIVE", **self.json["meta"])
            self.logger.info("QueueManager: New active manager {} detected.".format(name))

        elif self.json["data"]["operation"] == "shutdown":
            nshutdown = storage.queue_reset_status(name)
            storage.manager_update(name, returned=nshutdown, status="INACTIVE", **self.json["meta"])

            self.logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(
                name, nshutdown))

            ret = {"nshutdown": nshutdown}

        elif self.json["data"]["operation"] == "heartbeat":
            name = self._get_name_from_metadata(self.json["meta"])
            storage.manager_update(name, status="ACTIVE", **self.json["meta"])
            self.logger.info("QueueManager: Heartbeat of manager {} detected.".format(name))

        else:
            msg = "Operation '{}' not understood.".format(self.json["data"]["operation"])
            raise tornado.web.HTTPError(status_code=400, reason=msg)
        self.write({"meta": {}, "data": ret})

        # Update manager logs
