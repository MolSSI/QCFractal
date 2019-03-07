"""
Queue backend abstraction manager.
"""

import collections
import traceback

from ..interface.models.rest_models import (
    QueueManagerGETBody, QueueManagerGETResponse, QueueManagerPOSTBody, QueueManagerPOSTResponse, QueueManagerPUTBody,
    QueueManagerPUTResponse, ServiceQueueGETBody, ServiceQueueGETResponse, ServiceQueuePOSTBody,
    ServiceQueuePOSTResponse, TaskQueueGETBody, TaskQueueGETResponse, TaskQueuePOSTBody, TaskQueuePOSTResponse)
from ..procedures import get_procedure_parser
from ..services import initialize_service
from ..web_handlers import APIHandler


class TaskQueueHandler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    _required_auth = "compute"

    def post(self):
        """Summary
        """

        # Grab objects
        storage = self.objects["storage_socket"]

        post = TaskQueuePOSTBody.parse_raw(self.request.body)

        # Format and submit tasks
        procedure_parser = get_procedure_parser(post.meta["procedure"], storage, self.logger)
        payload = procedure_parser.submit_tasks(post)

        response = TaskQueuePOSTResponse(**payload)
        self.logger.info("TaskQueue: Added {} tasks.".format(response.meta.n_inserted))

        self.write(response.json())

    def get(self):
        """Posts new services to the service queue
        """

        # Grab objects
        storage = self.objects["storage_socket"]

        body = TaskQueueGETBody.parse_raw(self.request.body)
        tasks = storage.get_queue(**body.data, projection=body.meta.projection)
        response = TaskQueueGETResponse(**tasks)

        self.write(response.json())


class ServiceQueueHandler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    _required_auth = "compute"

    def post(self):
        """Posts new services to the service queue
        """

        # Grab objects
        storage = self.objects["storage_socket"]

        new_services = []
        for service_input in ServiceQueuePOSTBody.parse_raw(self.request.body).data:
            # Get molecules with ids
            if isinstance(service_input.initial_molecule, list):
                molecules = storage.get_add_molecules_mixed(service_input.initial_molecule)["data"]
                if len(molecules) != len(service_input.initial_molecule):
                    raise KeyError("We should catch this error.")
            else:
                molecules = storage.get_add_molecules_mixed([service_input.initial_molecule])["data"][0]

            # Update the input and build a service object
            service_input = service_input.copy(update={"initial_molecule": molecules})
            new_services.append(initialize_service(storage, self.logger, service_input))

        ret = storage.add_services(new_services)
        ret["data"] = {"ids": ret["data"], "existing": ret["meta"]["duplicates"]}
        ret["data"]["submitted"] = list(set(ret["data"]["ids"]) - set(ret["meta"]["duplicates"]))

        response = ServiceQueuePOSTResponse(**ret)
        self.logger.info("ServiceQueue: Added {} services.\n".format(response.meta.n_inserted))

        self.write(response.json())

    def get(self):
        """Gets services from the service queue
        """

        # Grab objects
        storage = self.objects["storage_socket"]

        body = ServiceQueueGETBody.parse_raw(self.request.body)

        projection = {x: True for x in ["status", "error", "tag"]}
        ret = storage.get_services(**body.data, projection=projection)
        response = ServiceQueueGETResponse(**ret)

        self.write(response.json())


class QueueManagerHandler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    Manages the external
    """
    _required_auth = "queue"

    def _get_name_from_metadata(self, meta):
        """
        Form the canonical name string.
        """
        ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
        return ret

    @staticmethod
    def insert_complete_tasks(storage_socket, results, logger):
        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(list)

        queue = storage_socket.get_queue(ids=results.keys())["data"]
        queue = {v["id"]: v for v in queue}

        error_data = []

        task_success = 0
        task_failures = 0
        task_totals = len(results.items())
        for key, result in results.items():
            try:
                # Successful task
                if result["success"] is False:
                    if "error" not in result:
                        error = {"error_type": "not_supplied", "error_message": "No error message found on task."}
                    else:
                        error = result["error"]

                    logger.warning("Computation key {key} did not complete successfully:\n"
                                   "error_type: {error_type}\nerror_message: {error_message}".format(
                                       key=str(key), **error))

                    error_data.append((key, error))
                    task_failures += 1

                # Failed task
                elif key not in queue:
                    logger.warning(f"Computation key {key} completed successfully, but not found in queue.")
                    error_data.append((key, "Internal Error: Queue key not found."))
                    task_failures += 1

                # Success!
                else:
                    parser = queue[key]["parser"]
                    new_results[parser].append({
                        "result": result,
                        "task_id": key,
                        "base_result": queue[key]["base_result"]
                    })
                    task_success += 1

            except Exception as e:
                msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                logger.warning("update: ERROR\n{}".format(msg))
                error_data.append((key, msg))
                task_failures += 1

        if task_totals:
            logger.info("QueueManager: Found {} complete tasks ({} successful, {} failed).".format(
                task_totals, task_success, task_failures))

        # Run output parsers
        completed = []
        for k, v in new_results.items():
            procedure_parser = get_procedure_parser(k, storage_socket, logger)
            com, err, hks = procedure_parser.parse_output(v)
            completed.extend(com)
            error_data.extend(err)

        # Handle complete tasks
        storage_socket.queue_mark_complete(completed)
        storage_socket.queue_mark_error(error_data)
        return len(completed), len(error_data)

    def get(self):
        """Pulls new tasks from the Servers queue
        """

        # Grab objects
        storage = self.objects["storage_socket"]

        body = QueueManagerGETBody.parse_raw(self.request.body)

        # Figure out metadata and kwargs
        name = self._get_name_from_metadata(body.meta)
        queue_tags = {
            "limit": body.data.limit,
            "tag": body.meta.tag,
        }  # yapf: disable

        # Grab new tasks and write out
        new_tasks = storage.queue_get_next(name, **queue_tags)
        response = QueueManagerGETResponse(
            meta={"n_found": len(new_tasks),
                  "success": True,
                  "errors": [],
                  "error_description": "",
                  "missing": []},
            data=new_tasks)
        self.write(response.json())
        self.logger.info("QueueManager: Served {} tasks.".format(response.meta.n_found))

        # Update manager logs
        storage.manager_update(name, submitted=len(new_tasks), **body.meta.dict())

    def post(self):
        """Posts complete tasks to the Servers queue
        """

        # Grab objects
        storage = self.objects["storage_socket"]

        body = QueueManagerPOSTBody.parse_raw(self.request.body)
        ret = self.insert_complete_tasks(storage, body.data, self.logger)

        response = QueueManagerPOSTResponse(
            meta={
                "n_inserted": ret[0],
                "duplicates": [],
                "validation_errors": [],
                "success": True,
                "errors": [],
                "error_description": "" if not ret[1] else "{} errors".format(ret[1])
            },
            data=True)
        self.write(response.json())
        self.logger.info("QueueManager: Acquired {} complete tasks.".format(len(body.data)))

        # Update manager logs
        name = self._get_name_from_metadata(body.meta)
        storage.manager_update(name, completed=len(body.data), **body.meta.dict())

    def put(self):
        """
        Various manager manipulation operations
        """

        storage = self.objects["storage_socket"]
        ret = True

        body = QueueManagerPUTBody.parse_raw(self.request.body)
        name = self._get_name_from_metadata(body.meta)
        op = body.data.operation
        if op == "startup":
            storage.manager_update(name, status="ACTIVE", **body.meta.dict())
            self.logger.info("QueueManager: New active manager {} detected.".format(name))

        elif op == "shutdown":
            nshutdown = storage.queue_reset_status(name)
            storage.manager_update(name, returned=nshutdown, status="INACTIVE", **body.meta.dict())

            self.logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(
                name, nshutdown))

            ret = {"nshutdown": nshutdown}

        elif op == "heartbeat":
            storage.manager_update(name, status="ACTIVE", **body.meta.dict())
            self.logger.info("QueueManager: Heartbeat of manager {} detected.".format(name))

        else:
            msg = "Operation '{}' not understood.".format(op)
            from tornado.web import HTTPError
            raise HTTPError(status_code=400, reason=msg)

        response = QueueManagerPUTResponse(meta={}, data=ret)
        self.write(response.json())

        # Update manager logs
