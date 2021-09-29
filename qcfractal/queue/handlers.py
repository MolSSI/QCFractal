"""
Queue backend abstraction manager.
"""

import collections
import traceback

import tornado.web

from ..interface.models.rest_models import rest_model
from ..interface.models.task_models import PriorityEnum, TaskStatusEnum
from ..interface.models.records import RecordStatusEnum
from ..interface.models.model_builder import build_procedure
from ..procedures import check_procedure_available, get_procedure_parser
from ..services import initialize_service
from ..web_handlers import APIHandler


class TaskQueueHandler(APIHandler):
    """
    Handles task management (querying/adding/modifying tasks)
    """

    _required_auth = "compute"

    def post(self):
        """Posts new tasks to the task queue."""

        body_model, response_model = rest_model("task_queue", "post")
        body = self.parse_bodymodel(body_model)

        # Format and submit tasks
        if not check_procedure_available(body.meta.procedure):
            raise tornado.web.HTTPError(status_code=400, reason="Unknown procedure {}.".format(body.meta.procedure))

        procedure_parser = get_procedure_parser(body.meta.procedure, self.storage, self.logger)

        # Verify the procedure
        verify = procedure_parser.verify_input(body)
        if verify is not True:
            raise tornado.web.HTTPError(status_code=400, reason=verify)

        payload = procedure_parser.submit_tasks(body)
        response = response_model(**payload)

        self.logger.info("POST: TaskQueue -  Added {} tasks.".format(response.meta.n_inserted))
        self.write(response)

    def get(self):
        """Gets task information from the task queue"""

        body_model, response_model = rest_model("task_queue", "get")
        body = self.parse_bodymodel(body_model)

        tasks = self.storage.get_queue(**{**body.data.dict(), **body.meta.dict()})
        response = response_model(**tasks)

        self.logger.info("GET: TaskQueue - {} pulls.".format(len(response.data)))
        self.write(response)

    def put(self):
        """Modifies tasks in the task queue"""

        body_model, response_model = rest_model("task_queue", "put")
        body = self.parse_bodymodel(body_model)

        if (body.data.id is None) and (body.data.base_result is None):
            raise tornado.web.HTTPError(status_code=400, reason="Id or ResultId must be specified.")

        if body.meta.operation == "restart":
            d = body.data.dict()
            d.pop("new_tag", None)
            d.pop("new_priority", None)
            tasks_updated = self.storage.queue_reset_status(**d, reset_error=True)
            data = {"n_updated": tasks_updated}
        elif body.meta.operation == "regenerate":
            tasks_updated = 0
            result_data = self.storage.get_procedures(id=body.data.base_result)["data"]

            new_tag = body.data.new_tag
            if body.data.new_priority is None:
                new_priority = PriorityEnum.NORMAL
            else:
                new_priority = PriorityEnum(int(body.data.new_priority))

            for r in result_data:
                model = build_procedure(r)

                # Only regenerate the task if the base record is not complete
                # This will not do anything if the task already exists
                if model.status != RecordStatusEnum.complete:
                    procedure_parser = get_procedure_parser(model.procedure, self.storage, self.logger)

                    task_info = procedure_parser.create_tasks([model], tag=new_tag, priority=new_priority)
                    n_inserted = task_info["meta"]["n_inserted"]
                    tasks_updated += n_inserted

                    # If we inserted a new task, then also reset base result statuses
                    # (ie, if it was running, then it obviously isn't since we made a new task)
                    if n_inserted > 0:
                        self.storage.reset_base_result_status(id=body.data.base_result)

                data = {"n_updated": tasks_updated}
        elif body.meta.operation == "modify":
            tasks_updated = self.storage.queue_modify_tasks(
                id=body.data.id,
                base_result=body.data.base_result,
                new_tag=body.data.new_tag,
                new_priority=body.data.new_priority,
            )
            data = {"n_updated": tasks_updated}
        else:
            raise tornado.web.HTTPError(status_code=400, reason=f"Operation '{operation}' is not valid.")

        response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

        self.logger.info(f"PUT: TaskQueue - Operation: {body.meta.operation} - {tasks_updated}.")
        self.write(response)


class ServiceQueueHandler(APIHandler):
    """
    Handles service management (querying/add/modifying)
    """

    _required_auth = "compute"

    def post(self):
        """Posts new services to the service queue."""

        body_model, response_model = rest_model("service_queue", "post")
        body = self.parse_bodymodel(body_model)

        new_services = []
        for service_input in body.data:
            # Get molecules with ids
            if isinstance(service_input.initial_molecule, list):
                molecules = self.storage.get_add_molecules_mixed(service_input.initial_molecule)["data"]
                if len(molecules) != len(service_input.initial_molecule):
                    raise KeyError("We should catch this error.")
            else:
                molecules = self.storage.get_add_molecules_mixed([service_input.initial_molecule])["data"][0]

            # Update the input and build a service object
            service_input = service_input.copy(update={"initial_molecule": molecules})
            new_services.append(
                initialize_service(
                    self.storage, self.logger, service_input, tag=body.meta.tag, priority=body.meta.priority
                )
            )

        ret = self.storage.add_services(new_services)
        ret["data"] = {"ids": ret["data"], "existing": ret["meta"]["duplicates"]}
        ret["data"]["submitted"] = list(set(ret["data"]["ids"]) - set(ret["meta"]["duplicates"]))
        response = response_model(**ret)

        self.logger.info("POST: ServiceQueue -  Added {} services.\n".format(response.meta.n_inserted))
        self.write(response)

    def get(self):
        """Gets information about services from the service queue."""

        body_model, response_model = rest_model("service_queue", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_services(**{**body.data.dict(), **body.meta.dict()})
        response = response_model(**ret)

        self.logger.info("GET: ServiceQueue - {} pulls.\n".format(len(response.data)))
        self.write(response)

    def put(self):
        """Modifies services in the service queue"""

        body_model, response_model = rest_model("service_queue", "put")
        body = self.parse_bodymodel(body_model)

        if (body.data.id is None) and (body.data.procedure_id is None):
            raise tornado.web.HTTPError(status_code=400, reason="Id or ProcedureId must be specified.")

        if body.meta.operation == "restart":
            updates = self.storage.update_service_status("running", **body.data.dict())
            data = {"n_updated": updates}
        else:
            raise tornado.web.HTTPError(status_code=400, reason=f"Operation '{operation}' is not valid.")

        response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

        self.logger.info(f"PUT: TaskQueue - Operation: {body.meta.operation} - {updates}.")
        self.write(response)


class QueueManagerHandler(APIHandler):
    """
    Manages the task queue.

    Used by compute managers for getting tasks, posting completed tasks, etc.
    """

    _required_auth = "queue"

    @staticmethod
    def _get_name_from_metadata(meta):
        """
        Form the canonical name string.
        """
        ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
        return ret

    @staticmethod
    def insert_complete_tasks(storage_socket, body, logger):

        results = body.data
        meta = body.meta
        task_ids = list(results.keys())

        manager_name = QueueManagerHandler._get_name_from_metadata(meta)
        logger.info("QueueManager: Received completed tasks from {}.".format(manager_name))
        logger.info("              Task ids: " + " ".join(task_ids))

        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(list)

        queue = storage_socket.get_queue(id=task_ids)["data"]
        queue = {v.id: v for v in queue}

        error_data = []

        task_success = 0
        task_failures = 0
        task_totals = len(results.items())

        for task_id, result in results.items():
            try:
                #################################################################
                # Perform some checks for consistency
                #################################################################
                existing_task_data = queue.get(task_id, None)

                # For the first three checks, don't add an error to error_data
                # We don't want to modify the queue in these cases

                # Does the task exist?
                if existing_task_data is None:
                    logger.warning(f"Task id {task_id} does not exist in the task queue.")
                    task_failures += 1

                # Is the task in the running state
                elif existing_task_data.status != TaskStatusEnum.running:
                    logger.warning(f"Task id {task_id} is not in the running state.")
                    task_failures += 1

                # Was the manager that sent the data the one that was assigned?
                elif existing_task_data.manager != manager_name:
                    logger.warning(f"Task id {task_id} belongs to {existing_task_data.manager}, not this manager")
                    task_failures += 1

                # Failed task
                elif result["success"] is False:
                    if "error" not in result:
                        error = {"error_type": "not_supplied", "error_message": "No error message found on task."}
                    else:
                        error = result["error"]

                    logger.debug(
                        "Task id {key} did not complete successfully:\n"
                        "error_type: {error_type}\nerror_message: {error_message}".format(key=str(task_id), **error)
                    )

                    error_data.append((task_id, error))
                    task_failures += 1

                # Success!
                else:
                    parser = queue[task_id].parser
                    new_results[parser].append(
                        {"result": result, "task_id": task_id, "base_result": queue[task_id].base_result}
                    )
                    task_success += 1

            except Exception:
                msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                error = {"error_type": "internal_fractal_error", "error_message": msg}
                logger.error("update: ERROR\n{}".format(msg))
                error_data.append((task_id, error))
                task_failures += 1

        if task_totals:
            logger.info(
                "QueueManager: Found {} complete tasks ({} successful, {} failed).".format(
                    task_totals, task_success, task_failures
                )
            )

        # Run output parsers and handle completed tasks
        completed = []
        for k, v in new_results.items():
            procedure_parser = get_procedure_parser(k, storage_socket, logger)
            com = procedure_parser.handle_completed_output(v)
            completed.extend(com)

        storage_socket.queue_mark_error(error_data)
        return len(completed), len(error_data)

    def get(self):
        """Pulls new tasks from the task queue"""

        body_model, response_model = rest_model("queue_manager", "get")
        body = self.parse_bodymodel(body_model)

        # Figure out metadata and kwargs
        name = self._get_name_from_metadata(body.meta)

        # Grab new tasks and write out
        new_tasks = self.storage.queue_get_next(
            name, body.meta.programs, body.meta.procedures, limit=body.data.limit, tag=body.meta.tag
        )
        response = response_model(
            **{
                "meta": {
                    "n_found": len(new_tasks),
                    "success": True,
                    "errors": [],
                    "error_description": "",
                    "missing": [],
                },
                "data": new_tasks,
            }
        )
        self.write(response)

        self.logger.info("QueueManager: Served {} tasks.".format(response.meta.n_found))

        # Update manager logs
        self.storage.manager_update(name, submitted=len(new_tasks), **body.meta.dict())

    def post(self):
        """Posts complete tasks to the task queue"""

        body_model, response_model = rest_model("queue_manager", "post")
        body = self.parse_bodymodel(body_model)

        success, error = self.insert_complete_tasks(self.storage, body, self.logger)

        completed = success + error

        response = response_model(
            **{
                "meta": {
                    "n_inserted": completed,
                    "duplicates": [],
                    "validation_errors": [],
                    "success": True,
                    "errors": [],
                    "error_description": "",
                },
                "data": True,
            }
        )
        self.write(response)
        self.logger.info("QueueManager: Inserted {} complete tasks.".format(len(body.data)))

        # Update manager logs
        name = self._get_name_from_metadata(body.meta)
        self.storage.manager_update(name, completed=completed, failures=error)

    def put(self):
        """
        Various manager manipulation operations
        """

        ret = True

        body_model, response_model = rest_model("queue_manager", "put")
        body = self.parse_bodymodel(body_model)

        name = self._get_name_from_metadata(body.meta)
        op = body.data.operation
        if op == "startup":
            self.storage.manager_update(
                name, status="ACTIVE", configuration=body.data.configuration, **body.meta.dict(), log=True
            )
            self.logger.info("QueueManager: New active manager {} detected.".format(name))

        elif op == "shutdown":
            nshutdown = self.storage.queue_reset_status(manager=name, reset_running=True)
            self.storage.manager_update(name, returned=nshutdown, status="INACTIVE", **body.meta.dict(), log=True)

            self.logger.info(
                "QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(name, nshutdown)
            )

            ret = {"nshutdown": nshutdown}

        elif op == "heartbeat":
            self.storage.manager_update(name, status="ACTIVE", **body.meta.dict(), log=True)
            self.logger.debug("QueueManager: Heartbeat of manager {} detected.".format(name))

        else:
            msg = "Operation '{}' not understood.".format(op)
            raise tornado.web.HTTPError(status_code=400, reason=msg)

        response = response_model(**{"meta": {}, "data": ret})
        self.write(response)

        # Update manager logs
        # TODO: ????


class ComputeManagerHandler(APIHandler):
    """
    Handles management/status querying of managers
    """

    _required_auth = "admin"

    def get(self):
        """Gets manager information from the task queue"""

        body_model, response_model = rest_model("manager", "get")
        body = self.parse_bodymodel(body_model)

        self.logger.info("GET: ComputeManagerHandler")
        managers = self.storage.get_managers(**{**body.data.dict(), **body.meta.dict()})

        # remove passwords?
        # TODO: Are passwords stored anywhere else? Other kinds of passwords?
        for m in managers["data"]:
            if "configuration" in m and isinstance(m["configuration"], dict) and "server" in m["configuration"]:
                m["configuration"]["server"].pop("password", None)

        response = response_model(**managers)
        self.write(response)
