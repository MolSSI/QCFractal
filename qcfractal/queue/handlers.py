"""
Queue backend abstraction manager.
"""

import collections
import traceback

from .. import procedures
from .. import services
from ..web_handlers import APIHandler

from pydantic import BaseModel
from typing import Dict

from ..interface.models.common_models import Molecule, json_encoders
from ..interface.models.rest_models import (
    ResponseGETMeta,
    TaskQueueGETBody, TaskQueueGETResponse, TaskQueuePOSTBody, TaskQueuePOSTResponse,
    ServiceQueueGETBody, ServiceQueueGETResponse, ServiceQueuePOSTBody, ServiceQueuePOSTResponse,
    QueueManagerGETBody, QueueManagerGETResponse, QueueManagerPOSTBody, QueueManagerPOSTResponse,
    QueueManagerPUTBody, QueueManagerPUTResponse
)


class MultiMoleculeGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: Dict[int, Molecule]

    class Config:
        json_encoders = json_encoders


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

        body = TaskQueuePOSTBody.parse_raw(self.request.body)
        # Format tasks
        procedure_parser = procedures.get_procedure_parser(body.meta["procedure"], storage)
        full_tasks, complete_tasks, errors = procedure_parser.parse_input(body.dict())

        # Add tasks to queue
        ret = storage.queue_submit(full_tasks)

        # Do some quick reformatting
        data_payload = {"submitted": [x for x in ret["data"] if x is not None],
                        "completed": list(complete_tasks),
                        "queue": ret["meta"]["duplicates"]
                        }
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        response = TaskQueuePOSTResponse(data=data_payload, meta=ret["meta"])
        self.logger.info("TaskQueue: Added {} tasks.".format(response.meta.n_inserted))

        self.write(response.json())

    def get(self):
        """Posts new services to the service queue
        """
        self.authenticate("read")

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

    def post(self):
        """Posts new services to the service queue
        """
        self.authenticate("compute")

        # Grab objects
        storage = self.objects["storage_socket"]

        # Figure out initial molecules
        errors = []
        body = ServiceQueuePOSTBody.parse_raw(self.request.body)
        mol_query = storage.get_add_molecules_mixed(body.data)
        mol_response = MultiMoleculeGETResponse(**mol_query)

        # Build out services
        submitted_services = []
        for idx, mol in mol_response.data.items():
            mol_dict = mol.json(as_dict=True)
            # Ensure ID is present
            if hasattr(mol, 'id') and mol.id is not None:
                # Workaround until we can better refine our Molecule object 'id' is preserved better when casting to str
                mol_dict['id'] = mol.id
            tmp = services.initializer(body.meta["service"], storage, body.meta, mol_dict)
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

        data_payload = {"submitted": ret["data"],
                        "completed": list(complete_tasks),
                        "queue": ret["meta"]["duplicates"]}
        ret["meta"]["duplicates"] = []
        ret["meta"]["errors"].extend(errors)

        response = ServiceQueuePOSTResponse(data=data_payload, meta=ret['meta'])
        self.logger.info("ServiceQueue: Added {} services.\n".format(response.meta.n_inserted))

        self.write(response.json())

    def get(self):
        """Gets services from the service queue
        """
        self.authenticate("read")

        # Grab objects
        storage = self.objects["storage_socket"]

        body = ServiceQueueGETBody.parse_raw(self.request.body)

        projection = {x: True for x in ["status", "error_message", "tag"]}
        ret = storage.get_services(body.data, projection=projection)
        response = ServiceQueueGETResponse(**ret)

        self.write(response.json())


class QueueManagerHandler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    Manages the external
    """

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

        body = QueueManagerGETBody.parse_raw(self.request.body)

        # Figure out metadata and kwargs
        name = self._get_name_from_metadata(body.meta)
        queue_tags = {
            "limit": body.data.limit,
            "tag": body.meta.tag,
        }  # yapf: disable

        # Grab new tasks and write out
        new_tasks = storage.queue_get_next(name, **queue_tags)
        response = QueueManagerGETResponse(meta={"n_found": len(new_tasks),
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
        self.authenticate("queue")

        # Grab objects
        storage = self.objects["storage_socket"]

        body = QueueManagerPOSTBody.parse_raw(self.request.body)
        ret = self.insert_complete_tasks(storage, body.data, self.logger)

        response = QueueManagerPOSTResponse(meta={"n_inserted": ret[0],
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
        self.authenticate("queue")

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

        response = QueueManagerPUTResponse(meta={},
                                           data=ret)
        self.write(response.json())

        # Update manager logs
