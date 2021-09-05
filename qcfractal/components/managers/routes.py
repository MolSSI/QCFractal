from flask import jsonify

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import parse_bodymodel, convert_get_response_metadata, SerializedResponse
from qcfractal.app.routes import check_access
from qcfractal.interface.models import rest_model, ManagerStatusEnum
from qcfractal.interface.models.rest_models import (
    ManagerInfoGETBody,
    ManagerInfoGETResponse,
    QueueManagerGETBody,
    QueueManagerGETResponse,
    QueueManagerPOSTBody,
    QueueManagerPOSTResponse,
)


@main.route("/manager", methods=["GET"])
@check_access
def query_manager_v1():
    """Gets manager information about managers"""

    body = parse_bodymodel(ManagerInfoGETBody)
    meta, managers = storage_socket.manager.query(**{**body.data.dict(), **body.meta.dict()})
    meta_old = convert_get_response_metadata(meta, missing=[])
    response = ManagerInfoGETResponse(meta=meta_old, data=managers)
    return SerializedResponse(response)


def _get_name_from_metadata(meta):
    """
    Form the canonical name string.
    """
    ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
    return ret


@main.route("/queue_manager", methods=["GET"])
@check_access
def queue_manager_claim_v1():
    """Pulls new tasks from the task queue"""

    body = parse_bodymodel(QueueManagerGETBody)

    # Figure out metadata and kwargs
    name = _get_name_from_metadata(body.meta)

    # Grab new tasks and write out
    new_tasks = storage_socket.procedure.claim_tasks(name, body.meta.programs, limit=body.data.limit, tag=body.meta.tag)
    response = QueueManagerGETResponse(
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
    # Update manager logs
    storage_socket.manager.update(name, submitted=len(new_tasks), **body.meta.dict())

    return SerializedResponse(response)


@main.route("/queue_manager", methods=["POST"])
@check_access
def queue_manager_return_v1():
    """Posts complete tasks to the task queue"""

    body = parse_bodymodel(QueueManagerPOSTBody)
    manager_name = _get_name_from_metadata(body.meta)
    storage_socket.procedure.update_completed(manager_name, body.data)

    response = QueueManagerPOSTResponse(
        **{
            "meta": {
                "n_inserted": len(body.data),
                "duplicates": [],
                "validation_errors": [],
                "success": True,
                "errors": [],
                "error_description": "",
            },
            "data": True,
        }
    )

    return SerializedResponse(response)


@main.route("/queue_manager", methods=["PUT"])
@check_access
def queue_manager_modify_v1():
    """
    Various manager manipulation operations
    """

    ret = True

    body_model, response_model = rest_model("queue_manager", "put")
    body = parse_bodymodel(body_model)

    name = _get_name_from_metadata(body.meta)
    op = body.data.operation
    if op == "startup":
        storage_socket.manager.update(
            name, status=ManagerStatusEnum.active, configuration=body.data.configuration, **body.meta.dict(), log=True
        )
        # current_app.logger.info("QueueManager: New active manager {} detected.".format(name))

    elif op == "shutdown":
        nshutdown = storage_socket.procedure.reset_tasks(manager=[name], reset_running=True)
        storage_socket.manager.update(
            name, returned=nshutdown, status=ManagerStatusEnum.inactive, **body.meta.dict(), log=True
        )

        # current_app.logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(name, nshutdown))

        ret = {"nshutdown": nshutdown}

    elif op == "heartbeat":
        storage_socket.manager.update(name, status=ManagerStatusEnum.active, **body.meta.dict(), log=True)
        # current_app.logger.debug("QueueManager: Heartbeat of manager {} detected.".format(name))

    else:
        msg = "Operation '{}' not understood.".format(op)
        return jsonify(msg=msg), 400

    response = response_model(**{"meta": {}, "data": ret})

    return SerializedResponse(response)
