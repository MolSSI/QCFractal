from flask import jsonify

from qcfractal.app import storage_socket
from qcfractal.app.new_routes.helpers import (
    parse_bodymodel,
    convert_get_response_metadata,
    SerializedResponse,
    convert_post_response_metadata,
)
from qcfractal.app.new_routes.main import main
from qcfractal.app.new_routes.permissions import check_access
from qcfractal.interface.models import PriorityEnum
from qcfractal.interface.models.rest_models import (
    TaskQueueGETBody,
    TaskQueueGETResponse,
    TaskQueuePOSTBody,
    TaskQueuePOSTResponse,
    TaskQueuePUTBody,
    TaskQueuePUTResponse,
)


@main.route("/task_queue", methods=["GET"])
@check_access
def query_task_v1():
    body = parse_bodymodel(TaskQueueGETBody)

    # Change base_result -> base_result_id
    data = body.data.dict()
    if "base_result" in data:
        data["base_result_id"] = data.pop("base_result")

    meta, tasks = storage_socket.procedure.query_tasks(**{**data, **body.meta.dict()})

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])

    for t in tasks:
        if "base_result_id" in t:
            t["base_result"] = t.pop("base_result_id")

    response = TaskQueueGETResponse(meta=meta_old, data=tasks)

    return SerializedResponse(response)


@main.route("/task_queue", methods=["POST"])
@check_access
def add_task_v1():
    body = parse_bodymodel(TaskQueuePOSTBody)
    meta, ids = storage_socket.procedure.create(body.data, body.meta)

    # Convert to the old response type
    duplicate_ids = [ids[i] for i in meta.existing_idx]
    submitted_ids = [ids[i] for i in meta.inserted_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)

    resp = TaskQueuePOSTResponse(
        meta=meta_old, data={"ids": ids, "submitted": submitted_ids, "existing": duplicate_ids}
    )
    return SerializedResponse(resp)


@main.route("/task_queue", methods=["PUT"])
@check_access
def modify_task_v1():
    """Modifies tasks in the task queue"""

    body = parse_bodymodel(TaskQueuePUTBody)

    if (body.data.id is None) and (body.data.base_result is None):
        return jsonify(msg="Id or ResultId must be specified."), 400

    if body.meta.operation == "restart":
        d = body.data.dict()
        d.pop("new_tag", None)
        d.pop("new_priority", None)
        tasks_updated = storage_socket.procedure.reset_tasks(**d, reset_error=True)
        data = {"n_updated": tasks_updated}
    elif body.meta.operation == "regenerate":

        new_tag = body.data.new_tag
        if body.data.new_priority is None:
            new_priority = PriorityEnum.normal
        else:
            new_priority = body.data.new_priority

        task_ids = storage_socket.procedure.regenerate_tasks(body.data.base_result, new_tag, new_priority)
        data = {"n_updated": len(task_ids) - task_ids.count(None)}

    elif body.meta.operation == "modify":
        tasks_updated = storage_socket.procedure.modify_tasks(
            id=body.data.id,
            base_result=body.data.base_result,
            new_tag=body.data.new_tag,
            new_priority=body.data.new_priority,
        )
        data = {"n_updated": tasks_updated}
    else:
        return jsonify(msg=f"Operation '{body.meta.operation}' is not valid."), 400

    response = TaskQueuePUTResponse(data=data, meta={"errors": [], "success": True, "error_description": False})

    return SerializedResponse(response)
