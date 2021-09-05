from flask import jsonify

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import (
    parse_bodymodel,
    convert_get_response_metadata,
    SerializedResponse,
    convert_post_response_metadata,
)
from qcfractal.app.routes import check_access
from qcfractal.interface.models.rest_models import (
    ServiceQueueGETBody,
    ServiceQueueGETResponse,
    ServiceQueuePOSTBody,
    ServiceQueuePOSTResponse,
    ServiceQueuePUTBody,
    ServiceQueuePUTResponse,
)


@main.route("/service_queue", methods=["GET"])
@check_access
def query_service_queue_v1():
    body = parse_bodymodel(ServiceQueueGETBody)

    meta, data = storage_socket.services.query_tasks(**{**body.data.dict(), **body.meta.dict()})

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])

    response = ServiceQueueGETResponse(meta=meta_old, data=data)

    return SerializedResponse(response)


@main.route("/service_queue", methods=["POST"])
@check_access
def post_service_queue():
    """Posts new services to the service queue."""

    body = parse_bodymodel(ServiceQueuePOSTBody)

    meta, ids = storage_socket.services.create(body.data)

    duplicate_ids = [ids[i] for i in meta.existing_idx]
    submitted_ids = [ids[i] for i in meta.inserted_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)

    resp = ServiceQueuePOSTResponse(
        meta=meta_old, data={"ids": ids, "submitted": submitted_ids, "existing": duplicate_ids}
    )

    return SerializedResponse(resp)


@main.route("/service_queue", methods=["PUT"])
@check_access
def put_service_queue():
    """Modifies services in the service queue"""

    body = parse_bodymodel(ServiceQueuePUTBody)

    if (body.data.id is None) and (body.data.procedure_id is None):
        return jsonify(msg="Id or ProcedureId must be specified."), 400

    if body.meta.operation == "restart":
        updates = storage_socket.services.reset_tasks(**body.data.dict())
        data = {"n_updated": updates}
    else:
        return jsonify(msg="Operation '{operation}' is not valid."), 400

    response = ServiceQueuePUTResponse(data=data, meta={"errors": [], "success": True, "error_description": False})

    return SerializedResponse(response)
