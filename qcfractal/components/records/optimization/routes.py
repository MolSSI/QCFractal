from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import parse_bodymodel, convert_get_response_metadata, SerializedResponse
from qcfractal.app.routes import check_access
from qcfractal.interface.models.rest_models import OptimizationGETBody, OptimizationGETResponse


@main.route("/optimization", methods=["GET"])
@check_access
def query_optimization_v1():
    body = parse_bodymodel(OptimizationGETBody)

    meta, ret = storage_socket.procedure.optimization.query(**{**body.data.dict(), **body.meta.dict()})

    # Remove result_type. This isn't used right now and is missing from the model
    for r in ret:
        r.pop("result_type", None)

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])

    response = OptimizationGETResponse(meta=meta_old, data=ret)

    return SerializedResponse(response)
