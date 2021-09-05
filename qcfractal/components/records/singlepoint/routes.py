from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import parse_bodymodel, convert_get_response_metadata, SerializedResponse
from qcfractal.app.routes import check_access
from qcfractal.interface.models.rest_models import ResultGETBody, ResultGETResponse


@main.route("/result", methods=["GET"])
@check_access
def query_result_v1():

    body = parse_bodymodel(ResultGETBody)
    meta, results = storage_socket.procedure.single.query(**{**body.data.dict(), **body.meta.dict()})

    # Remove result_type. This isn't used right now and is missing from the model
    for r in results:
        r.pop("result_type", None)

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])
    response = ResultGETResponse(meta=meta_old, data=results)
    return SerializedResponse(response)
