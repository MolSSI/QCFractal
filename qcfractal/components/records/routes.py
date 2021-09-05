from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import parse_bodymodel, convert_get_response_metadata, SerializedResponse
from qcfractal.app.routes import check_access
from qcfractal.interface.models import rest_model

# These are used by the flask app. The flask app will
# import this file, which will cause all the routes in these
# subdirectories to be registered with the blueprint
from .singlepoint import routes
from .optimization import routes
from .gridoptimization import routes
from .torsiondrive import routes


@main.route("/procedure", methods=["GET"])
@check_access
def query_procedure_v1():
    body_model, response_model = rest_model("procedure", "get")
    body = parse_bodymodel(body_model)

    meta, ret = storage_socket.record.query(**{**body.data.dict(), **body.meta.dict()})

    # Remove result_type. This isn't used right now and is missing from the model
    for r in ret:
        r.pop("result_type", None)

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])

    response = response_model(meta=meta_old, data=ret)

    return SerializedResponse(response)
