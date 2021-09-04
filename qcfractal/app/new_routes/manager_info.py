from qcfractal.app import storage_socket
from qcfractal.app.new_routes.helpers import parse_bodymodel, convert_get_response_metadata, SerializedResponse
from qcfractal.app.new_routes.main import main
from qcfractal.app.new_routes.permissions import check_access
from qcfractal.interface.models.rest_models import ManagerInfoGETBody, ManagerInfoGETResponse


@main.route("/manager", methods=["GET"])
@check_access
def query_manager_v1():
    """Gets manager information about managers"""

    body = parse_bodymodel(ManagerInfoGETBody)
    meta, managers = storage_socket.manager.query(**{**body.data.dict(), **body.meta.dict()})
    meta_old = convert_get_response_metadata(meta, missing=[])
    response = ManagerInfoGETResponse(meta=meta_old, data=managers)
    return SerializedResponse(response)
