from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import parse_bodymodel, convert_get_response_metadata, SerializedResponse
from qcfractal.app.routes import check_access
from qcfractal.interface.models.rest_models import ResultGETBody, ResultGETResponse
from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.common_rest import CommonGetProjURLParameters, CommonDeleteURLParameters
from qcfractal.portal.components.records import RecordModifyBody, RecordQueryBody, RecordStatusEnum


@main.route("/v1/record/singlepoint", methods=["GET"])
@main.route("/v1/record/singlepoint/<int:id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_singlepoint_records_v1(id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        id, url_params.id, url_params.include, None, url_params.missing_ok, storage_socket.records.singlepoint.get
    )
