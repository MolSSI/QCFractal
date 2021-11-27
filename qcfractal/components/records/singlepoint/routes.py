from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.common_rest import CommonGetProjURLParameters


@main.route("/v1/record/singlepoint", methods=["GET"])
@main.route("/v1/record/singlepoint/<int:id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_singlepoint_records_v1(id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        id, url_params.id, url_params.include, None, url_params.missing_ok, storage_socket.records.singlepoint.get
    )
