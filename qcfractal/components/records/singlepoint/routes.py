from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper
from qcfractal.app.routes import check_access, wrap_route
from qcportal.base_models import CommonGetProjURLParameters
from qcportal.records.singlepoint import SinglepointAddBody, SinglepointQueryBody


@main.route("/v1/record/singlepoint", methods=["POST"])
@wrap_route(SinglepointAddBody, None)
@check_access
def add_singlepoint_records_v1(body_data: SinglepointAddBody):
    return storage_socket.records.singlepoint.add(
        sp_spec=body_data.specification, molecules=body_data.molecules, tag=body_data.tag, priority=body_data.priority
    )


@main.route("/v1/record/singlepoint", methods=["GET"])
@main.route("/v1/record/singlepoint/<record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_singlepoint_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        record_id,
        url_params.id,
        url_params.include,
        None,
        url_params.missing_ok,
        storage_socket.records.singlepoint.get,
    )


@main.route("/v1/record/singlepoint/<int:record_id>/wavefunction", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_singlepoint_wavefunction_v1(record_id: int):
    rec = storage_socket.records.singlepoint.get([record_id], include=["wavefunction"])
    return rec[0]["wavefunction"]


@main.route("/v1/record/singlepoint/query", methods=["POST"])
@wrap_route(SinglepointQueryBody, None)
@check_access
def query_singlepoint_v1(body_data: SinglepointQueryBody):
    return storage_socket.records.singlepoint.query(body_data)
