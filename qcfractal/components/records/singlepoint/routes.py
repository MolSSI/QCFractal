from typing import Optional

from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper
from qcfractal.app.routes import wrap_route
from qcportal.base_models import CommonGetProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.records.singlepoint import SinglepointAddBody, SinglepointQueryBody
from qcportal.utils import calculate_limit


@main.route("/v1/record/singlepoint", methods=["POST"])
@wrap_route(SinglepointAddBody, None)
def add_singlepoint_records_v1(body_data: SinglepointAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.molecules) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.molecules)} singlepoint records - limit is {limit}")

    return storage_socket.records.singlepoint.add(
        molecules=body_data.molecules, qc_spec=body_data.specification, tag=body_data.tag, priority=body_data.priority
    )


@main.route("/v1/record/singlepoint", methods=["GET"])
@main.route("/v1/record/singlepoint/<record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
def get_singlepoint_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    if url_params.id is not None and len(url_params.id) > limit:
        raise LimitExceededError(f"Cannot get {len(url_params.id)} singlepoint records - limit is {limit}")

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
def get_singlepoint_wavefunction_v1(record_id: int):
    rec = storage_socket.records.singlepoint.get([record_id], include=["wavefunction"])

    assert rec[0] is not None
    return rec[0]["wavefunction"]


@main.route("/v1/record/singlepoint/query", methods=["POST"])
@wrap_route(SinglepointQueryBody, None)
def query_singlepoint_v1(body_data: SinglepointQueryBody):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.singlepoint.query(body_data)
