from flask import current_app

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import storage_socket
from qcportal.exceptions import LimitExceededError
from qcportal.singlepoint import SinglepointAddBody, SinglepointQueryFilters
from qcportal.utils import calculate_limit


@api_v1.route("/records/singlepoint/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_records_v1(body_data: SinglepointAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.molecules) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.molecules)} singlepoint records - limit is {limit}")

    return storage_socket.records.singlepoint.add(
        molecules=body_data.molecules, qc_spec=body_data.specification, tag=body_data.tag, priority=body_data.priority
    )


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction", methods=["GET"])
@wrap_route("READ")
def get_singlepoint_wavefunction_v1(record_id: int):
    rec = storage_socket.records.singlepoint.get([record_id], include=["wavefunction"])

    assert rec[0] is not None
    return rec[0]["wavefunction"]


@api_v1.route("/records/singlepoint/query", methods=["POST"])
@wrap_route("READ")
def query_singlepoint_v1(body_data: SinglepointQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.singlepoint.query(body_data)
