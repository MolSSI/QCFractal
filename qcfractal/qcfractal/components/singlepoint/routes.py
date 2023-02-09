from typing import List

from flask import current_app, g

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import storage_socket
from qcportal.exceptions import LimitExceededError
from qcportal.singlepoint import (
    SinglepointDatasetSpecification,
    SinglepointDatasetNewEntry,
    SinglepointAddBody,
    SinglepointQueryFilters,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/singlepoint/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_records_v1(body_data: SinglepointAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.molecules) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.molecules)} singlepoint records - limit is {limit}")

    return storage_socket.records.singlepoint.add(
        molecules=body_data.molecules,
        qc_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
    )


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction", methods=["GET"])
@wrap_route("READ")
def get_singlepoint_wavefunction_v1(record_id: int):
    rec = storage_socket.records.singlepoint.get([record_id], include=["wavefunction"])
    return rec[0].get("wavefunction", None)


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction/data", methods=["GET"])
@wrap_route("READ")
def get_singlepoint_wavefunction_data_v1(record_id: int):
    with storage_socket.session_scope(True) as session:
        lb_id = storage_socket.records.singlepoint.get_wavefunction_lb_id(record_id, session=session)
        return storage_socket.largebinary.get_raw(lb_id, session=session)


@api_v1.route("/records/singlepoint/query", methods=["POST"])
@wrap_route("READ")
def query_singlepoint_v1(body_data: SinglepointQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.singlepoint.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_specifications_v1(dataset_id: int, body_data: List[SinglepointDatasetSpecification]):
    return storage_socket.datasets.singlepoint.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_entries_v1(dataset_id: int, body_data: List[SinglepointDatasetNewEntry]):
    return storage_socket.datasets.singlepoint.add_entries(
        dataset_id,
        new_entries=body_data,
    )
