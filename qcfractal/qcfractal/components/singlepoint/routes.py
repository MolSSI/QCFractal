from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.exceptions import LimitExceededError
from qcportal.singlepoint import (
    SinglepointDatasetSpecification,
    SinglepointDatasetNewEntry,
    SinglepointAddBody,
    SinglepointQueryFilters,
    SinglepointDatasetEntriesFrom,
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
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction", methods=["GET"])
@wrap_route("READ")
def get_singlepoint_wavefunction_v1(record_id: int):
    return storage_socket.records.singlepoint.get_wavefunction_metadata(record_id)


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction/data", methods=["GET"])
@wrap_route("READ")
def get_singlepoint_wavefunction_data_v1(record_id: int):
    return storage_socket.records.singlepoint.get_wavefunction_rawdata(record_id)


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
    return storage_socket.datasets.singlepoint.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/background_add_entries", methods=["POST"])
@wrap_route("WRITE")
def background_add_singlepoint_dataset_entries_v1(dataset_id: int, body_data: List[SinglepointDatasetNewEntry]):
    return storage_socket.datasets.singlepoint.background_add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/entries/addFrom", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_entries_from_v1(dataset_id: int, body_data: SinglepointDatasetEntriesFrom):
    return storage_socket.datasets.singlepoint.add_entries_from_ds(
        dataset_id=dataset_id,
        from_dataset_id=body_data.dataset_id,
        from_dataset_type=body_data.dataset_type,
        from_dataset_name=body_data.dataset_name,
        from_specification_name=body_data.specification_name,
    )
