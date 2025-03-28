from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.exceptions import LimitExceededError
from qcportal.torsiondrive import (
    TorsiondriveDatasetSpecification,
    TorsiondriveDatasetNewEntry,
    TorsiondriveAddBody,
    TorsiondriveQueryFilters,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/torsiondrive/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_records_v1(body_data: TorsiondriveAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(
            f"Cannot add {len(body_data.initial_molecules)} torsiondrive records - limit is {limit}"
        )

    return storage_socket.records.torsiondrive.add(
        initial_molecules=body_data.initial_molecules,
        td_spec=body_data.specification,
        as_service=body_data.as_service,
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/torsiondrive/<int:record_id>/optimizations", methods=["GET"])
@wrap_route("READ")
def get_torsiondrive_optimizations_v1(record_id: int):
    return storage_socket.records.torsiondrive.get_optimizations(record_id)


@api_v1.route("/records/torsiondrive/<int:record_id>/minimum_optimizations", methods=["GET"])
@wrap_route("READ")
def get_torsiondrive_minimum_optimizations_v1(record_id: int):
    return storage_socket.records.torsiondrive.get_minimum_optimizations(record_id)


@api_v1.route("/records/torsiondrive/<int:record_id>/initial_molecules", methods=["GET"])
@wrap_route("READ")
def get_torsiondrive_initial_molecules_ids_v1(record_id: int):
    return storage_socket.records.torsiondrive.get_initial_molecules_ids(record_id)


@api_v1.route("/records/torsiondrive/query", methods=["POST"])
@wrap_route("READ")
def query_torsiondrive_v1(body_data: TorsiondriveQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.torsiondrive.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_specifications_v1(dataset_id: int, body_data: List[TorsiondriveDatasetSpecification]):
    return storage_socket.datasets.torsiondrive.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_entries_v1(dataset_id: int, body_data: List[TorsiondriveDatasetNewEntry]):
    return storage_socket.datasets.torsiondrive.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/background_add_entries", methods=["POST"])
@wrap_route("WRITE")
def background_add_torsiondrive_dataset_entries_v1(dataset_id: int, body_data: List[TorsiondriveDatasetNewEntry]):
    return storage_socket.datasets.torsiondrive.background_add_entries(dataset_id, new_entries=body_data)
