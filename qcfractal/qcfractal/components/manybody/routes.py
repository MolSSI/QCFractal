from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.exceptions import LimitExceededError
from qcportal.manybody import (
    ManybodyDatasetSpecification,
    ManybodyDatasetNewEntry,
    ManybodyAddBody,
    ManybodyQueryFilters,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/manybody/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_records_v1(body_data: ManybodyAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.initial_molecules)} manybody records - limit is {limit}")

    return storage_socket.records.manybody.add(
        initial_molecules=body_data.initial_molecules,
        mb_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/manybody/<int:record_id>/clusters", methods=["GET"])
@wrap_route("READ")
def get_manybody_clusters_v1(record_id: int):
    return storage_socket.records.manybody.get_clusters(record_id)


@api_v1.route("/records/manybody/query", methods=["POST"])
@wrap_route("READ")
def query_manybody_v1(body_data: ManybodyQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.manybody.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/manybody/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_specifications_v1(dataset_id: int, body_data: List[ManybodyDatasetSpecification]):
    return storage_socket.datasets.manybody.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/manybody/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_entries_v1(dataset_id: int, body_data: List[ManybodyDatasetNewEntry]):
    return storage_socket.datasets.manybody.add_entries(
        dataset_id,
        new_entries=body_data,
    )
