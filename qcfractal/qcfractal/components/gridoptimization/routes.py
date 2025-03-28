from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.exceptions import LimitExceededError
from qcportal.gridoptimization import (
    GridoptimizationDatasetSpecification,
    GridoptimizationDatasetNewEntry,
    GridoptimizationAddBody,
    GridoptimizationQueryFilters,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/gridoptimization/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_gridoptimization_records_v1(body_data: GridoptimizationAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(
            f"Cannot add {len(body_data.initial_molecules)} gridoptimization records - limit is {limit}"
        )

    return storage_socket.records.gridoptimization.add(
        initial_molecules=body_data.initial_molecules,
        go_spec=body_data.specification,
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/gridoptimization/<int:record_id>/optimizations", methods=["GET"])
@wrap_route("READ")
def get_gridoptimization_optimizations_v1(record_id: int):
    return storage_socket.records.gridoptimization.get_optimizations(record_id)


@api_v1.route("/records/gridoptimization/query", methods=["POST"])
@wrap_route("READ")
def query_gridoptimization_v1(body_data: GridoptimizationQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.gridoptimization.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/gridoptimization/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_gridoptimization_dataset_specifications_v1(
    dataset_id: int, body_data: List[GridoptimizationDatasetSpecification]
):
    return storage_socket.datasets.gridoptimization.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/gridoptimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_gridoptimization_dataset_entries_v1(dataset_id: int, body_data: List[GridoptimizationDatasetNewEntry]):
    return storage_socket.datasets.gridoptimization.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/gridoptimization/<int:dataset_id>/background_add_entries", methods=["POST"])
@wrap_route("WRITE")
def background_add_gridoptimization_dataset_entries_v1(
    dataset_id: int, body_data: List[GridoptimizationDatasetNewEntry]
):
    return storage_socket.datasets.gridoptimization.background_add_entries(dataset_id, new_entries=body_data)
