from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.exceptions import LimitExceededError
from qcportal.optimization import (
    OptimizationDatasetSpecification,
    OptimizationDatasetNewEntry,
    OptimizationAddBody,
    OptimizationQueryFilters,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/optimization/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_records_v1(body_data: OptimizationAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(
            f"Cannot add {len(body_data.initial_molecules)} optimization records - limit is {limit}"
        )

    return storage_socket.records.optimization.add(
        initial_molecules=body_data.initial_molecules,
        opt_spec=body_data.specification,
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/optimization/<int:record_id>/trajectory", methods=["GET"])
@wrap_route("READ")
def get_optimization_trajectory_ids_v1(record_id: int):
    return storage_socket.records.optimization.get_trajectory_ids(record_id)


@api_v1.route("/records/optimization/query", methods=["POST"])
@wrap_route("READ")
def query_optimization_v1(body_data: OptimizationQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.optimization.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/optimization/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_dataset_specifications_v1(dataset_id: int, body_data: List[OptimizationDatasetSpecification]):
    return storage_socket.datasets.optimization.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/optimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_dataset_entries_v1(dataset_id: int, body_data: List[OptimizationDatasetNewEntry]):
    return storage_socket.datasets.optimization.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/optimization/<int:dataset_id>/background_add_entries", methods=["POST"])
@wrap_route("WRITE")
def background_add_optimization_dataset_entries_v1(dataset_id: int, body_data: List[OptimizationDatasetNewEntry]):
    return storage_socket.datasets.optimization.background_add_entries(dataset_id, new_entries=body_data)
