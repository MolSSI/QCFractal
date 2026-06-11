from typing import Any

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcportal.exceptions import LimitExceededError
from qcportal.metadata_models import InsertMetadata
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
@check_permissions("records", "add")
@serialization()
def add_gridoptimization_records_v1(body_data: GridoptimizationAddBody) -> tuple[InsertMetadata, list[int | None]]:
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
        creator_user=g.username,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/gridoptimization/<int:record_id>/optimizations", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_gridoptimization_optimizations_v1(record_id: int) -> list[dict[str, Any]]:
    return storage_socket.records.gridoptimization.get_optimizations(record_id)


@api_v1.route("/records/gridoptimization/query", methods=["POST"])
@check_permissions("records", "read")
@serialization()
def query_gridoptimization_v1(body_data: GridoptimizationQueryFilters) -> list[int]:
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.gridoptimization.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/gridoptimization/<int:dataset_id>/specifications", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_gridoptimization_dataset_specifications_v1(
    dataset_id: int, body_data: list[GridoptimizationDatasetSpecification]
) -> InsertMetadata:
    return storage_socket.datasets.gridoptimization.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/gridoptimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_gridoptimization_dataset_entries_v1(
    dataset_id: int, body_data: list[GridoptimizationDatasetNewEntry]
) -> InsertMetadata:
    return storage_socket.datasets.gridoptimization.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/gridoptimization/<int:dataset_id>/background_add_entries", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def background_add_gridoptimization_dataset_entries_v1(
    dataset_id: int, body_data: list[GridoptimizationDatasetNewEntry]
) -> int:
    return storage_socket.datasets.gridoptimization.background_add_entries(dataset_id, new_entries=body_data)
