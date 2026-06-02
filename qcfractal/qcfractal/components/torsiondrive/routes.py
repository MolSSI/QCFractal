from typing import Any

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcportal.exceptions import LimitExceededError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.torsiondrive import (
    TorsiondriveDatasetSpecification,
    TorsiondriveDatasetNewEntry,
    TorsiondriveAddBody,
    TorsiondriveQueryFilters,
    TorsiondriveDatasetEntriesFrom,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/torsiondrive/bulkCreate", methods=["POST"])
@check_permissions("records", "add")
@serialization()
def add_torsiondrive_records_v1(body_data: TorsiondriveAddBody) -> tuple[InsertMetadata, list[int | None]]:
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
        creator_user=g.username,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/torsiondrive/<int:record_id>/optimizations", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_torsiondrive_optimizations_v1(record_id: int) -> list[dict[str, Any]]:
    return storage_socket.records.torsiondrive.get_optimizations(record_id)


@api_v1.route("/records/torsiondrive/<int:record_id>/minimum_optimizations", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_torsiondrive_minimum_optimizations_v1(record_id: int) -> dict[str, int]:
    return storage_socket.records.torsiondrive.get_minimum_optimizations(record_id)


@api_v1.route("/records/torsiondrive/<int:record_id>/initial_molecules", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_torsiondrive_initial_molecules_ids_v1(record_id: int) -> list[int]:
    return storage_socket.records.torsiondrive.get_initial_molecules_ids(record_id)


@api_v1.route("/records/torsiondrive/query", methods=["POST"])
@check_permissions("datasets", "read")
@serialization()
def query_torsiondrive_v1(body_data: TorsiondriveQueryFilters) -> list[int]:
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.torsiondrive.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/specifications", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_torsiondrive_dataset_specifications_v1(
    dataset_id: int, body_data: list[TorsiondriveDatasetSpecification]
) -> InsertMetadata:
    return storage_socket.datasets.torsiondrive.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_torsiondrive_dataset_entries_v1(
    dataset_id: int, body_data: list[TorsiondriveDatasetNewEntry]
) -> InsertMetadata:
    return storage_socket.datasets.torsiondrive.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/background_add_entries", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def background_add_torsiondrive_dataset_entries_v1(
    dataset_id: int, body_data: list[TorsiondriveDatasetNewEntry]
) -> int:
    return storage_socket.datasets.torsiondrive.background_add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/entries/addFrom", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_torsiondrive_dataset_entries_from_v1(dataset_id: int, body_data: TorsiondriveDatasetEntriesFrom):
    return storage_socket.datasets.torsiondrive.add_entries_from_ds(
        dataset_id=dataset_id,
        from_dataset_id=body_data.dataset_id,
        from_dataset_type=body_data.dataset_type,
        from_dataset_name=body_data.dataset_name,
        from_specification_name=body_data.specification_name,
    )
