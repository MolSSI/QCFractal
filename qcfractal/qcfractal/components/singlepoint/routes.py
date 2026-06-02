from typing import Any

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcportal.exceptions import LimitExceededError
from qcportal.compression import CompressionEnum
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
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
@check_permissions("records", "add")
@serialization()
def add_singlepoint_records_v1(body_data: SinglepointAddBody) -> tuple[InsertMetadata, list[int | None]]:
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.molecules) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.molecules)} singlepoint records - limit is {limit}")

    return storage_socket.records.singlepoint.add(
        molecules=body_data.molecules,
        qc_spec=body_data.specification,
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        creator_user=g.username,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_singlepoint_wavefunction_v1(record_id: int) -> dict[str, Any] | None:
    return storage_socket.records.singlepoint.get_wavefunction_metadata(record_id)


@api_v1.route("/records/singlepoint/<int:record_id>/wavefunction/data", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_singlepoint_wavefunction_data_v1(record_id: int) -> tuple[bytes, CompressionEnum]:
    return storage_socket.records.singlepoint.get_wavefunction_rawdata(record_id)


@api_v1.route("/records/singlepoint/query", methods=["POST"])
@check_permissions("records", "read")
@serialization()
def query_singlepoint_v1(body_data: SinglepointQueryFilters) -> list[int]:
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.singlepoint.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/specifications", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_singlepoint_dataset_specifications_v1(
    dataset_id: int, body_data: list[SinglepointDatasetSpecification]
) -> InsertMetadata:
    return storage_socket.datasets.singlepoint.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_singlepoint_dataset_entries_v1(dataset_id: int, body_data: list[SinglepointDatasetNewEntry]) -> InsertMetadata:
    return storage_socket.datasets.singlepoint.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/background_add_entries", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def background_add_singlepoint_dataset_entries_v1(
    dataset_id: int, body_data: list[SinglepointDatasetNewEntry]
) -> int:
    return storage_socket.datasets.singlepoint.background_add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/singlepoint/<int:dataset_id>/entries/addFrom", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_singlepoint_dataset_entries_from_v1(
    dataset_id: int, body_data: SinglepointDatasetEntriesFrom
) -> InsertCountsMetadata:
    return storage_socket.datasets.singlepoint.add_entries_from_ds(
        dataset_id=dataset_id,
        from_dataset_id=body_data.dataset_id,
        from_dataset_type=body_data.dataset_type,
        from_dataset_name=body_data.dataset_name,
        from_specification_name=body_data.specification_name,
    )
