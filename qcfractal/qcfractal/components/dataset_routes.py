from typing import Dict

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.base_models import ProjURLParameters
from qcportal.dataset_models import (
    DatasetAddBody,
    DatasetQueryModel,
    DatasetFetchRecordsBody,
    DatasetFetchEntryBody,
    DatasetSubmitBody,
    DatasetDeleteStrBody,
    DatasetRecordModifyBody,
    DatasetRemoveRecordsBody,
    DatasetRecordRevertBody,
    DatasetModifyMetadata,
    DatasetQueryRecords,
    DatasetDeleteParams,
)
from qcportal.exceptions import LimitExceededError


@api_v1.route("/datasets", methods=["GET"])
@wrap_route("READ")
def list_dataset_v1():
    return storage_socket.datasets.list()


@api_v1.route("/datasets/<int:dataset_id>", methods=["GET"])
@wrap_route("READ")
def get_general_dataset_v1(dataset_id: int, url_params: ProjURLParameters):
    with storage_socket.session_scope(True) as session:
        ds_type = storage_socket.datasets.lookup_type(dataset_id, session=session)
        ds_socket = storage_socket.datasets.get_socket(ds_type)
        return ds_socket.get(dataset_id, url_params.include, url_params.exclude, session=session)


@api_v1.route("/datasets/query", methods=["POST"])
@wrap_route("READ")
def query_general_dataset_v1(body_data: DatasetQueryModel):
    with storage_socket.session_scope(True) as session:
        dataset_id = storage_socket.datasets.lookup_id(body_data.dataset_type, body_data.dataset_name, session=session)
        ds_type = storage_socket.datasets.lookup_type(dataset_id, session=session)
        ds_socket = storage_socket.datasets.get_socket(ds_type)
        return ds_socket.get(dataset_id, body_data.include, body_data.exclude, session=session)


@api_v1.route("/datasets/queryrecords", methods=["POST"])
@wrap_route("READ")
def query_dataset_records_v1(body_data: DatasetQueryRecords):
    return storage_socket.datasets.query_dataset_records(
        record_id=body_data.record_id, dataset_type=body_data.dataset_type
    )


@api_v1.route("/datasets/<int:dataset_id>", methods=["DELETE"])
@wrap_route("DELETE")
def delete_dataset_v1(dataset_id: int, url_params: DatasetDeleteParams):
    with storage_socket.session_scope(True) as session:
        ds_type = storage_socket.datasets.lookup_type(dataset_id, session=session)
        ds_socket = storage_socket.datasets.get_socket(ds_type)
        return ds_socket.delete_dataset(dataset_id, url_params.delete_records)


#################################################################
# COMMON FUNCTIONS
# These functions are common to all datasets
# Note that the inputs are all the same, but the returned dicts
# are different
#################################################################


########################
# Adding a dataset
########################
@api_v1.route("/datasets/<string:dataset_type>", methods=["POST"])
@wrap_route("WRITE")
def add_dataset_v1(dataset_type: str, body_data: DatasetAddBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.add(
        name=body_data.name,
        description=body_data.description,
        tagline=body_data.tagline,
        tags=body_data.tags,
        group=body_data.group,
        provenance=body_data.provenance,
        visibility=body_data.visibility,
        default_tag=body_data.default_tag,
        default_priority=body_data.default_priority,
        metadata=body_data.metadata,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        existing_ok=body_data.existing_ok,
    )


#########################
# Getting info
#########################
@api_v1.route("/datasets/<int:dataset_id>", methods=["GET"])
@wrap_route("READ")
def get_dataset_general_v1(dataset_id: int, url_params: ProjURLParameters):
    return storage_socket.datasets.get(
        dataset_id,
        url_params.include,
        url_params.exclude,
    )


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>", methods=["GET"])
@wrap_route("READ")
def get_dataset_v1(dataset_type: str, dataset_id: int, url_params: ProjURLParameters):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get(
        dataset_id,
        url_params.include,
        url_params.exclude,
    )


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/status", methods=["GET"])
@wrap_route("READ")
def get_dataset_status_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.status(dataset_id)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/detailed_status", methods=["GET"])
@wrap_route("READ")
def get_dataset_detailed_status_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.detailed_status(dataset_id)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/record_count", methods=["GET"])
@wrap_route("READ")
def get_dataset_record_count_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_record_count(dataset_id)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/computed_properties", methods=["GET"])
@wrap_route("READ")
def get_dataset_computed_properties_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_computed_properties(dataset_id)


#########################
# Modifying metadata
#########################
@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>", methods=["PATCH"])
@wrap_route("WRITE")
def modify_dataset_metadata_v1(dataset_type: str, dataset_id: int, body_data: DatasetModifyMetadata):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.update_metadata(dataset_id, new_metadata=body_data)


#########################
# Computation submission
#########################
@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/submit", methods=["POST"])
@wrap_route("WRITE")
def submit_dataset_v1(dataset_type: str, dataset_id: int, body_data: DatasetSubmitBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.submit(
        dataset_id,
        entry_names=body_data.entry_names,
        specification_names=body_data.specification_names,
        tag=body_data.tag,
        priority=body_data.priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


###################
# Specifications
###################
@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/specifications", methods=["GET"])
@wrap_route("READ")
def fetch_dataset_specifications_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.fetch_specifications(dataset_id)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/specifications/bulkDelete", methods=["POST"])
@wrap_route("DELETE")
def delete_dataset_specifications_v1(dataset_type: str, dataset_id: int, body_data: DatasetDeleteStrBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.delete_specifications(dataset_id, body_data.names, body_data.delete_records)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/specifications", methods=["PATCH"])
@wrap_route("WRITE")
def rename_dataset_specifications_v1(dataset_type: str, dataset_id: int, body_data: Dict[str, str]):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.rename_specifications(dataset_id, body_data)


###################
# Entries
###################
@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/entry_names", methods=["GET"])
@wrap_route("READ")
def fetch_dataset_entry_names_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.fetch_entry_names(dataset_id)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/entries/bulkDelete", methods=["POST"])
@wrap_route("DELETE")
def delete_dataset_entries_v1(dataset_type: str, dataset_id: int, body_data: DatasetDeleteStrBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.delete_entries(dataset_id, body_data.names, body_data.delete_records)


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/entries/bulkFetch", methods=["POST"])
@wrap_route("READ")
def fetch_dataset_entries_v1(dataset_type: str, dataset_id: int, body_data: DatasetFetchEntryBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_dataset_entries

    if len(body_data.names) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.names)} dataset entries - limit is {limit}")

    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.fetch_entries(
        dataset_id,
        entry_names=body_data.names,
        missing_ok=body_data.missing_ok,
    )


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/entries", methods=["PATCH"])
@wrap_route("WRITE")
def rename_dataset_entries_v1(dataset_type: str, dataset_id: int, body_data: Dict[str, str]):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.rename_entries(dataset_id, body_data)


#########################
# Records
#########################
@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/records/bulkFetch", methods=["POST"])
@wrap_route("READ")
def fetch_dataset_records_v1(dataset_type: str, dataset_id: int, body_data: DatasetFetchRecordsBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records

    n_requested = len(body_data.entry_names) * len(body_data.specification_names)
    if n_requested > limit:
        raise LimitExceededError(f"Cannot get {n_requested} dataset records - limit is {limit}")

    ds_socket = storage_socket.datasets.get_socket(dataset_type)

    return ds_socket.fetch_records(
        dataset_id,
        entry_names=body_data.entry_names,
        specification_names=body_data.specification_names,
        status=body_data.status,
    )


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/records/bulkDelete", methods=["POST"])
@wrap_route("DELETE")
def remove_dataset_records_v1(dataset_type: str, dataset_id: int, body_data: DatasetRemoveRecordsBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.remove_records(
        dataset_id,
        entry_names=body_data.entry_names,
        specification_names=body_data.specification_names,
        delete_records=body_data.delete_records,
    )


#########################
# Record modification
#########################
@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/records", methods=["PATCH"])
@wrap_route("WRITE")
def modify_dataset_records_v1(dataset_type: str, dataset_id: int, body_data: DatasetRecordModifyBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.modify_records(
        dataset_id,
        g.username,
        body_data.entry_names,
        body_data.specification_names,
        body_data.status,
        body_data.priority,
        body_data.tag,
        body_data.comment,
    )


@api_v1.route("/datasets/<string:dataset_type>/<int:dataset_id>/records/revert", methods=["POST"])
@wrap_route("WRITE")
def revert_dataset_records_v1(dataset_type: str, dataset_id: int, body_data: DatasetRecordRevertBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.revert_records(
        dataset_id,
        body_data.revert_status,
        body_data.entry_names,
        body_data.specification_names,
    )


###################
# Contributed Values
###################
@api_v1.route("/datasets/<int:dataset_id>/contributed_values", methods=["GET"])
@wrap_route("READ")
def fetch_dataset_contributed_values_v1(dataset_id: int):
    return storage_socket.datasets.get_contributed_values(dataset_id)
