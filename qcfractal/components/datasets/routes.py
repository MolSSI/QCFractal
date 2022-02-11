from typing import Dict

from flask import g

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters
from qcportal.datasets import (
    DatasetQueryModel,
    DatasetGetRecordItemsBody,
    DatasetGetEntryBody,
    DatasetSubmitBody,
    DatasetDeleteStrBody,
    DatasetRecordModifyBody,
    DatasetDeleteRecordItemsBody,
    DatasetRecordRevertBody,
)


@main.route("/v1/datasets", methods=["GET"])
@wrap_route(None, None, "READ")
def list_dataset_v1():
    return storage_socket.datasets.list()


@main.route("/v1/datasets/<int:dataset_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_general_dataset_v1(dataset_id: int, *, url_params: ProjURLParameters):

    with storage_socket.session_scope(True) as session:
        ds_type = storage_socket.datasets.lookup_type(dataset_id, session=session)
        ds_socket = storage_socket.datasets.get_socket(ds_type)
        return ds_socket.get(dataset_id, url_params.include, url_params.exclude, session=session)


@main.route("/v1/datasets/query", methods=["POST"])
@wrap_route(DatasetQueryModel, None, "READ")
def query_general_dataset_v1(body_data: DatasetQueryModel):
    with storage_socket.session_scope(True) as session:
        dataset_id = storage_socket.datasets.lookup_id(body_data.dataset_type, body_data.name, session=session)
        ds_socket = storage_socket.datasets.lookup_type(dataset_id, session=session)
        return ds_socket.get(dataset_id, body_data.include, body_data.exclude, session=session)


#################################################################
# COMMON FUNCTIONS
# These functions are common to all datasets
# Note that the inputs are all the same, but the returned dicts
# are different
#################################################################

#########################
# Getting info
#########################
@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_dataset_v1(dataset_type: str, dataset_id: int, *, url_params: ProjURLParameters):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get(
        dataset_id,
        url_params.include,
        url_params.exclude,
    )


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/status", methods=["GET"])
@wrap_route(None, None, "READ")
def get_dataset_status_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.status(dataset_id)


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/detailed_status", methods=["GET"])
@wrap_route(None, None, "READ")
def get_dataset_detailed_status_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.detailed_status(dataset_id)


#########################
# Computation submission
#########################
@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/submit", methods=["POST"])
@wrap_route(DatasetSubmitBody, None, "WRITE")
def submit_dataset_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetSubmitBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.submit(
        dataset_id,
        entry_names=body_data.entry_names,
        specification_names=body_data.specification_names,
        tag=body_data.tag,
        priority=body_data.priority,
    )


###################
# Specifications
###################
@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/specifications", methods=["GET"])
@wrap_route(None, None, "READ")
def get_dataset_specifications_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    ds_data = ds_socket.get(dataset_id, ["specifications.*", "specifications.specification"], None, False)
    return ds_data["specifications"]


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/specifications/bulkDelete", methods=["POST"])
@wrap_route(DatasetDeleteStrBody, None, "DELETE")
def delete_dataset_specifications_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetDeleteStrBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.delete_specifications(dataset_id, body_data.names, body_data.delete_records)


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/specifications", methods=["PATCH"])
@wrap_route(Dict[str, str], None, "WRITE")
def rename_dataset_specifications_v1(dataset_type: str, dataset_id: int, *, body_data: Dict[str, str]):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.rename_specifications(dataset_id, body_data)


###################
# Entries
###################
@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/entry_names", methods=["GET"])
@wrap_route(None, None, "READ")
def get_dataset_entry_names_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_entry_names(dataset_id)


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/entries/bulkDelete", methods=["POST"])
@wrap_route(DatasetDeleteStrBody, None, "DELETE")
def delete_dataset_entries_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetDeleteStrBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.delete_entries(dataset_id, body_data.names, body_data.delete_records)


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/entries/bulkGet", methods=["POST"])
@wrap_route(DatasetGetEntryBody, None, "READ")
def get_dataset_entries_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetGetEntryBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_entries(
        dataset_id,
        entry_names=body_data.names,
        include=body_data.include,
        exclude=body_data.exclude,
        missing_ok=body_data.missing_ok,
    )


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/entries", methods=["PATCH"])
@wrap_route(Dict[str, str], None, "WRITE")
def rename_dataset_entries_v1(dataset_type: str, dataset_id: int, *, body_data: Dict[str, str]):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.rename_entries(dataset_id, body_data)


#########################
# Record items
#########################
@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/record_items/bulkGet", methods=["POST"])
@wrap_route(DatasetGetRecordItemsBody, None, "READ")
def get_dataset_record_items_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetGetRecordItemsBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_record_items(
        dataset_id,
        entry_names=body_data.entry_names,
        specification_names=body_data.specification_names,
        include=body_data.include,
        exclude=body_data.exclude,
    )


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/record_items/bulkDelete", methods=["POST"])
@wrap_route(DatasetDeleteRecordItemsBody, None, "DELETE")
def delete_dataset_record_items_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetDeleteRecordItemsBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.delete_record_items(
        dataset_id,
        entry_names=body_data.entry_names,
        specification_names=body_data.specification_names,
        delete_records=body_data.delete_records,
    )


#########################
# Record modification
#########################
@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/records", methods=["PATCH"])
@wrap_route(DatasetRecordModifyBody, None, "WRITE")
def modify_dataset_records_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetRecordModifyBody):
    username = (g.user if "user" in g else None,)
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.modify_records(dataset_id, body_data, username)


@main.route("/v1/datasets/<string:dataset_type>/<int:dataset_id>/records/revert", methods=["POST"])
@wrap_route(DatasetRecordRevertBody, None, "WRITE")
def revert_dataset_records_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetRecordRevertBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.revert_records(dataset_id, body_data)
