from flask import current_app, g

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import prefix_projection, storage_socket
from qcportal.base_models import ProjURLParameters, CommonBulkGetBody
from qcportal.exceptions import LimitExceededError
from qcportal.record_models import (
    RecordModifyBody,
    RecordQueryFilters,
    RecordDeleteBody,
    RecordRevertBody,
)


@api_v1.route("/records/<int:record_id>", methods=["GET"])
@wrap_route("READ")
def get_records_v1(record_id: int, url_params: ProjURLParameters):
    records = storage_socket.records.get([record_id], url_params.include, url_params.exclude)
    return records[0]


@api_v1.route("/records/bulkGet", methods=["POST"])
@wrap_route("READ")
def bulk_get_records_v1(body_data: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    if len(body_data.ids) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.ids)} records - limit is {limit}")

    return storage_socket.records.get(body_data.ids, body_data.include, body_data.exclude, body_data.missing_ok)


@api_v1.route("/records/<int:record_id>/compute_history", methods=["GET"])
@wrap_route("READ")
def get_record_history_v1(record_id: int, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the compute history
    ch_includes, ch_excludes = prefix_projection(url_params, "compute_history")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["compute_history"]


@api_v1.route("/records/<int:record_id>/task", methods=["GET"])
@wrap_route("READ")
def get_record_task_v1(record_id: int, url_params: ProjURLParameters):
    ch_includes, ch_excludes = prefix_projection(url_params, "task")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["task"]


@api_v1.route("/records/<int:record_id>/service", methods=["GET"])
@wrap_route("READ")
def get_record_service_v1(record_id: int, url_params: ProjURLParameters):
    ch_includes, ch_excludes = prefix_projection(url_params, "service")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["service"]


@api_v1.route("/records/<int:record_id>/comments", methods=["GET"])
@wrap_route("READ")
def get_record_comments_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["comments"])
    return rec[0]["comments"]


@api_v1.route("/records/<int:record_id>/native_files", methods=["GET"])
@wrap_route("READ")
def get_record_native_files_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["native_files"])
    return rec[0]["native_files"]


@api_v1.route("/records/<int:record_id>", methods=["DELETE"])
@wrap_route("DELETE")
def delete_records_v1(record_id: int):
    return storage_socket.records.delete([record_id], soft_delete=True, delete_children=True)


@api_v1.route("/records/bulkDelete", methods=["POST"])
@wrap_route("DELETE")
def bulk_delete_records_v1(body_data: RecordDeleteBody):
    return storage_socket.records.delete(
        body_data.record_ids, soft_delete=body_data.soft_delete, delete_children=body_data.delete_children
    )


@api_v1.route("/records/revert", methods=["POST"])
@wrap_route("WRITE")
def revert_records_v1(body_data: RecordRevertBody):
    return storage_socket.records.revert_generic(body_data.record_ids, body_data.revert_status)


@api_v1.route("/records", methods=["PATCH"])
@wrap_route("WRITE")
def modify_records_v1(body_data: RecordModifyBody):

    return storage_socket.records.modify_generic(
        body_data.record_ids, g.user_id, body_data.status, body_data.priority, body_data.tag, body_data.comment
    )


@api_v1.route("/records/query", methods=["POST"])
@wrap_route("READ")
def query_records_v1(body_data: RecordQueryFilters):
    return storage_socket.records.query(body_data)


#################################################################
# COMMON HANDLERS
# These functions are common to all record types
# Note that the inputs are all the same, but the returned dicts
# are different
#################################################################
@api_v1.route("/records/<string:record_type>/<record_id>", methods=["GET"])
@wrap_route("READ")
def get_general_records_v1(record_type: str, record_id: int, url_params: ProjURLParameters):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get(record_id, url_params.include, url_params.exclude)


@api_v1.route("/records/<string:record_type>/bulkGet", methods=["POST"])
@wrap_route("READ")
def bulk_get_general_records_v1(record_type: str, body_data: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    if len(body_data.ids) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.ids)} records - limit is {limit}")

    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get(body_data.ids, body_data.include, body_data.exclude, body_data.missing_ok)