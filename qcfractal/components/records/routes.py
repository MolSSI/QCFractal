from flask import current_app, g

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import prefix_projection
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters, CommonBulkGetBody
from qcportal.exceptions import LimitExceededError
from qcportal.records import (
    RecordModifyBody,
    RecordQueryBody,
    RecordDeleteBody,
    RecordRevertBody,
)


@main.route("/v1/records/<int:record_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_records_v1(record_id: int, *, url_params: ProjURLParameters):
    return storage_socket.records.get([record_id], url_params.include, url_params.exclude)


@main.route("/v1/records/bulkGet", methods=["POST"])
@wrap_route(CommonBulkGetBody, None, "READ")
def bulk_get_records_v1(body_data: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    if len(body_data.id) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.id)} records - limit is {limit}")

    return storage_socket.records.get(body_data.id, body_data.include, body_data.exclude, body_data.missing_ok)


@main.route("/v1/records/<int:record_id>/compute_history", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_record_history_v1(record_id: int, *, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the compute history
    ch_includes, ch_excludes = prefix_projection(url_params, "compute_history")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["compute_history"]


@main.route("/v1/records/<int:record_id>/task", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_record_task_v1(record_id: int, *, url_params: ProjURLParameters):
    ch_includes, ch_excludes = prefix_projection(url_params, "task")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["task"]


@main.route("/v1/records/<int:record_id>/service", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_record_service_v1(record_id: int, *, url_params: ProjURLParameters):
    ch_includes, ch_excludes = prefix_projection(url_params, "service")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["service"]


@main.route("/v1/records/<int:record_id>/comments", methods=["GET"])
@wrap_route(None, None, "READ")
def get_record_comments_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["comments"])
    return rec[0]["comments"]


@main.route("/v1/records/<int:record_id>", methods=["DELETE"])
@wrap_route(None, None, "DELETE")
def delete_records_v1(record_id: int):
    return storage_socket.records.delete([record_id], soft_delete=True, delete_children=True)


@main.route("/v1/records/bulkDelete", methods=["POST"])
@wrap_route(RecordDeleteBody, None, "DELETE")
def bulk_delete_records_v1(body_data: RecordDeleteBody):
    return storage_socket.records.delete(
        body_data.record_id, soft_delete=body_data.soft_delete, delete_children=body_data.delete_children
    )


@main.route("/v1/records/revert", methods=["POST"])
@wrap_route(RecordRevertBody, None, "WRITE")
def revert_records_v1(body_data: RecordRevertBody):
    return storage_socket.records.revert_generic(body_data.record_id, body_data.revert_status)


@main.route("/v1/records", methods=["PATCH"])
@wrap_route(RecordModifyBody, None, "WRITE")
def modify_records_v1(body_data: RecordModifyBody):
    username = (g.user if "user" in g else None,)
    return storage_socket.records.modify_generic(body_data, username)


@main.route("/v1/records/query", methods=["POST"])
@wrap_route(RecordQueryBody, None, "READ")
def query_records_v1(body_data: RecordQueryBody):
    return storage_socket.records.query(body_data)


#################################################################
# COMMON HANDLERS
# These functions are common to all record types
# Note that the inputs are all the same, but the returned dicts
# are different
#################################################################
@main.route("/v1/records/<string:record_type>/<record_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_general_records_v1(record_type: str, record_id: int, *, url_params: ProjURLParameters):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get(record_id, url_params.include, url_params.exclude)


@main.route("/v1/records/<string:record_type>/bulkGet", methods=["POST"])
@wrap_route(CommonBulkGetBody, None, "READ")
def bulk_get_general_records_v1(record_type: str, *, body_data: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    if len(body_data.id) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.id)} records - limit is {limit}")

    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get(body_data.id, body_data.include, body_data.exclude, body_data.missing_ok)
