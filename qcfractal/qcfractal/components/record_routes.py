from typing import Optional

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.base_models import ProjURLParameters, CommonBulkGetBody
from qcportal.exceptions import LimitExceededError
from qcportal.record_models import (
    RecordModifyBody,
    RecordQueryFilters,
    RecordDeleteBody,
    RecordRevertBody,
)


#################################################################
# Base record route
# A few things can be done directly through /records (rather than
# a /records/<record_type>/ route
#################################################################


@api_v1.route("/records/query", methods=["POST"])
@wrap_route("READ")
def query_records_v1(body_data: RecordQueryFilters):
    return storage_socket.records.query(body_data)


@api_v1.route("/records/revert", methods=["POST"])
@wrap_route("WRITE")
def revert_records_v1(body_data: RecordRevertBody):
    return storage_socket.records.revert_generic(body_data.record_ids, body_data.revert_status)


@api_v1.route("/records", methods=["PATCH"])
@wrap_route("WRITE")
def modify_records_v1(body_data: RecordModifyBody):
    return storage_socket.records.modify_generic(
        body_data.record_ids,
        g.user_id,
        body_data.status,
        body_data.compute_priority,
        body_data.compute_tag,
        body_data.comment,
    )


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


@api_v1.route("/records/<int:record_id>/waiting_reason", methods=["GET"])
@wrap_route("READ")
def get_record_waiting_reason_v1(record_id: int):
    return storage_socket.records.get_waiting_reason(record_id)


#################################################################
# Routes for individual record types
# These can also be accessed through /records
#################################################################


@api_v1.route("/records/<string:record_type>/<int:record_id>", methods=["GET"])
@api_v1.route("/records/<int:record_id>", methods=["GET"])
@wrap_route("READ")
def get_records_v1(record_id: int, url_params: ProjURLParameters, record_type: Optional[str] = None):
    if record_type is None:
        return storage_socket.records.get([record_id], url_params.include, url_params.exclude)[0]
    else:
        record_socket = storage_socket.records.get_socket(record_type)
        return record_socket.get([record_id], url_params.include, url_params.exclude)[0]


@api_v1.route("/records/<string:record_type>/bulkGet", methods=["POST"])
@api_v1.route("/records/bulkGet", methods=["POST"])
@wrap_route("READ")
def bulk_get_records_v1(body_data: CommonBulkGetBody, record_type: Optional[str] = None):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    if len(body_data.ids) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.ids)} records - limit is {limit}")

    # Getting is handled a little differently. If no type specified, use the more generic version
    # in the upper-level record socket
    if record_type is None:
        return storage_socket.records.get(body_data.ids, body_data.include, body_data.exclude, body_data.missing_ok)
    else:
        record_socket = storage_socket.records.get_socket(record_type)
        return record_socket.get(body_data.ids, body_data.include, body_data.exclude, body_data.missing_ok)


@api_v1.route("/records/<string:record_type>/<int:record_id>", methods=["GET"])
@api_v1.route("/records/<int:record_id>", methods=["GET"])
@wrap_route("READ")
def get_general_records_v1(record_id: int, url_params: ProjURLParameters, record_type: Optional[str] = None):
    # Getting is handled a little differently. If no type specified, use the more generic version
    # in the upper-level record socket
    if record_type is None:
        records = storage_socket.records.get([record_id], url_params.include, url_params.exclude)
    else:
        record_socket = storage_socket.records.get_socket(record_type)
        records = record_socket.get([record_id], url_params.include, url_params.exclude)

    return records[0]


#################################################################
# Functions for getting individual record properties
# These are only accessible through routes for the individual
# record types
# But we can still handle them generically
#################################################################


@api_v1.route("/records/<string:record_type>/<int:record_id>/comments", methods=["GET"])
@wrap_route("READ")
def get_record_comments_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_comments(record_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/task", methods=["GET"])
@wrap_route("READ")
def get_record_task_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_task(record_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/service", methods=["GET"])
@wrap_route("READ")
def get_record_service_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_service(record_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/compute_history", methods=["GET"])
@wrap_route("READ")
def get_record_history_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_all_compute_history(record_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/compute_history/<int:history_id>", methods=["GET"])
@wrap_route("READ")
def get_record_history_single_v1(record_id: int, history_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_single_compute_history(record_id, history_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/compute_history/<int:history_id>/outputs", methods=["GET"])
@wrap_route("READ")
def get_record_outputs_v1(record_id: int, history_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_all_output_metadata(record_id, history_id)


@api_v1.route(
    "/records/<string:record_type>/<int:record_id>/compute_history/<int:history_id>/outputs/<string:output_type>",
    methods=["GET"],
)
@wrap_route("READ")
def get_record_outputs_single_v1(record_id: int, history_id: int, output_type: str, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_single_output_metadata(record_id, history_id, output_type)


@api_v1.route(
    "/records/<string:record_type>/<int:record_id>/compute_history/<int:history_id>/outputs/<string:output_type>/data",
    methods=["GET"],
)
@wrap_route("READ")
def get_record_outputs_data_v1(record_id: int, history_id: int, output_type: str, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_single_output_rawdata(record_id, history_id, output_type)


@api_v1.route("/records/<string:record_type>/<int:record_id>/native_files", methods=["GET"])
@wrap_route("READ")
def get_record_native_files_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_all_native_files_metadata(record_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/native_files/<string:name>", methods=["GET"])
@wrap_route("READ")
def get_record_native_file_single_v1(record_id: int, name: str, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_single_native_file_metadata(record_id, name)


@api_v1.route("/records/<string:record_type>/<int:record_id>/native_files/<string:name>/data", methods=["GET"])
@wrap_route("READ")
def get_record_native_file_data_v1(record_id: int, name: str, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_single_native_file_rawdata(record_id, name)


@api_v1.route("/records/<string:record_type>/<int:record_id>/children_status", methods=["GET"])
@wrap_route("READ")
def get_record_children_status_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_children_status(record_id)


@api_v1.route("/records/<string:record_type>/<int:record_id>/children_errors", methods=["GET"])
@wrap_route("READ")
def get_record_children_errors_v1(record_id: int, record_type: str):
    record_socket = storage_socket.records.get_socket(record_type)
    return record_socket.get_children_errors(record_id)
