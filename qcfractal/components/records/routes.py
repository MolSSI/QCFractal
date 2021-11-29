from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.base_models import CommonGetProjURLParameters, CommonDeleteURLParameters
from qcfractal.portal.records import (
    RecordModifyBody,
    RecordQueryBody,
    RecordDeleteURLParameters,
    ComputeHistoryURLParameters,
)
from qcfractal.portal.records import RecordStatusEnum


@main.route("/v1/record", methods=["GET"])
@main.route("/v1/record/<int:record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        record_id, url_params.id, url_params.include, None, url_params.missing_ok, storage_socket.records.get
    )


@main.route("/v1/record/<int:record_id>/compute_history", methods=["GET"])
@wrap_route(None, ComputeHistoryURLParameters)
@check_access
def get_record_history_v1(record_id: Optional[int] = None, *, url_params: ComputeHistoryURLParameters):
    return storage_socket.records.get_history(record_id, url_params.include_outputs)


@main.route("/v1/record/<int:record_id>/task", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_record_task_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["task"])
    return rec[0]["task"]


@main.route("/v1/record", methods=["DELETE"])
@main.route("/v1/record/<int:record_id>", methods=["DELETE"])
@wrap_route(None, RecordDeleteURLParameters)
@check_access
def delete_records_v1(record_id: Optional[int] = None, *, url_params: RecordDeleteURLParameters):
    return delete_helper(
        record_id, url_params.record_id, storage_socket.records.delete, soft_delete=url_params.soft_delete
    )


@main.route("/v1/record", methods=["PATCH"])
@main.route("/v1/record/<int:record_id>", methods=["PATCH"])
@wrap_route(RecordModifyBody, None)
@check_access
def modify_records_v1(record_id: Optional[int] = None, *, body_data: RecordModifyBody):

    if record_id is not None:
        record_id = [record_id]
    elif body_data.record_id is not None:
        record_id = body_data.record_id
    else:
        return {}

    # do all in a single session
    with storage_socket.session_scope() as session:
        if body_data.status is not None:
            if body_data.status == RecordStatusEnum.waiting:
                return storage_socket.records.reset(record_id=record_id, session=session)
            if body_data.status == RecordStatusEnum.cancelled:
                return storage_socket.records.cancel(record_id=record_id, session=session)
            if body_data.status == RecordStatusEnum.deleted:
                return storage_socket.records.delete(record_id=record_id, session=session)

            # ignore all other statuses

        if body_data.delete_tag or body_data.tag is not None or body_data.priority is not None:
            return storage_socket.records.modify(
                record_id,
                new_tag=body_data.tag,
                new_priority=body_data.priority,
                delete_tag=body_data.delete_tag,
                session=session,
            )


@main.route("/v1/record/query", methods=["POST"])
@wrap_route(RecordQueryBody, None)
@check_access
def query_records_v1(body_data: RecordQueryBody):
    return storage_socket.records.query(body_data)
