from typing import Optional

from flask import g

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper, prefix_projection
from qcfractal.app.routes import wrap_route
from qcportal.base_models import CommonGetProjURLParameters
from qcportal.records import (
    RecordModifyBody,
    RecordQueryBody,
    RecordDeleteURLParameters,
    RecordRevertBodyParameters,
    RecordStatusEnum,
)


@main.route("/v1/record", methods=["GET"])
@main.route("/v1/record/<int:record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
def get_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        record_id, url_params.id, url_params.include, None, url_params.missing_ok, storage_socket.records.get
    )


@main.route("/v1/record/<int:record_id>/compute_history", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
def get_record_history_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    # adjust the includes/excludes to refer to the compute history
    ch_includes, ch_excludes = prefix_projection(url_params, "compute_history")
    rec = storage_socket.records.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["compute_history"]


@main.route("/v1/record/<int:record_id>/task", methods=["GET"])
@wrap_route(None, None)
def get_record_task_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["task"])
    return rec[0]["task"]


@main.route("/v1/record/<int:record_id>/service", methods=["GET"])
@wrap_route(None, None)
def get_record_service_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["service.*", "service.dependencies"])
    return rec[0]["service"]


@main.route("/v1/record/<int:record_id>/comments", methods=["GET"])
@wrap_route(None, None)
def get_record_comments_v1(record_id: int):
    rec = storage_socket.records.get([record_id], include=["comments"])
    return rec[0]["comments"]


@main.route("/v1/record", methods=["DELETE"])
@main.route("/v1/record/<int:record_id>", methods=["DELETE"])
@wrap_route(None, RecordDeleteURLParameters)
def delete_records_v1(record_id: Optional[int] = None, *, url_params: RecordDeleteURLParameters):
    return delete_helper(
        record_id,
        url_params.record_id,
        storage_socket.records.delete,
        soft_delete=url_params.soft_delete,
        delete_children=url_params.delete_children,
    )


@main.route("/v1/record/revert", methods=["POST"])
@wrap_route(RecordRevertBodyParameters, None)
def revert_records_v1(body_data: RecordRevertBodyParameters):
    if body_data.revert_status == RecordStatusEnum.cancelled:
        return storage_socket.records.uncancel(body_data.record_id)

    if body_data.revert_status == RecordStatusEnum.invalid:
        return storage_socket.records.uninvalidate(body_data.record_id)

    if body_data.revert_status == RecordStatusEnum.deleted:
        return storage_socket.records.undelete(body_data.record_id)

    raise RuntimeError(f"Unknown status to revert: ", body_data.revert_status)


@main.route("/v1/record", methods=["PATCH"])
@main.route("/v1/record/<int:record_id>", methods=["PATCH"])
@wrap_route(RecordModifyBody, None)
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
            if body_data.status == RecordStatusEnum.invalid:
                return storage_socket.records.invalidate(record_id=record_id, session=session)

            # ignore all other statuses

        if body_data.tag is not None or body_data.priority is not None:
            return storage_socket.records.modify(
                record_id,
                new_tag=body_data.tag,
                new_priority=body_data.priority,
                session=session,
            )

        if body_data.comment:
            return storage_socket.records.add_comment(
                record_id=record_id,
                username=g.user if "user" in g else None,
                comment=body_data.comment,
                session=session,
            )


@main.route("/v1/record/query", methods=["POST"])
@wrap_route(RecordQueryBody, None)
def query_records_v1(body_data: RecordQueryBody):
    return storage_socket.records.query(body_data)
