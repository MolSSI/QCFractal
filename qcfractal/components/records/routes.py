from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.common_rest import CommonGetProjURLParameters, CommonDeleteURLParameters
from qcfractal.portal.components.records import (
    RecordModifyBody,
    RecordQueryBody,
    RecordStatusEnum,
    ComputeHistoryURLParameters,
)


# These are used by the flask app. The flask app will
# import this file, which will cause all the routes in these
# subdirectories to be registered with the blueprint
# These are used by the flask app. The flask app will
# import this file, which will cause all the routes in these
# subdirectories to be registered with the blueprint
from .singlepoint import routes
from .optimization import routes
from .gridoptimization import routes
from .torsiondrive import routes


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


@main.route("/v1/record", methods=["DELETE"])
@main.route("/v1/record/<int:record_id>", methods=["DELETE"])
@wrap_route(None, CommonDeleteURLParameters)
@check_access
def delete_records_v1(record_id: Optional[int] = None, *, url_params: CommonDeleteURLParameters):
    return delete_helper(record_id, url_params.id, storage_socket.records.delete)


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
        return

    # do all in a single session
    with storage_socket.session_scope() as session:
        if body_data.status is not None:
            if body_data.status == RecordStatusEnum.waiting:
                storage_socket.records.reset(record_id=record_id, session=session)
            if body_data.status == RecordStatusEnum.cancelled:
                storage_socket.records.cancel(record_id=record_id, session=session)
            if body_data.status == RecordStatusEnum.deleted:
                storage_socket.records.delete(record_id=record_id, session=session)

            # ignore all other statuses

        if body_data.delete_tag or body_data.tag is not None or body_data.priority is not None:
            storage_socket.records.modify_task(
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
    return storage_socket.records.query(
        id=body_data.id,
        record_type=body_data.record_type,
        manager_name=body_data.manager_name,
        status=body_data.status,
        created_before=body_data.created_before,
        created_after=body_data.created_after,
        modified_before=body_data.modified_before,
        modified_after=body_data.modified_after,
        include=body_data.include,
        exclude=body_data.exclude,
        limit=body_data.limit,
        skip=body_data.skip,
    )
