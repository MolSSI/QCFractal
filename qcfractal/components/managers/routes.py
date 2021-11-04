from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.common_rest import CommonGetURLParametersName
from qcfractal.portal.components.managers import (
    ManagerActivationBody,
    ManagerUpdateBody,
    ManagerStatusEnum,
    ManagerQueryBody,
)


@main.route("/v1/manager", methods=["POST"])
@wrap_route(ManagerActivationBody, None)
@check_access
def activate_manager_v1(body_data: ManagerActivationBody):
    """Activates/Registers a manager for use with the server"""

    return storage_socket.managers.activate(
        name_data=body_data.name_data,
        manager_version=body_data.manager_version,
        qcengine_version=body_data.qcengine_version,
        username=body_data.username,
        programs=body_data.programs,
        tags=body_data.tags,
    )


@main.route("/v1/manager/<string:name>", methods=["PATCH"])
@wrap_route(ManagerUpdateBody, None)
@check_access
def update_manager_v1(name: str, body_data: ManagerUpdateBody):
    """Updates a manager's info

    This endpoint is used for heartbeats and deactivation
    """

    # Will raise an exception if manager is not active
    storage_socket.managers.update_resource_stats(
        name=name,
        total_worker_walltime=body_data.total_worker_walltime,
        total_task_walltime=body_data.total_task_walltime,
        active_tasks=body_data.active_tasks,
        active_cores=body_data.active_cores,
        active_memory=body_data.active_memory,
    )

    # Deactivate if specified
    if body_data.status != ManagerStatusEnum.active:
        storage_socket.managers.deactivate([name])


@main.route("/v1/manager", methods=["GET"])
@main.route("/v1/manager/<string:name>", methods=["GET"])
@wrap_route(None, CommonGetURLParametersName)
@check_access
def get_managers_v1(name: Optional[str] = None, *, url_params: CommonGetURLParametersName):
    return get_helper(name, url_params.name, url_params.missing_ok, storage_socket.managers.get)


@main.route("/v1/manager/query", methods=["POST"])
@wrap_route(ManagerQueryBody, None)
@check_access
def query_managers_v1(body_data: ManagerQueryBody):
    return storage_socket.managers.query(
        id=body_data.id,
        name=body_data.name,
        cluster=body_data.cluster,
        hostname=body_data.hostname,
        status=body_data.status,
        modified_before=body_data.modified_before,
        modified_after=body_data.modified_after,
        include=body_data.include,
        exclude=body_data.exclude,
        limit=body_data.limit,
        skip=body_data.skip,
    )
