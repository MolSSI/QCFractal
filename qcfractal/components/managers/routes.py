from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.base_models import CommonBulkGetNamesBody
from qcportal.exceptions import LimitExceededError
from qcportal.managers import (
    ManagerActivationBody,
    ManagerUpdateBody,
    ManagerStatusEnum,
    ManagerQueryBody,
)
from qcportal.utils import calculate_limit


@main.route("/v1/managers", methods=["POST"])
@wrap_route(ManagerActivationBody, None, "WRITE")
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


@main.route("/v1/managers/<string:name>", methods=["PATCH"])
@wrap_route(ManagerUpdateBody, None, "WRITE")
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


@main.route("/v1/managers/<string:name>", methods=["GET"])
@wrap_route(None, None, "READ")
def get_managers_v1(name: str):
    return storage_socket.managers.get([name])[0]


@main.route("/v1/managers/bulkGet", methods=["POST"])
@wrap_route(CommonBulkGetNamesBody, None, "READ")
def bulk_get_managers_v1(body_data: CommonBulkGetNamesBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_managers
    if body_data.name is not None and len(body_data.name) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.name)} manager records - limit is {limit}")

    return storage_socket.managers.get(body_data.name, body_data.include, body_data.include, body_data.missing_ok)


@main.route("/v1/managers/query", methods=["POST"])
@wrap_route(ManagerQueryBody, None, "READ")
def query_managers_v1(body_data: ManagerQueryBody):

    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_managers

    return storage_socket.managers.query(
        manager_id=body_data.id,
        name=body_data.name,
        cluster=body_data.cluster,
        hostname=body_data.hostname,
        status=body_data.status,
        modified_before=body_data.modified_before,
        modified_after=body_data.modified_after,
        include=body_data.include,
        exclude=body_data.exclude,
        limit=calculate_limit(max_limit, body_data.limit),
        skip=body_data.skip,
    )
