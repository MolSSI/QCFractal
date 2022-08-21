from flask import current_app

from qcfractal.app import main, wrap_route, storage_socket
from qcportal.base_models import CommonBulkGetNamesBody
from qcportal.exceptions import LimitExceededError
from qcportal.managers import (
    ManagerActivationBody,
    ManagerUpdateBody,
    ManagerStatusEnum,
    ManagerQueryFilters,
)
from qcportal.utils import calculate_limit


@main.route("/v1/managers", methods=["POST"])
@wrap_route("WRITE")
def activate_manager_v1(body_data: ManagerActivationBody):
    return storage_socket.managers.activate(
        name_data=body_data.name_data,
        manager_version=body_data.manager_version,
        username=body_data.username,
        programs=body_data.programs,
        tags=body_data.tags,
    )


@main.route("/v1/managers/<string:name>", methods=["PATCH"])
@wrap_route("WRITE")
def update_manager_v1(name: str, body_data: ManagerUpdateBody):
    # This endpoint is used for heartbeats and deactivation

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
@wrap_route("READ")
def get_managers_v1(name: str):
    return storage_socket.managers.get([name])[0]


@main.route("/v1/managers/bulkGet", methods=["POST"])
@wrap_route("READ")
def bulk_get_managers_v1(body_data: CommonBulkGetNamesBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_managers
    if len(body_data.names) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.names)} manager records - limit is {limit}")

    return storage_socket.managers.get(body_data.names, body_data.include, body_data.exclude, body_data.missing_ok)


@main.route("/v1/managers/query", methods=["POST"])
@wrap_route("READ")
def query_managers_v1(body_data: ManagerQueryFilters):

    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_managers
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.managers.query(body_data)
