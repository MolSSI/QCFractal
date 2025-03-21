from flask import current_app

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcfractal.flask_app.compute_v1.blueprint import compute_v1
from qcportal.base_models import CommonBulkGetNamesBody
from qcportal.exceptions import LimitExceededError
from qcportal.managers import (
    ManagerActivationBody,
    ManagerUpdateBody,
    ManagerStatusEnum,
    ManagerQueryFilters,
)
from qcportal.utils import calculate_limit


##################################################
# Routes that deal with manager activation, etc
##################################################


@compute_v1.route("/managers", methods=["POST"])
@wrap_route("WRITE")
def activate_manager_v1(body_data: ManagerActivationBody):
    return storage_socket.managers.activate(
        name_data=body_data.name_data,
        manager_version=body_data.manager_version,
        username=body_data.username,
        programs=body_data.programs,
        compute_tags=body_data.compute_tags,
    )


@compute_v1.route("/managers/<string:name>", methods=["PATCH"])
@wrap_route("WRITE")
def update_manager_v1(name: str, body_data: ManagerUpdateBody):
    # This endpoint is used for heartbeats and deactivation

    # Will raise an exception if manager is not active
    storage_socket.managers.update_resource_stats(
        name=name,
        total_cpu_hours=body_data.total_cpu_hours,
        active_tasks=body_data.active_tasks,
        active_cores=body_data.active_cores,
        active_memory=body_data.active_memory,
    )

    # Deactivate if specified
    if body_data.status != ManagerStatusEnum.active:
        storage_socket.managers.deactivate([name])


######################################################
# Routes for the user API to get information about managers
######################################################


@api_v1.route("/managers/<string:name>", methods=["GET"])
@wrap_route("READ")
def get_managers_v1(name: str):
    return storage_socket.managers.get([name])[0]


@api_v1.route("/managers/bulkGet", methods=["POST"])
@wrap_route("READ")
def bulk_get_managers_v1(body_data: CommonBulkGetNamesBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_managers
    if len(body_data.names) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.names)} manager records - limit is {limit}")

    return storage_socket.managers.get(body_data.names, body_data.include, body_data.exclude, body_data.missing_ok)


@api_v1.route("/managers/<string:name>/log", methods=["GET"])
@wrap_route("READ")
def get_manager_log_v1(name: str):
    return storage_socket.managers.get_log(name)


@api_v1.route("/managers/query", methods=["POST"])
@wrap_route("READ")
def query_managers_v1(body_data: ManagerQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_managers
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.managers.query(body_data)
