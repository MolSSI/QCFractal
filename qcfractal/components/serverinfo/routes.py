from flask import current_app

from qcfractal import __version__ as qcfractal_version
from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcfractal.client_versions import client_version_lower_limit, client_version_upper_limit
from qcportal.serverinfo import (
    AccessLogSummaryParameters,
    AccessLogQueryBody,
    ServerStatsQueryParameters,
    ErrorLogQueryBody,
    DeleteBeforeDateBody,
)
from qcportal.utils import calculate_limit


@main.route("/v1/information", methods=["GET"])
@wrap_route(None, None, "READ")
def get_information():
    qcf_cfg = current_app.config["QCFRACTAL_CONFIG"]

    # TODO FOR RELEASE - change lower and upper version limits?
    public_info = {
        "name": qcf_cfg.name,
        "manager_heartbeat_frequency": qcf_cfg.heartbeat_frequency,
        "version": qcfractal_version,
        "api_limits": qcf_cfg.api_limits.dict(),
        "client_version_lower_limit": client_version_lower_limit,
        "client_version_upper_limit": client_version_upper_limit,
    }

    return public_info


@main.route("/v1/access_logs/query", methods=["POST"])
@wrap_route(AccessLogQueryBody, None, "READ")
def query_access_log_v1(body_data: AccessLogQueryBody):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_access_logs

    return storage_socket.serverinfo.query_access_log(
        access_type=body_data.access_type,
        access_method=body_data.access_method,
        username=body_data.username,
        before=body_data.before,
        after=body_data.after,
        include=body_data.include,
        exclude=body_data.exclude,
        limit=calculate_limit(max_limit, body_data.limit),
        skip=body_data.skip,
    )


@main.route("/v1/access_logs/bulkDelete", methods=["POST"])
@wrap_route(DeleteBeforeDateBody, None, "DELETE")
def delete_access_log_v1(body_data: DeleteBeforeDateBody):
    return storage_socket.serverinfo.delete_access_logs(before=body_data.before)


@main.route("/v1/access_logs/summary", methods=["GET"])
@wrap_route(None, AccessLogSummaryParameters, "READ")
def query_access_summary_v1(url_params: AccessLogSummaryParameters):
    return storage_socket.serverinfo.query_access_summary(
        group_by=url_params.group_by, before=url_params.before, after=url_params.after
    )


@main.route("/v1/server_stats", methods=["GET"])
@wrap_route(None, ServerStatsQueryParameters, "READ")
def query_server_stats_v1(url_params: ServerStatsQueryParameters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_server_stats

    return storage_socket.serverinfo.query_server_stats(
        before=url_params.before,
        after=url_params.after,
        limit=calculate_limit(max_limit, url_params.limit),
        skip=url_params.skip,
    )


@main.route("/v1/server_stats/bulkDelete", methods=["POST"])
@wrap_route(DeleteBeforeDateBody, None, "DELETE")
def delete_server_stats_v1(body_data: DeleteBeforeDateBody):
    return storage_socket.serverinfo.delete_server_stats(before=body_data.before)


@main.route("/v1/server_errors/query", methods=["POST"])
@wrap_route(ErrorLogQueryBody, None, "READ")
def query_error_log_v1(body_data: ErrorLogQueryBody):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_server_stats

    return storage_socket.serverinfo.query_error_log(
        error_id=body_data.id,
        username=body_data.username,
        before=body_data.before,
        after=body_data.after,
        limit=calculate_limit(max_limit, body_data.limit),
        skip=body_data.skip,
    )


@main.route("/v1/server_errors/bulkDelete", methods=["POST"])
@wrap_route(DeleteBeforeDateBody, None, "DELETE")
def delete_server_error_log_v1(body_data: DeleteBeforeDateBody):
    return storage_socket.serverinfo.delete_error_logs(before=body_data.before)
