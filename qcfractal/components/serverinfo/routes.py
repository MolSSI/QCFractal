from flask import current_app

from qcfractal import __version__ as qcfractal_version
from qcfractal.app import main, storage_socket
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.serverinfo import (
    AccessLogQuerySummaryParameters,
    AccessLogQueryParameters,
    ServerStatsQueryParameters,
    ErrorLogQueryParameters,
    DeleteBeforeDateParameters,
)


@main.route("/v1/information", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_information():
    qcf_cfg = current_app.config["QCFRACTAL_CONFIG"]

    # TODO FOR RELEASE - change lower and upper version limits?
    public_info = {
        "name": qcf_cfg.name,
        "manager_heartbeat_frequency": qcf_cfg.heartbeat_frequency,
        "version": qcfractal_version,
        "response_limits": qcf_cfg.response_limits.dict(),
        "client_lower_version_limit": qcfractal_version,
        "client_upper_version_limit": qcfractal_version,
    }

    return public_info


@main.route("/v1/access", methods=["GET"])
@wrap_route(None, AccessLogQueryParameters)
@check_access
def query_access_log_v1(url_params: AccessLogQueryParameters):
    return storage_socket.serverinfo.query_access_log(
        access_type=url_params.access_type,
        access_method=url_params.access_method,
        username=url_params.username,
        before=url_params.before,
        after=url_params.after,
        include=url_params.include,
        exclude=url_params.exclude,
        limit=url_params.limit,
        skip=url_params.skip,
    )


@main.route("/v1/access", methods=["DELETE"])
@wrap_route(None, DeleteBeforeDateParameters)
@check_access
def delete_access_log_v1(url_params: DeleteBeforeDateParameters):
    return storage_socket.serverinfo.delete_access_logs(before=url_params.before)


@main.route("/v1/access/summary", methods=["GET"])
@wrap_route(None, AccessLogQuerySummaryParameters)
@check_access
def query_access_summary_v1(url_params: AccessLogQuerySummaryParameters):
    return storage_socket.serverinfo.query_access_summary(
        group_by=url_params.group_by, before=url_params.before, after=url_params.after
    )


@main.route("/v1/server_stats", methods=["GET"])
@wrap_route(None, ServerStatsQueryParameters)
@check_access
def query_server_stats(url_params: ServerStatsQueryParameters):
    return storage_socket.serverinfo.query_server_stats(
        before=url_params.before, after=url_params.after, limit=url_params.limit, skip=url_params.skip
    )


@main.route("/v1/server_stats", methods=["DELETE"])
@wrap_route(None, DeleteBeforeDateParameters)
@check_access
def delete_server_stats_v1(url_params: DeleteBeforeDateParameters):
    return storage_socket.serverinfo.delete_server_stats(before=url_params.before)


@main.route("/v1/server_error", methods=["GET"])
@wrap_route(None, ErrorLogQueryParameters)
@check_access
def query_error_log_v1(url_params: ErrorLogQueryParameters):
    return storage_socket.serverinfo.query_error_log(
        error_id=url_params.id,
        username=url_params.username,
        before=url_params.before,
        after=url_params.after,
        limit=url_params.limit,
        skip=url_params.skip,
    )


@main.route("/v1/server_error", methods=["DELETE"])
@wrap_route(None, DeleteBeforeDateParameters)
@check_access
def delete_server_error_log_v1(url_params: DeleteBeforeDateParameters):
    return storage_socket.serverinfo.delete_error_logs(before=url_params.before)
