from flask import current_app

from qcfractal import __version__ as qcfractal_version
from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import SerializedResponse, parse_bodymodel, convert_get_response_metadata
from qcfractal.app.routes import check_access
from qcfractal.interface.models.rest_models import (
    AccessLogGETBody,
    AccessLogGETResponse,
    ServerStatsGETBody,
    ServerStatsGETResponse,
    AccessSummaryGETBody,
    AccessSummaryGETResponse,
    InternalErrorLogGETBody,
    InternalErrorLogGETResponse,
)


@main.route("/information", methods=["GET"])
def get_information():
    qcf_cfg = current_app.config["QCFRACTAL_CONFIG"]

    # TODO FOR RELEASE - change lower and upper version limits?
    public_info = {
        "name": qcf_cfg.name,
        "manager_heartbeat_frequency": qcf_cfg.heartbeat_frequency,
        "version": qcfractal_version,
        "query_limits": qcf_cfg.response_limits.dict(),
        "client_lower_version_limit": qcfractal_version,
        "client_upper_version_limit": qcfractal_version,
    }

    return SerializedResponse(public_info)


@main.route("/access/log", methods=["GET"])
@check_access
def query_access_log_v1():
    """
    Queries access logs
    """

    body = parse_bodymodel(AccessLogGETBody)
    meta, logs = storage_socket.serverinfo.query_access_logs(**{**body.data.dict(), **body.meta.dict()})
    response = AccessLogGETResponse(meta=convert_get_response_metadata(meta, missing=[]), data=logs)
    return SerializedResponse(response)


@main.route("/server_stats", methods=["GET"])
@check_access
def get_server_stats():
    """
    Queries access logs
    """

    body = parse_bodymodel(ServerStatsGETBody)
    meta, logs = storage_socket.serverinfo.query_stats(**{**body.data.dict(), **body.meta.dict()})
    response = ServerStatsGETResponse(meta=convert_get_response_metadata(meta, missing=[]), data=logs)
    return SerializedResponse(response)


@main.route("/access/summary", methods=["GET"])
@check_access
def query_access_summary_v1():
    """
    Queries access logs
    """

    body = parse_bodymodel(AccessSummaryGETBody)
    summary = storage_socket.serverinfo.query_access_summary(**{**body.data.dict()})
    response = AccessSummaryGETResponse(data=summary)
    return SerializedResponse(response)


@main.route("/error", methods=["GET"])
@check_access
def query_internal_error_log_v1():
    """
    Queries internal error logs
    """

    body = parse_bodymodel(InternalErrorLogGETBody)
    meta, logs = storage_socket.serverinfo.query_error_logs(**{**body.data.dict(), **body.meta.dict()})
    response = InternalErrorLogGETResponse(meta=convert_get_response_metadata(meta, missing=[]), data=logs)
    return SerializedResponse(response)
