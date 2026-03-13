from flask import current_app

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcfractal.flask_app.helpers import get_public_server_information
from qcportal.serverinfo import (
    AccessLogSummaryFilters,
    AccessLogQueryFilters,
    ErrorLogQueryFilters,
    DeleteBeforeDateBody,
)
from qcportal.utils import calculate_limit


@api_v1.route("/information", methods=["GET"])
@check_permissions("information", "read")
@serialization()
def get_information():
    return get_public_server_information()


@api_v1.route("/motd", methods=["GET"])
@check_permissions("information", "read")
@serialization()
def get_motd():
    return storage_socket.serverinfo.get_motd()


@api_v1.route("/motd", methods=["PUT"])
@check_permissions("information", "modify")
@serialization()
def set_motd(body_data: str):
    return storage_socket.serverinfo.set_motd(body_data)


@api_v1.route("/access_logs/query", methods=["POST"])
@check_permissions("access_log", "read")
@serialization()
def query_access_log_v1(body_data: AccessLogQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_access_logs
    body_data.limit = calculate_limit(max_limit, body_data.limit)
    return storage_socket.serverinfo.query_access_log(body_data)


@api_v1.route("/access_logs/bulkDelete", methods=["POST"])
@check_permissions("access_log", "delete")
@serialization()
def delete_access_log_v1(body_data: DeleteBeforeDateBody):
    return storage_socket.serverinfo.delete_access_logs(before=body_data.before)


@api_v1.route("/access_logs/summary", methods=["GET"])
@check_permissions("access_log", "read")
@serialization()
def query_access_summary_v1(url_params: AccessLogSummaryFilters):
    return storage_socket.serverinfo.query_access_summary(url_params)


@api_v1.route("/server_errors/query", methods=["POST"])
@check_permissions("server_errors", "read")
@serialization()
def query_error_log_v1(body_data: ErrorLogQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_error_logs
    body_data.limit = calculate_limit(max_limit, body_data.limit)
    return storage_socket.serverinfo.query_error_log(body_data)


@api_v1.route("/server_errors/bulkDelete", methods=["POST"])
@check_permissions("server_errors", "delete")
@serialization()
def delete_server_error_log_v1(body_data: DeleteBeforeDateBody):
    return storage_socket.serverinfo.delete_error_logs(before=body_data.before)
