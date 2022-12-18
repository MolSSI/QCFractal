import time
import traceback
from typing import Dict, Any

from flask import g, request, current_app, jsonify, Response
from werkzeug.exceptions import InternalServerError, HTTPException

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.flask_app import storage_socket, get_url_major_component
from qcportal.exceptions import UserReportableError, AuthenticationFailure, ComputeManagerError


#####################################################################
# This registers "global" error handlers and before/after
# request functions. Not that we are using _app_ decorators,
# which make them global, even though we are using a decorator
# from a specific blueprint
#####################################################################


@api_v1.before_app_request
def before_request_func():
    # Store timing information in the request/app context
    # g here refers to flask.g
    g.request_start = time.time()

    if request.data:
        g.request_bytes = len(request.data)
    else:
        g.request_bytes = 0


@api_v1.after_app_request
def after_request_func(response: Response):
    #################################################################
    # NOTE: Do not touch response.response! It may mess up streaming
    #       responses and result in no content being sent
    #################################################################

    # Determine the time the request took
    # g here refers to flask.g

    request_duration = time.time() - g.request_start

    log_access = current_app.config["QCFRACTAL_CONFIG"].log_access
    if log_access and request.path != "/api/v1/ping":
        # What we are going to log to the DB
        log: Dict[str, Any] = {}
        access_type = get_url_major_component(request.path)
        access_method = request.method  # GET, POST, etc

        log["access_type"] = access_type
        log["access_method"] = access_method

        # Replace null in URI (since a malevolent user can do that)
        log["full_uri"] = request.path.replace("\0", "\\0")

        # get the real IP address behind a proxy or ngnix
        real_ip = request.headers.get("X-Real-IP", None)

        # The IP address is the last address listed in access_route, which
        # comes from the X-FORWARDED-FOR header
        # (If access_route is empty, use the original request ip)
        if real_ip is None:
            real_ip = request.access_route[-1] if len(request.access_route) > 0 else request.remote_addr

        if real_ip:
            log["ip_address"] = real_ip

        log["user_agent"] = request.headers["User-Agent"]

        log["request_bytes"] = g.request_bytes
        log["request_duration"] = request_duration
        log["user_id"] = g.get("user_id", None)

        response_bytes = response.content_length
        log["response_bytes"] = response_bytes

        storage_socket.serverinfo.save_access(log)
        current_app.logger.debug(
            f"{access_method} {access_type}: {g.request_bytes} -> {response_bytes} [{request_duration*1000:.1f}ms]"
        )

    return response


@api_v1.app_errorhandler(InternalServerError)
def handle_internal_error(error):
    # For otherwise unhandled errors
    # Do not report the details to the user. Instead, log it,
    # and send the user the error id

    # Copy the headers to a dict, and remove the JWT stuff
    headers = dict(request.headers.items())
    headers.pop("Authorization", None)

    tb = traceback.format_exc()

    error_log = {
        "error_text": tb,
        "user_id": g.get("user_id", None),
        "request_path": request.full_path,
        "request_headers": str(headers),
        "request_body": str(request.data)[:8192],
    }

    # Log it to the internal error table
    err_id = storage_socket.serverinfo.save_error(error_log)

    # Should we hide the error from the user?
    hide = current_app.config["QCFRACTAL_CONFIG"].hide_internal_errors

    if hide:
        msg = error.description + f"  **Refer to internal error id {err_id} when asking your admin**"
        return jsonify(msg=msg), error.code
    else:
        return jsonify(msg=tb), error.code


@api_v1.app_errorhandler(HTTPException)
def handle_http_exception(error):
    # This handles many errors, such as NotFound, Unauthorized, etc
    # These are all reportable to the user
    return jsonify(msg=str(error)), error.code


@api_v1.app_errorhandler(UserReportableError)
def handle_userreport_error(error):
    # This handles any errors that are reportable to the user
    return jsonify(msg=str(error)), 400


@api_v1.app_errorhandler(AuthenticationFailure)
def handle_auth_error(error):
    # This handles Authentication errors (invalid user, password, etc)
    return jsonify(msg=str(error)), 401


@api_v1.app_errorhandler(ComputeManagerError)
def handle_compute_manager_error(error: ComputeManagerError):
    # Handle compute manager errors
    return jsonify(msg=str(error)), 400
