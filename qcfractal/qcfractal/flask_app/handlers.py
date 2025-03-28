import datetime
import time
import traceback
from typing import Dict, Any

from flask import g, request, current_app, jsonify, Response
from flask_jwt_extended import (
    get_jwt,
    get_jwt_identity,
    set_access_cookies,
    get_jwt_request_location,
)
from jwt.exceptions import InvalidSubjectError
from werkzeug.exceptions import InternalServerError, HTTPException

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.helpers import access_token_from_user
from qcportal.auth import UserInfo, RoleInfo
from qcportal.exceptions import UserReportableError, AuthenticationFailure, ComputeManagerError, AuthorizationFailure
from .home_v1 import home_v1


#####################################################################
# This registers "global" error handlers and before/after
# request functions. Not that we are using _app_ decorators,
# which make them global, even though we are using a decorator
# from a specific blueprint
#####################################################################


@home_v1.before_app_request
def before_request_func():
    # Store timing information in the request/app context
    # g here refers to flask.g
    g.request_start = time.time()

    if request.data:
        g.request_bytes = len(request.data)
    else:
        g.request_bytes = 0


@home_v1.after_app_request
def after_request_func(response: Response):
    #################################################################
    # NOTE: Do not touch response.response! It may mess up streaming
    #       responses and result in no content being sent
    #################################################################

    # Determine the time the request took
    # g here refers to flask.g

    request_duration = time.time() - g.request_start

    log_access = current_app.config["QCFRACTAL_CONFIG"].log_access
    if log_access:
        # What we are going to log to the DB
        log: Dict[str, Any] = {}

        log["module"] = request.blueprint
        log["method"] = request.method

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

        log["user_agent"] = request.headers.get("User-Agent", "")

        log["request_bytes"] = 0 if g.request_bytes is None else g.request_bytes
        log["request_duration"] = request_duration
        log["user_id"] = g.get("user_id", None)

        response_bytes = response.content_length
        log["response_bytes"] = 0 if response_bytes is None else response_bytes

        storage_socket.serverinfo.save_access(log)
        current_app.logger.debug(
            f"{request.method} {request.blueprint}: {g.request_bytes} -> {response_bytes} [{request_duration*1000:.1f}ms]"
        )

        # Basically taken from the flask-jwt-extended docs
        # If there is a jwt and it comes from cookies (ie, being accessed by a browser), then
        # automatically refresh if necessary
        jwt_loc = get_jwt_request_location()
        if jwt_loc == "cookies":
            expires_timestamp = get_jwt()["exp"]
            now = datetime.datetime.now(datetime.timezone.utc)
            target_timestamp = datetime.datetime.timestamp(now + datetime.timedelta(minutes=10))

            # print("JWT EXPIRES: ", datetime.datetime.fromtimestamp(expires_timestamp))
            # print("TARGET: ", datetime.datetime.fromtimestamp(target_timestamp))

            # is users token expiring in the next 10 minutes?
            if target_timestamp > expires_timestamp:
                user_id = get_jwt_identity()

                user_dict = storage_socket.users.get(user_id)
                role_dict = storage_socket.roles.get(user_dict["role"])
                user_info = UserInfo(**user_dict)
                role_info = RoleInfo(**role_dict)

                access_token = access_token_from_user(user_info, role_info)
                set_access_cookies(response, access_token)
        return response

    return response


@home_v1.app_errorhandler(InternalServerError)
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


@home_v1.app_errorhandler(HTTPException)
def handle_http_exception(error):
    # This handles many errors, such as NotFound, Unauthorized, etc
    # These are all reportable to the user
    return jsonify(msg=str(error)), error.code


@home_v1.app_errorhandler(UserReportableError)
def handle_userreport_error(error):
    # This handles any errors that are reportable to the user
    return jsonify(msg=str(error)), 400


@home_v1.app_errorhandler(AuthenticationFailure)
def handle_authentication_error(error):
    # This handles Authentication errors (invalid user, password, etc)
    # Or if the user tries to request a resource without being logged in
    return (
        jsonify(
            msg=str(error),
            user_id=g.user_id if "user_id" in g else None,
            user_name=g.user_name if "user_name" in g else None,
        ),
        401,
    )


@home_v1.app_errorhandler(AuthorizationFailure)
def handle_authorization_error(error):
    # This handles when a logged-in user does not have access to something
    return (
        jsonify(
            msg=str(error),
            user_id=g.user_id if "user_id" in g else None,
            user_name=g.user_name if "user_name" in g else None,
        ),
        403,
    )


@home_v1.app_errorhandler(ComputeManagerError)
def handle_compute_manager_error(error: ComputeManagerError):
    # Handle compute manager errors
    return jsonify(msg=str(error)), 400


@home_v1.app_errorhandler(InvalidSubjectError)
def handle_old_tokens(error):
    # Handle old tokens that have integers as the subject
    # Just say they have been expired, and you need to login again
    return jsonify(msg="Token has expired"), 401
