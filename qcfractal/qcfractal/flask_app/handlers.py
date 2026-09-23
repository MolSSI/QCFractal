import time
import traceback
from typing import Dict, Any

from flask import g, request, current_app, jsonify, Response
from jwt.exceptions import InvalidSubjectError
from werkzeug.exceptions import InternalServerError, HTTPException, TooManyRequests, MethodNotAllowed, NotFound
from werkzeug.routing import RequestRedirect

from qcfractal.flask_app import storage_socket
from qcportal.exceptions import (
    UserReportableError,
    AuthenticationFailure,
    ComputeManagerError,
    AuthorizationFailure,
    SecurityNotEnabledError,
)
from .home_v1 import home_v1

#####################################################################
# This registers "global" error handlers and before/after
# request functions. Not that we are using _app_ decorators,
# which make them global, even though we are using a decorator
# from a specific blueprint
#####################################################################

# Request headers whose values must never be persisted (credentials, session keys)
_REDACTED_HEADERS = {"authorization", "proxy-authorization", "cookie", "x-api-key"}

# Maximum number of bytes of a request body to store in the internal error log
_MAX_LOGGED_BODY = 8192


def redacted_request_headers() -> Dict[str, str]:
    """
    Returns the headers of the current request, with credential-bearing values replaced
    """
    return {k: ("<redacted>" if k.lower() in _REDACTED_HEADERS else v) for k, v in request.headers.items()}


def redacted_request_body() -> str:
    """
    Returns a representation of the current request body that is safe to persist

    Bodies of authentication requests, password changes, and anything that appears to carry
    a password are replaced with a short description so that plaintext credentials never
    reach the internal error log.
    """
    data = request.data
    if not data:
        return ""

    path = request.path.lower()
    if path.startswith("/auth/") or "password" in path or b"password" in data.lower():
        content_type = request.headers.get("Content-Type", "")
        return f"<redacted: {len(data)} bytes, content-type {content_type!r}>"

    return str(data)[:_MAX_LOGGED_BODY]


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
        log["api_token_id"] = g.get("api_token_id", None)

        response_bytes = response.content_length
        log["response_bytes"] = 0 if response_bytes is None else response_bytes

        storage_socket.serverinfo.save_access(log)
        current_app.logger.debug(
            f"{request.method} {request.blueprint}: {g.request_bytes} -> {response_bytes} [{request_duration*1000:.1f}ms]"
        )

    return response


@home_v1.app_errorhandler(InternalServerError)
def handle_internal_error(error):
    # For otherwise unhandled errors
    # Do not report the details to the user. Instead, log it,
    # and send the user the error id

    # Headers and body are stored for debugging, but never any credentials or session keys
    tb = traceback.format_exc()

    error_log = {
        "error_text": tb,
        "user_id": g.get("user_id", None),
        "request_path": request.full_path,
        "request_headers": str(redacted_request_headers()),
        "request_body": redacted_request_body(),
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


@home_v1.app_errorhandler(TooManyRequests)
def handle_too_many_requests(error: TooManyRequests):
    # Rate-limited (e.g. too many failed logins). Include Retry-After if we know it
    response = jsonify(msg=error.description)
    if getattr(error, "retry_after", None) is not None:
        response.headers["Retry-After"] = str(error.retry_after)
    return response, error.code


def _path_matches_a_real_route(path: str) -> bool:
    """
    True if some route other than the catch-all homepage route matches the given path

    The homepage blueprint registers a catch-all route (`/<path:file_path>`, GET only) so it
    can serve a static site or redirect for arbitrary paths. Because that rule matches every
    path, Werkzeug considers it a "match" (with the wrong method) for any unknown, non-GET
    request, and raises MethodNotAllowed (405) instead of NotFound (404). To tell a genuine
    405 (a real, registered endpoint that just doesn't support this method) apart from that,
    we re-match the path against ROUTES_WITHOUT_HOMEPAGE_MAP - a copy of the URL map (built
    once at app-creation time; see flask_app.py) with the homepage route excluded.
    """
    routes_without_homepage = current_app.config["ROUTES_WITHOUT_HOMEPAGE_MAP"]
    adapter = routes_without_homepage.bind(request.host)
    try:
        adapter.match(path, method=request.method)
        return True
    except (MethodNotAllowed, RequestRedirect):
        # MethodNotAllowed: a real rule matches this path, just not this method
        # RequestRedirect: a real rule matches (eg a trailing-slash mismatch)
        return True
    except NotFound:
        return False


@home_v1.app_errorhandler(MethodNotAllowed)
def handle_method_not_allowed(error):
    # See _path_matches_a_real_route - avoid leaking a 405 for paths that don't correspond to
    # any real endpoint, which would otherwise happen because of the homepage catch-all route
    if not _path_matches_a_real_route(request.path):
        return jsonify(msg="404 Not Found: The requested URL was not found on the server."), 404

    return jsonify(msg=str(error)), error.code


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
            username=g.username if "username" in g else None,
        ),
        401,
    )


@home_v1.app_errorhandler(AuthorizationFailure)
def handle_authorization_error(error: AuthorizationFailure):
    # This handles when a logged-in user does not have access to something
    return (
        jsonify(
            msg=f"Forbidden: {str(error)}",
            user_id=g.user_id if "user_id" in g else None,
            username=g.username if "username" in g else None,
        ),
        403,
    )


@home_v1.app_errorhandler(SecurityNotEnabledError)
def handle_compute_manager_error(error: SecurityNotEnabledError):
    # Handle compute manager errors
    return jsonify(msg=str(error)), 401


@home_v1.app_errorhandler(ComputeManagerError)
def handle_compute_manager_error(error: ComputeManagerError):
    # Handle compute manager errors
    return jsonify(msg=str(error)), 400


@home_v1.app_errorhandler(InvalidSubjectError)
def handle_old_tokens(error):
    # Handle old tokens that have integers as the subject
    # Just say they have been expired, and you need to login again
    return jsonify(msg="Token has expired"), 401
