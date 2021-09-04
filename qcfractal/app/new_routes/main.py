import json
import time
import traceback

from flask import Blueprint, g, request, current_app, jsonify
from qcelemental.util import deserialize
from werkzeug.exceptions import BadRequest, InternalServerError, HTTPException

from qcfractal.app import api_logger, storage_socket
from qcfractal.app.new_routes.helpers import _valid_encodings, SerializedResponse
from qcfractal.exceptions import UserReportableError, AuthenticationFailure

main = Blueprint("main", __name__)


@main.before_request
def before_request_func():
    ###############################################################
    # Deserialize the various encodings we support (like msgpack) #
    ###############################################################

    # Store timing information in the request/app context
    # g here refers to flask.g
    g.request_start = time.time()

    # default to "application/json"
    content_type = request.headers.get("Content-Type", "application/json")
    encoding = _valid_encodings.get(content_type, None)

    if encoding is None:
        raise BadRequest(f"Did not understand 'Content-Type {content_type}")

    try:
        # Check to see if we have a json that is encoded as bytes rather than a string
        if (encoding == "json") and isinstance(request.data, bytes):
            blob = request.data.decode()
        else:
            blob = request.data

        if blob:
            request.data = deserialize(blob, encoding)
        else:
            request.data = None
    except Exception as e:
        raise BadRequest(f"Could not deserialize body. {e}")


@main.after_request
def after_request_func(response: SerializedResponse):

    # Determine the time the request took
    # g here refers to flask.g
    request_duration = time.time() - g.request_start

    exclude_uris = ["/task_queue", "/service_queue", "/queue_manager"]

    # No associated data, so skip all of this
    # (maybe caused by not using portal or not using the REST API correctly?)
    if request.data is None:
        return response

    log_access = current_app.config["QCFRACTAL_CONFIG"].log_access
    if log_access and request.method == "GET" and request.path not in exclude_uris:
        extra_params = request.data.copy()
        if _logging_param_counts:
            for key in _logging_param_counts:
                if "data" in extra_params and extra_params["data"].get(key, None):
                    extra_params["data"][key] = len(extra_params["data"][key])

        if "data" in extra_params:
            extra_params["data"] = {k: v for k, v in extra_params["data"].items() if v is not None}

        extra_params = json.dumps(extra_params)

        log = api_logger.get_api_access_log(request=request, extra_params=extra_params)

        log["request_duration"] = request_duration
        log["user"] = g.user if "user" in g else None

        if isinstance(response.response, (bytes, str)):
            log["response_bytes"] = len(response.response)

        storage_socket.server_log.save_access(log)

    return response


@main.errorhandler(InternalServerError)
def handle_internal_error(error):
    # For otherwise unhandled errors
    # Do not report the details to the user. Instead, log it,
    # and send the user the error id

    # Obtain the original exception that caused the error
    # original = getattr(error, "original_exception", None)

    # Copy the headers to a dict, and remove the JWT stuff
    headers = dict(request.headers.items())
    headers.pop("Authorization", None)

    user = g.user if "user" in g else None
    error_log = {
        "error_text": traceback.format_exc(),
        "user": user,
        "request_path": request.full_path,
        "request_headers": str(headers),
        "request_body": str(request.data)[:8192],
    }

    # Log it to the internal error table
    err_id = storage_socket.server_log.save_error(error_log)

    msg = error.description + f"  **Refer to internal error id {err_id} when asking your admin**"
    return jsonify(msg=msg), error.code


@main.errorhandler(HTTPException)
def handle_http_exception(error):
    # This handles many errors, such as NotFound, Unauthorized, etc
    # These are all reportable to the user
    return jsonify(msg=str(error)), error.code


@main.errorhandler(UserReportableError)
def handle_userreport_error(error):
    # This handles any errors that are reportable to the user
    return jsonify(msg=str(error)), 400


@main.errorhandler(AuthenticationFailure)
def handle_auth_error(error):
    # This handles Authentication errors (invalid user, password, etc)
    return jsonify(msg=str(error)), 401


_logging_param_counts = {"id"}
