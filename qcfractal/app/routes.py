import time
import traceback
from functools import wraps
from typing import Optional, Type, Callable
from urllib.parse import urlparse

import pydantic
import qcelemental
from flask import g, request, current_app, jsonify, Response
from flask_jwt_extended import (
    verify_jwt_in_request,
    get_jwt,
    get_jwt_identity,
    create_access_token,
    create_refresh_token,
    jwt_required,
)
from werkzeug.exceptions import BadRequest, InternalServerError, HTTPException, Forbidden

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import _valid_encodings, SerializedResponse
from qcfractal.exceptions import UserReportableError, AuthenticationFailure
from qcfractal.app.policyuniverse import Policy
from qcfractal.portal.serialization import deserialize, serialize

_read_permissions = {}


@main.before_request
def before_request_func():
    ###############################################################
    # Deserialize the various encodings we support (like msgpack) #
    ###############################################################

    # Store timing information in the request/app context
    # g here refers to flask.g
    g.request_start = time.time()

    # The rest of this function is only for old endpoints
    if request.path.startswith("/v1/"):
        return

    # default to "application/json"
    content_type = request.headers.get("Content-Type", "application/json")
    encoding = _valid_encodings.get(content_type, None)

    if encoding is None:
        raise BadRequest(f"Did not understand Content-Type {content_type}")

    try:
        # Check to see if we have a json that is encoded as bytes rather than a string
        if (encoding == "json") and isinstance(request.data, bytes):
            blob = request.data.decode()
        else:
            blob = request.data

        if blob:
            request.data = qcelemental.util.deserialize(blob, encoding)
        else:
            request.data = None
    except Exception as e:
        raise BadRequest(f"Could not deserialize body. {e}")


def wrap_route(body_model: Optional[Type], query_model: Optional[Type[pydantic.BaseModel]] = None) -> Callable:
    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            content_type = request.headers.get("Content-Type")

            # Find an appropriate return type (from the "Accept" header)
            # Flask helpfully parses this for us
            # By default, use plain json
            possible_types = ["text/html", "application/msgpack", "application/json"]
            accept_type = request.accept_mimetypes.best_match(possible_types, "application/json")

            # If text/html is first, then this is probably a browser. Send json, as most browsers
            # will accept that
            if accept_type == "text/html":
                accept_type = "application/json"

            # 1.) The body is stored in request.data
            if body_model is not None:
                if content_type is None:
                    raise BadRequest("No Content-Type specified")

                if not request.data:
                    raise BadRequest("Expected body, but it is empty")

                try:
                    deserialized_data = deserialize(request.data, content_type)
                    g.validated_data = pydantic.parse_obj_as(body_model, deserialized_data)
                except Exception as e:
                    raise BadRequest("Invalid body: " + str(e))

            # 2.) Query parameters are in request.args
            if query_model is not None:
                try:
                    g.validated_args = query_model(**request.args.to_dict(False))
                except Exception as e:
                    raise BadRequest("Invalid request arguments: " + str(e))

            # Now call the function, and validate the output
            ret = fn(*args, **kwargs)

            # Serialize the output
            serialized = serialize(ret, accept_type)
            return Response(serialized, content_type=accept_type)

        return wrapper

    return decorate


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
        # What we are going to log to the DB
        log = {}
        log["access_type"] = request.path[1:]  # remove /
        log["access_method"] = request.method  # GET or POST

        # get the real IP address behind a proxy or ngnix
        real_ip = request.headers.get("X-Real-IP", None)

        # The IP address is the last address listed in access_route, which
        # comes from the X-FORWARDED-FOR header
        # (If access_route is empty, use the original request ip)
        if real_ip is None:
            real_ip = request.access_route[-1] if len(request.access_route) > 0 else request.remote_addr

        log["ip_address"] = real_ip
        log["user_agent"] = request.headers["User-Agent"]

        log["request_duration"] = request_duration
        log["user"] = g.user if "user" in g else None

        if isinstance(response.response, (bytes, str)):
            log["response_bytes"] = len(response.response)

        storage_socket.serverinfo.save_access(log)

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
    err_id = storage_socket.serverinfo.save_error(error_log)

    # Should we hide the error from the user?
    hide = current_app.config["QCFRACTAL_CONFIG"].hide_internal_errors

    if hide:
        msg = error.description + f"  **Refer to internal error id {err_id} when asking your admin**"
        return jsonify(msg=msg), error.code
    else:
        return jsonify(traceback.format_exc())


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


def check_access(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        """
        Call the route (fn) if allowed to access the url using the given
        permissions in the JWT token in the request headers

        1- If no security (enable_security is False), always allow
        2- If enable_security:
            if read allowed (allow_unauthenticated_read=True), use the default read permissions
            otherwise, check against the logged-in user permissions
            from the headers' JWT token
        """

        security_enabled = current_app.config["QCFRACTAL_CONFIG"].enable_security
        allow_unauthenticated_read = current_app.config["QCFRACTAL_CONFIG"].allow_unauthenticated_read

        # if no auth required, always allowed
        if not security_enabled:
            return fn(*args, **kwargs)

        # load read permissions from DB if not read
        global _read_permissions
        if not _read_permissions:
            _read_permissions = storage_socket.roles.get("read")["permissions"]

        # if read is allowed without login, use read_permissions
        # otherwise, check logged-in permissions
        if allow_unauthenticated_read:
            # don't raise exception if no JWT is found
            verify_jwt_in_request(optional=True)
        else:
            # read JWT token from request headers
            verify_jwt_in_request(optional=False)

        try:
            claims = get_jwt()
            permissions = claims.get("permissions", {})

            identity = get_jwt_identity()  # may be None

            # Pull the second part of the URL (ie, /v1/molecule -> molecule)
            # We will consistently ignore the version prefix
            resource = urlparse(request.url).path.split("/")[2]
            context = {
                "Principal": identity,
                "Action": request.method,
                "Resource": resource
                # "IpAddress": request.remote_addr,
                # "AccessTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
            policy = Policy(permissions)
            if not policy.evaluate(context):
                # If that doesn't work, but we allow unauthenticated read, then try that
                if not allow_unauthenticated_read:
                    raise Forbidden(f"User {identity} is not authorized to access '{resource}'")

                if not Policy(_read_permissions).evaluate(context):
                    raise Forbidden(f"User {identity} is not authorized to access '{resource}'")

            # Store the user in the global app/request context
            g.user = identity

        except Forbidden:
            raise
        except Exception as e:
            current_app.logger.info("Error in evaluating JWT permissions: \n" + str(e))
            raise BadRequest("Error in evaluating JWT permissions")

        return fn(*args, **kwargs)

    return wrapper


# @main.route("/register", methods=["POST"])
# def register():
#    if request.is_json:
#        username = request.json["username"]
#        password = request.json["password"]
#        fullname = request.json["fullname"]
#        email = request.json["email"]
#        organization = request.json["organization"]
#    else:
#        username = request.form["username"]
#        password = request.form["password"]
#        fullname = request.form["fullname"]
#        email = request.form["email"]
#        organization = request.form["organization"]
#
#    role = "read"
#    try:
#        user_info = UserInfo(
#            username=username,
#            enabled=True,
#            fullname=fullname,
#            email=email,
#            organization=organization,
#            role=role,
#        )
#    except Exception as e:
#        return jsonify(msg=f"Invalid user information: {str(e)}"), 500
#
#    # add returns the password. Raises exception on error
#    # Exceptions should be handled property by the flask errorhandlers
#    pw = storage_socket.users.add(user_info, password=password)
#    if password is None or len(password) == 0:
#        return jsonify(msg="New user created!"), 201
#    else:
#        return jsonify(msg="New user created! Password is '{pw}'"), 201


@main.route("/login", methods=["POST"])
def login():
    try:
        if request.is_json:
            username = request.json["username"]
            password = request.json["password"]
        else:
            username = request.form["username"]
            password = request.form["password"]
    except Exception:
        raise AuthenticationFailure("Invalid/malformed login request")

    if username is None:
        raise AuthenticationFailure("No username provided for login")
    if password is None:
        raise AuthenticationFailure("No password provided for login")

    # Raises exceptions on error
    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
    # This should be handled properly by the flask errorhandlers
    permissions = storage_socket.users.verify(username, password)

    access_token = create_access_token(identity=username, additional_claims={"permissions": permissions})
    # expires_delta=datetime.timedelta(days=3))
    refresh_token = create_refresh_token(identity=username)
    return jsonify(msg="Login succeeded!", access_token=access_token, refresh_token=refresh_token), 200


@main.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    username = get_jwt_identity()
    role, permissions = storage_socket.users.get_permissions(username)
    ret = {
        "access_token": create_access_token(
            identity=username, additional_claims={"role": role, "permissions": permissions}
        )
    }
    return jsonify(ret), 200


@main.route("/fresh-login", methods=["POST"])
def fresh_login():
    if request.is_json:
        username = request.json["username"]
        password = request.json["password"]
    else:
        username = request.form["username"]
        password = request.form["password"]

    # Raises exceptions on error
    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
    # This should be handled properly by the flask errorhandlers
    permissions = storage_socket.users.verify(username, password)

    access_token = create_access_token(
        identity=username, additional_claims={"permissions": permissions.dict()}, fresh=True
    )
    return jsonify(msg="Fresh login succeeded!", access_token=access_token), 200
