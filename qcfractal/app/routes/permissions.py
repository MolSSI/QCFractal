from functools import wraps
from urllib.parse import urlparse

from flask import current_app, request, g, jsonify
from flask_jwt_extended import (
    verify_jwt_in_request,
    get_jwt,
    get_jwt_identity,
    create_access_token,
    create_refresh_token,
    jwt_required,
)
from werkzeug.exceptions import Forbidden, BadRequest

from qcfractal.app import storage_socket
from qcfractal.app.routes.main import main
from qcfractal.exceptions import AuthenticationFailure
from qcfractal.interface.models import UserInfo
from qcfractal.policyuniverse import Policy


def check_access(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        """
        Call the route (fn) if allowed to access the url using the given
        permissions in the JWT token in the request headers

        1- If no security (JWT_ENABLED=False), always allow
        2- If JWT_ENABLED:
            if read allowed (allow_read=True), use the default read permissions
            otherwise, check against the logged-in user permissions
            from the headers' JWT token
        """

        # current_app.logger.debug(f"JWT_ENABLED: {current_app.config['JWT_ENABLED']}")
        # current_app.logger.debug(f"ALLOW_UNAUTHENTICATED_READ: {current_app.config['ALLOW_UNAUTHENTICATED_READ']}")
        # current_app.logger.debug(f"SECRET_KEY: {current_app.secret_key}")
        # current_app.logger.debug(f"SECRET_KEY: {current_app.config['SECRET_KEY']}")
        # current_app.logger.debug(f"JWT_SECRET_KEY: {current_app.config['JWT_SECRET_KEY']}")
        # current_app.logger.debug(f"JWT_ACCESS_TOKEN_EXPIRES: {current_app.config['JWT_ACCESS_TOKEN_EXPIRES']}")
        # current_app.logger.debug(f"JWT_REFRESH_TOKEN_EXPIRES: {current_app.config['JWT_REFRESH_TOKEN_EXPIRES']}")

        # if no auth required, always allowed
        if not current_app.config["JWT_ENABLED"]:
            return fn(*args, **kwargs)

        # load read permissions from DB if not read
        global _read_permissions
        if not _read_permissions:
            _read_permissions = storage_socket.role.get("read").permissions

        # if read is allowed without login, use read_permissions
        # otherwise, check logged-in permissions
        if current_app.config["ALLOW_UNAUTHENTICATED_READ"]:
            # don't raise exception if no JWT is found
            verify_jwt_in_request(optional=True)
        else:
            # read JWT token from request headers
            verify_jwt_in_request(optional=False)

        claims = get_jwt()
        permissions = claims.get("permissions", {})

        try:
            # host_url = request.host_url
            identity = get_jwt_identity() or "anonymous"
            resource = urlparse(request.url).path.split("/")[1]
            context = {
                "Principal": identity,
                "Action": request.method,
                "Resource": resource
                # "IpAddress": request.remote_addr,
                # "AccessTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
            current_app.logger.info(f"Permissions: {permissions}")
            current_app.logger.info(f"Context: {context}")
            policy = Policy(permissions)
            if not policy.evaluate(context):
                if not Policy(_read_permissions).evaluate(context):
                    return Forbidden(f"User {identity} is not authorized to access '{resource}' resource.")

            # Store the user in the global app/request context
            g.user = identity

        except Exception as e:
            current_app.logger.info("Error in evaluating JWT permissions: \n" + str(e))
            return BadRequest("Error in evaluating JWT permissions")

        return fn(*args, **kwargs)

    return wrapper


@main.route("/register", methods=["POST"])
def register():
    if request.is_json:
        username = request.json["username"]
        password = request.json["password"]
        fullname = request.json["fullname"]
        email = request.json["email"]
        organization = request.json["organization"]
    else:
        username = request.form["username"]
        password = request.form["password"]
        fullname = request.form["fullname"]
        email = request.form["email"]
        organization = request.form["organization"]

    role = "read"
    try:
        user_info = UserInfo(
            username=username,
            enabled=True,
            fullname=fullname,
            email=email,
            organization=organization,
            role=role,
        )
    except Exception as e:
        return jsonify(msg=f"Invalid user information: {str(e)}"), 500

    # add returns the password. Raises exception on error
    # Exceptions should be handled property by the flask errorhandlers
    pw = storage_socket.user.add(user_info, password=password)
    if password is None or len(password) == 0:
        return jsonify(msg="New user created!"), 201
    else:
        return jsonify(msg="New user created! Password is '{pw}'"), 201


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
    permissions = storage_socket.user.verify(username, password)

    access_token = create_access_token(identity=username, additional_claims={"permissions": permissions})
    # expires_delta=datetime.timedelta(days=3))
    refresh_token = create_refresh_token(identity=username)
    return jsonify(msg="Login succeeded!", access_token=access_token, refresh_token=refresh_token), 200


@main.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    username = get_jwt_identity()
    permissions = storage_socket.user.get_permissions(username)
    ret = {"access_token": create_access_token(identity=username, additional_claims={"permissions": permissions})}
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
    permissions = storage_socket.user.verify(username, password)

    access_token = create_access_token(
        identity=username, additionalclaims={"permissions": permissions.dict()}, fresh=True
    )
    return jsonify(msg="Fresh login succeeded!", access_token=access_token), 200
