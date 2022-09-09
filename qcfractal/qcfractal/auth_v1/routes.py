from typing import List, Tuple

from flask import request, current_app, jsonify
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    jwt_required,
    verify_jwt_in_request,
    get_jwt,
    get_jwt_identity,
)

from qcfractal.auth_v1.blueprint import auth_v1
from qcfractal.auth_v1.policyuniverse import Policy
from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.helpers import get_unauth_read_permissions, get_all_endpoints
from qcportal.exceptions import AuthenticationFailure

# Endpoints not accessible if security is disabled
_protected_endpoints = {"users", "roles", "me"}


@auth_v1.route("/login", methods=["POST"])
def login():
    try:
        if request.is_json:
            username = request.json["username"]
            password = request.json["password"]
        else:
            username = request.form["username"]
            password = request.form["password"]
    except Exception:
        current_app.logger.info("Invalid/malformed login request")
        raise AuthenticationFailure("Invalid/malformed login request")

    if username is None:
        current_app.logger.info("No username provided for login")
        raise AuthenticationFailure("No username provided for login")
    if password is None:
        current_app.logger.info(f"No username provided for login of user {username}")
        raise AuthenticationFailure("No password provided for login")

    # Raises exceptions on error
    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
    # This should be handled properly by the flask errorhandlers
    try:
        permissions = storage_socket.users.verify(username, password)
    except AuthenticationFailure as e:
        current_app.logger.info(f"Authentication failed for user {username}: {str(e)}")
        raise

    access_token = create_access_token(identity=username, additional_claims={"permissions": permissions})
    # expires_delta=datetime.timedelta(days=3))
    refresh_token = create_refresh_token(identity=username)

    current_app.logger.info(f"Successful login for user {username}")
    return jsonify(msg="Login succeeded!", access_token=access_token, refresh_token=refresh_token), 200


@auth_v1.route("/refresh", methods=["POST"])
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


@auth_v1.route("/allowed", methods=["GET"])
def get_allowed_actions():
    # Read in config parameters
    security_enabled: bool = current_app.config["QCFRACTAL_CONFIG"].enable_security
    allow_unauthenticated_read: bool = current_app.config["QCFRACTAL_CONFIG"].allow_unauthenticated_read

    all_endpoints = get_all_endpoints()
    all_actions = {"READ", "WRITE", "DELETE"}

    allowed: List[Tuple[str, str]] = []

    # JWT is optional
    verify_jwt_in_request(optional=True)

    # if no auth required, always allowed, except for protected endpoints
    if security_enabled is False:
        for endpoint in all_endpoints:
            endpoint_last = endpoint.split("/")[-1]
            if endpoint_last in _protected_endpoints:
                continue
            allowed.extend((endpoint, x) for x in all_actions)
    else:
        read_permissions = get_unauth_read_permissions()
        read_policy = Policy(read_permissions)

        identity = get_jwt_identity()  # may be None
        claims = get_jwt()
        permissions = claims.get("permissions", {})
        policy = Policy(permissions)

        for endpoint in all_endpoints:
            for action in all_actions:
                context = {"Principal": identity, "Action": action, "Resource": endpoint}
                if policy.evaluate(context):
                    allowed.append((endpoint, action))
                elif allow_unauthenticated_read and read_policy.evaluate(context) and not endpoint.endswith("/me"):
                    allowed.append((endpoint, action))

    return jsonify(allowed), 200


# @auth.route("/fresh-login", methods=["POST"])
# def fresh_login():
#    if request.is_json:
#        username = request.json["username"]
#        password = request.json["password"]
#    else:
#        username = request.form["username"]
#        password = request.form["password"]
#
#    # Raises exceptions on error
#    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
#    # This should be handled properly by the flask errorhandlers
#    permissions = storage_socket.users.verify(username, password)
#
#    access_token = create_access_token(
#        identity=username, additional_claims={"permissions": permissions.dict()}, fresh=True
#    )
#    return jsonify(msg="Fresh login succeeded!", access_token=access_token), 200


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
