from flask import jsonify, g
from flask_jwt_extended import (
    jwt_required,
    verify_jwt_in_request,
    get_jwt,
    get_jwt_identity,
    set_access_cookies,
    unset_jwt_cookies,
)

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.auth_v1.blueprint import auth_v1
from qcfractal.flask_app.helpers import get_all_endpoints, access_token_from_user, login_and_get_jwt


@auth_v1.route("/login", methods=["POST"])
def login():
    access_token, refresh_token = login_and_get_jwt(get_refresh_token=True)
    return jsonify(msg="Login succeeded!", access_token=access_token, refresh_token=refresh_token), 200


@auth_v1.route("/browser_login", methods=["POST"])
def browser_login():
    # Browsers don't need the refresh token
    access_token, _ = login_and_get_jwt(get_refresh_token=False)
    response = jsonify(msg="Login succeeded!")
    set_access_cookies(response, access_token)
    return response, 200


@auth_v1.route("/browser_logout", methods=["POST"])
def browser_logout():
    response = jsonify(msg="Logout successful!")
    unset_jwt_cookies(response)
    return response, 200


@auth_v1.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    user_id = get_jwt_identity()

    user_info, role_info = storage_socket.auth.verify(user_id)
    access_token = access_token_from_user(user_info, role_info)

    # For logging purposes (in the after_request_func)
    g.user_id = user_id

    ret = jsonify(access_token=access_token)
    return ret, 200


@auth_v1.route("/allowed", methods=["GET"])
def get_allowed_actions():
    all_endpoints = get_all_endpoints()
    all_actions = {"READ", "WRITE", "DELETE"}

    # JWT is optional
    verify_jwt_in_request(optional=True)

    username = get_jwt_identity()
    claims = get_jwt()
    policies = claims.get("permissions", ())

    subject = {"username": username}

    allowed = storage_socket.auth.allowed_actions(
        subject=subject, resources=all_endpoints, actions=all_actions, policies=policies
    )

    return jsonify(allowed), 200
