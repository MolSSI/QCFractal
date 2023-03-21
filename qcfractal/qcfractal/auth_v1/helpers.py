from __future__ import annotations

from typing import Tuple, Optional

from flask import request, current_app
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
)

from qcfractal.flask_app import storage_socket
from qcportal.auth import UserInfo, RoleInfo
from qcportal.exceptions import AuthenticationFailure


def access_token_from_user(user_info: UserInfo, role_info: RoleInfo):
    """
    Creates a JWT access token from user/role information
    """
    return create_access_token(
        identity=user_info.id,
        additional_claims={
            "username": user_info.username,
            "role": user_info.role,
            "groups": user_info.groups,
            "permissions": role_info.permissions.dict(),
        },
    )


def login_and_get_jwt(get_refresh_token: bool) -> Tuple[str, Optional[str]]:
    """
    Handle a login from flask

    This function authenticates the username/password sent to flask, and returns the JWT tokens.
    It handles the username/password being stored in json as well as form data.

    If get_refresh_token is True, then a refresh token is also return. Otherwise, None is returned
    for the refresh token.

    Returns
    -------
    :
        The access token and optionally the refresh token
    """
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
        current_app.logger.info(f"No password provided for login of user {username}")
        raise AuthenticationFailure("No password provided for login")

    try:
        user_info = storage_socket.auth.authenticate(username, password)
        role_dict = storage_socket.roles.get(user_info.role)
        role_info = RoleInfo(**role_dict)
    except AuthenticationFailure as e:
        current_app.logger.info(f"Authentication failed for user {username}: {str(e)}")
        raise

    access_token = access_token_from_user(user_info, role_info)

    if get_refresh_token:
        refresh_token = create_refresh_token(identity=user_info.id)
    else:
        refresh_token = None

    current_app.logger.info(f"Successful login for user {username}")
    return access_token, refresh_token
