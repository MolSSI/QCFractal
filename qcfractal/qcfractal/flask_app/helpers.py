from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional
from urllib.parse import urlparse

from flask import request, g, current_app, session
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
)

from qcfractal import __version__ as qcfractal_version
from qcfractal.flask_app import storage_socket
from qcportal.auth import UserInfo
from qcportal.exceptions import AuthenticationFailure

if TYPE_CHECKING:
    from typing import Set

_all_endpoints: Set[str] = set()


def get_all_endpoints() -> Set[str]:
    """
    Get a list of all endpoints on the server

    These endpoints are the first three parts of the resource (ie,
    /api/v1/molecules, not /api/v1/molecules/bulkGet)
    """
    global _all_endpoints

    if not _all_endpoints:
        for url in current_app.url_map.iter_rules():
            endpoint = get_url_major_component(url.rule)
            # Don't add "static"
            if not endpoint.startswith("/static"):
                _all_endpoints.add(endpoint)

    return _all_endpoints


def get_url_major_component(url: str):
    """
    Obtains the major parts of a URL's components

    For example, /api/v1/molecule/a/b/c -> /api/v1/molecule
    """

    components = urlparse(url).path.split("/")
    resource = "/".join(components[:4])

    # Force leading slash, but only one
    return "/" + resource.lstrip("/")


def login_user() -> UserInfo:
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
        user_info = storage_socket.users.authenticate(username, password)

        # Used for logging (in the after_request_func)
        g.user_id = user_info.id

        return user_info

    except AuthenticationFailure as e:
        current_app.logger.info(f"Authentication failed for user {username}: {str(e)}")
        raise


def login_user_session() -> UserInfo:
    # Raises exception on invalid username, password, etc
    # Submitted user/password are stored in the flask request object
    session.clear()
    user_info = login_user()
    session["user_id"] = str(user_info.id)

    return user_info


def logout_user_session():
    session.clear()


def access_token_from_user(user_info: UserInfo):
    """
    Creates a JWT access token from user/role information
    """
    return create_access_token(
        identity=str(user_info.id),
        additional_claims={
            "username": user_info.username,
            "role": user_info.role,
            "groups": user_info.groups,
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

    # Will raise exceptions on invalid username/password
    user_info = login_user()
    access_token = access_token_from_user(user_info)

    if get_refresh_token:
        refresh_token = create_refresh_token(identity=str(user_info.id))
    else:
        refresh_token = None

    current_app.logger.info(f"Successful login for user {user_info.username}")
    return access_token, refresh_token


def get_public_server_information():
    qcf_cfg = current_app.config["QCFRACTAL_CONFIG"]

    # TODO - remove version limits after a while. They are there to support older clients
    public_info = {
        "name": qcf_cfg.name,
        "manager_heartbeat_frequency": qcf_cfg.heartbeat_frequency,
        "manager_heartbeat_frequency_jitter": qcf_cfg.heartbeat_frequency_jitter,
        "manager_heartbeat_max_missed": qcf_cfg.heartbeat_max_missed,
        "version": qcfractal_version,
        "api_limits": qcf_cfg.api_limits.model_dump(mode="json"),
        "client_version_lower_limit": "0.50",
        "client_version_upper_limit": "1.00",
        "manager_version_lower_limit": "0.50",
        "manager_version_upper_limit": "1.00",
        "motd": storage_socket.serverinfo.get_motd(),
    }

    return public_info
