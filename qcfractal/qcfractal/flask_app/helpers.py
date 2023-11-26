from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional
from urllib.parse import urlparse

from flask import request, g, current_app
from flask_jwt_extended import (
    verify_jwt_in_request,
    get_jwt,
    get_jwt_identity,
    create_access_token,
    create_refresh_token,
)
from werkzeug.exceptions import BadRequest, Forbidden

from qcfractal.flask_app import storage_socket
from qcportal.auth import UserInfo, RoleInfo
from qcportal.exceptions import AuthorizationFailure, AuthenticationFailure

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


def assert_role_permissions(requested_action: str):
    """
    Check for access to the URL given permissions in the JWT token in the request headers

    1. If no security (enable_security is False), always allow
    2. If security is enabled, and if read allowed (allow_unauthenticated_read=True), use the default read permissions.
       Otherwise, check against the logged-in user permissions from the headers' JWT token
    """

    # Check for the JWT in the header
    # don't raise exception if no JWT is found
    verify_jwt_in_request(optional=True)

    try:
        # TODO - some of these may not be None in the future
        claims = get_jwt()
        user_id = get_jwt_identity()  # may be None
        username = claims.get("username", None)
        policies = claims.get("permissions", {})
        role = claims.get("role", None)
        groups = claims.get("groups", None)

        subject = {"user_id": user_id, "username": username}

        # Pull the first part of the URL (ie, /api/v1/molecule/a/b/c -> /api/v1/molecule)
        resource = {"type": get_url_major_component(request.url)}

        storage_socket.auth.assert_authorized(
            resource=resource, action=requested_action, subject=subject, context={}, policies=policies
        )

        # Store the user in the global app/request context
        g.user_id = user_id
        g.username = username
        g.role = role
        g.groups = groups

    except AuthorizationFailure as e:
        raise Forbidden(str(e))
    except Exception as e:
        current_app.logger.info("Error in evaluating JWT permissions: \n" + str(e))
        raise BadRequest("Error in evaluating JWT permissions")


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
        user_info, role_info = storage_socket.auth.authenticate(username, password)

        # Used for logging (in the after_request_func)
        g.user_id = user_info.id

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
