from __future__ import annotations

from flask import session, g
from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity, get_jwt
from werkzeug.exceptions import InternalServerError

from qcfractal.flask_app import storage_socket
from qcportal.exceptions import AuthorizationFailure, AuthenticationFailure
from qcportal.utils import time_based_cache


@time_based_cache(seconds=3, maxsize=256)
def _cached_verify(user_id: int):
    return storage_socket.auth.verify(user_id=user_id)


def load_logged_in_user():
    ##############################################
    # Load any user information from a JWT or
    # session info in the database (retrieved
    # via the typical flask session mechanism)
    ##############################################
    user_id = None
    username = None
    role = None
    groups = []

    try:
        # Is the info stored in the session?
        if session and "user_id" in session:
            user_id = int(session["user_id"])  # may be a string? Just to make sure

            user_info = _cached_verify(user_id=user_id)
            username = user_info.username
            role = user_info.role
            groups = user_info.groups
        elif verify_jwt_in_request(optional=True) is not None:
            user_id = get_jwt_identity()

            if user_id is not None:
                # user_id is stored in the JWT as a string
                user_id = int(user_id)

                # Get from JWT in header
                # TODO - some of these may not be None in the future
                claims = get_jwt()
                username = claims.get("username", None)
                role = claims.get("role", None)
                groups = claims.get("groups", [])
    except (AuthorizationFailure, AuthenticationFailure):
        raise
    except Exception as e:
        raise InternalServerError(f"Failed to verify user info: {e}")

    # Store the user in the global app/request context
    g.user_id = user_id
    g.username = username
    g.role = role
    g.groups = groups
