from __future__ import annotations

from flask import session, g, current_app
from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity
from flask_jwt_extended.exceptions import JWTExtendedException
from jwt.exceptions import ExpiredSignatureError, PyJWTError
from werkzeug.exceptions import InternalServerError

from qcfractal.flask_app import storage_socket
from qcportal.exceptions import AuthorizationFailure, AuthenticationFailure
from qcportal.utils import time_based_cache


@time_based_cache(seconds=5, maxsize=256)
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

                # Re-verify the user against the database rather than trusting the
                # authorization attributes copied into the JWT. This ensures that
                # disabling an account or changing its role/groups takes effect within
                # the cache lifetime, rather than persisting until the token expires.
                # The (short) cache keeps this from hitting the database on every request.
                user_info = _cached_verify(user_id=user_id)
                username = user_info.username
                role = user_info.role
                groups = user_info.groups
    except (AuthorizationFailure, AuthenticationFailure):
        raise
    except ExpiredSignatureError:
        # Note: the qcportal client matches on "Token has expired" to trigger a refresh
        raise AuthenticationFailure("Authentication failure - JWT Token has expired")
    except (PyJWTError, JWTExtendedException) as e:
        # Malformed, tampered, or otherwise invalid tokens are a client error, not a server error.
        # Report only the exception type - the details are not useful to a legitimate client
        raise AuthenticationFailure(f"Authentication failure - invalid token ({type(e).__name__})")
    except Exception:
        # Do not send exception details to the client. The full traceback (including this
        # exception as the context) is captured by the internal error handler
        current_app.logger.exception("Failed to verify user info")
        raise InternalServerError("Failed to verify user info")

    # Store the user in the global app/request context
    g.user_id = user_id
    g.username = username
    g.role = role
    g.groups = groups
