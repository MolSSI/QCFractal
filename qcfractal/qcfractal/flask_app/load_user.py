from __future__ import annotations

from flask import session, g, current_app, request
from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity
from flask_jwt_extended.exceptions import JWTExtendedException
from jwt.exceptions import ExpiredSignatureError, PyJWTError
from werkzeug.exceptions import InternalServerError

from qcfractal.flask_app.csrf import check_csrf
from qcfractal.flask_app import user_verifier
from qcportal.exceptions import AuthorizationFailure, AuthenticationFailure


def load_logged_in_user():
    """
    Loads information about the current user into the flask request context (flask.g)

    The user may be authenticated by a bearer token (JWT) in the Authorization header, or by the
    browser session cookie. An explicit Authorization header always takes precedence - a request
    that carries one is never authenticated by the cookie, so the two can never disagree.

    Requests authenticated by the session cookie are subject to CSRF checks (see check_csrf).
    """
    user_id = None
    username = None
    role = None
    groups = []

    # How the user was authenticated ("jwt", "session", or None)
    auth_source = None

    try:
        if request.headers.get("Authorization"):
            # An Authorization header takes precedence and is never a fall back to the session
            # cookie. verify_jwt_in_request(optional=True) returns None (rather than raising) when
            # the header is not a usable bearer JWT - a wrong scheme ("Basic ..."), a wrong case
            # ("bearer ..."), or a comma-separated list. Those must be a 401, not a silent
            # downgrade to anonymous access
            if verify_jwt_in_request(optional=True) is not None:
                user_id = get_jwt_identity()

            if user_id is None:
                raise AuthenticationFailure("Authentication failure - unsupported or invalid Authorization header")

            # user_id is stored in the JWT as a string
            user_id = int(user_id)

            # Re-verify the user against the database rather than trusting the
            # authorization attributes copied into the JWT. This ensures that
            # disabling an account or changing its role/groups takes effect within
            # the cache lifetime, rather than persisting until the token expires.
            # The (short) cache keeps this from hitting the database on every request.
            user_info = user_verifier.verify(user_id)
            username = user_info.username
            role = user_info.role
            groups = user_info.groups
            auth_source = "jwt"

        elif session and "user_id" in session:
            # Browser session (the session data was validated against the database when loaded)
            user_id = int(session["user_id"])  # may be a string? Just to make sure

            user_info = user_verifier.verify(user_id)
            username = user_info.username
            role = user_info.role
            groups = user_info.groups
            auth_source = "session"

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

    # Cookie-authenticated requests that may change state must pass CSRF checks
    if auth_source == "session":
        check_csrf()

    # Store the user in the global app/request context
    g.user_id = user_id
    g.username = username
    g.role = role
    g.groups = groups
    g.auth_source = auth_source
