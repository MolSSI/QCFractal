from __future__ import annotations

from flask import session, g, current_app, request
from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity
from flask_jwt_extended.exceptions import JWTExtendedException
from jwt.exceptions import ExpiredSignatureError, PyJWTError
from werkzeug.exceptions import InternalServerError

from qcfractal.flask_app.csrf import check_csrf
from qcfractal.flask_app import storage_socket, user_verifier
from qcportal.auth import API_TOKEN_PREFIX
from qcportal.exceptions import AuthorizationFailure, AuthenticationFailure


def _bearer_api_token(authorization: str) -> str | None:
    """
    Returns the API token from an Authorization header, or None if it is not one

    Recognizes ``Bearer <qcf_...>`` with a case-insensitive scheme (per RFC 9110). Anything else -
    a JWT, another scheme, a malformed header - returns None and is left for the JWT path.
    """

    parts = authorization.split()
    if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].startswith(API_TOKEN_PREFIX):
        return parts[1]
    return None


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

    # How the user was authenticated ("jwt", "api_token", "session", or None)
    auth_source = None

    # An API token (if any) authenticated for this request, so it can be attributed
    api_token_id = None

    try:
        authorization = request.headers.get("Authorization")
        if authorization is not None:
            # An Authorization header takes precedence and is never a fall back to the session
            # cookie - even an empty or malformed one, which is a 401 rather than a silent downgrade
            # to cookie or anonymous access. It is either one of our opaque API tokens (dispatched on
            # shape, before the JWT machinery, since verify_jwt_in_request would otherwise raise on a
            # non-JWT bearer value) or a JWT. Anything else is a 401.
            api_token = _bearer_api_token(authorization)

            if api_token is not None:
                # Look up the token (uncached, so revocation is immediate), then re-verify the user
                user_id, api_token_id = storage_socket.auth.verify_api_token(api_token)
                auth_source = "api_token"
            else:
                # verify_jwt_in_request(optional=True) returns None (rather than raising) when the
                # header is not a usable bearer JWT - a wrong scheme ("Basic ..."), a wrong case
                # ("bearer ..."), or a comma-separated list. Those must be a 401.
                if verify_jwt_in_request(optional=True) is not None:
                    user_id = get_jwt_identity()

                if user_id is None:
                    raise AuthenticationFailure("Authentication failure - unsupported or invalid Authorization header")

                # user_id is stored in the JWT as a string
                user_id = int(user_id)
                auth_source = "jwt"

            # Re-verify the user against the database rather than trusting the authorization
            # attributes copied into a JWT (or the user id behind a long-lived token). This ensures
            # that disabling an account or changing its role/groups takes effect within the cache
            # lifetime, rather than persisting for the credential's lifetime. The (short) cache
            # keeps this from hitting the database on every request.
            user_info = user_verifier.verify(user_id)
            username = user_info.username
            role = user_info.role
            groups = user_info.groups

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
    g.api_token_id = api_token_id
