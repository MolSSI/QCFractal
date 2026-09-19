"""
Cross-site request forgery (CSRF) protection for requests authenticated by the browser session cookie

Requests authenticated with a bearer token (JWT) are not subject to CSRF - a hostile site cannot
attach an Authorization header without a CORS preflight. Requests authenticated by the session cookie
are, because browsers attach cookies to cross-site requests automatically.
"""

from __future__ import annotations

from urllib.parse import urlparse

from flask import request, current_app

from qcportal.exceptions import AuthorizationFailure

# Header that must accompany every state-changing request authenticated by the session cookie.
# Its value is not checked, only its presence.
CSRF_HEADER = "X-Requested-With"

# Methods that must not change state, and are therefore not checked
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _normalize_origin(origin: str) -> str:
    return origin.strip().rstrip("/").lower()


def is_trusted_origin(origin: str) -> bool:
    """
    Determines if a value of the Origin header is trusted to make cookie-authenticated requests

    Trusted origins are this server itself (same host and port; the scheme is not compared since
    it may differ behind a TLS-terminating proxy) and the exact origins listed in the CORS
    configuration (if CORS is enabled). A "null" origin is never trusted.
    """

    if origin == "null":
        return False

    parsed = urlparse(origin)
    if not parsed.scheme or not parsed.netloc:
        return False

    if parsed.netloc.lower() == request.host.lower():
        return True

    cors_config = current_app.config["QCFRACTAL_CONFIG"].cors
    if cors_config.enabled:
        trusted = {_normalize_origin(o) for o in cors_config.origins}
        return _normalize_origin(origin) in trusted

    return False


def check_csrf() -> None:
    """
    Verifies that a state-changing, cookie-authenticated request did not originate from another site

    Two independent checks are made:

    1. The request must carry the ``X-Requested-With`` header. Browsers only attach a custom header
       to a cross-origin request after a successful CORS preflight, so a hostile site cannot add it
       unless the server's CORS configuration explicitly trusts that site. This also covers multipart
       form posts, which browsers otherwise send without a preflight.
    2. If the browser sent an ``Origin`` header, it must be a trusted origin (see is_trusted_origin).

    Requests using safe methods (GET, HEAD, OPTIONS) are not checked.

    Raises AuthorizationFailure (403) if the checks fail.
    """

    if request.method in _SAFE_METHODS:
        return

    if not request.headers.get(CSRF_HEADER):
        raise AuthorizationFailure(f"Cross-site request check failed - missing {CSRF_HEADER} header")

    origin = request.headers.get("Origin")
    if origin is not None and not is_trusted_origin(origin):
        current_app.logger.warning(f"Rejected cookie-authenticated {request.method} request from origin {origin!r}")
        raise AuthorizationFailure("Cross-site request check failed - untrusted origin")
