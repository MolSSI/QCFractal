from __future__ import annotations

from typing import Dict, List
from urllib.parse import urlparse

from flask import request, g, current_app
from flask_jwt_extended import verify_jwt_in_request, get_jwt, get_jwt_identity
from werkzeug.exceptions import Forbidden, BadRequest

from qcfractal.auth.policyuniverse import Policy
from qcfractal.flask_app import storage_socket

_read_permissions: Dict[str, Dict[str, List[Dict[str, str]]]] = {}


def check_permissions(app, requested_action: str):
    """
    Check for access to the URL given permissions in the JWT token in the request headers

    1. If no security (enable_security is False), always allow
    2. If security is enabled, and if read allowed (allow_unauthenticated_read=True), use the default read permissions.
       Otherwise, check against the logged-in user permissions from the headers' JWT token
    """

    # uppercase by convention
    requested_action = requested_action.upper()

    # Read in config parameters
    security_enabled: bool = app.config["QCFRACTAL_CONFIG"].enable_security
    allow_unauthenticated_read: bool = app.config["QCFRACTAL_CONFIG"].allow_unauthenticated_read

    # if no auth required, always allowed
    if security_enabled is False:
        return

    # load read permissions from DB if not already loaded
    global _read_permissions
    if not _read_permissions:
        _read_permissions = storage_socket.roles.get("read")["permissions"]

    # Check for the JWT in the header
    if allow_unauthenticated_read:
        # don't raise exception if no JWT is found
        verify_jwt_in_request(optional=True)
    else:
        # read JWT token from request headers
        verify_jwt_in_request(optional=False)

    try:
        claims = get_jwt()
        permissions = claims.get("permissions", {})

        identity = get_jwt_identity()  # may be None

        # Pull the second part of the URL (ie, /v1/molecule -> molecule)
        # We will consistently ignore the version prefix
        resource = urlparse(request.url).path.split("/")[2]
        context = {"Principal": identity, "Action": requested_action, "Resource": resource}
        policy = Policy(permissions)
        if not policy.evaluate(context):
            # If that doesn't work, but we allow unauthenticated read, then try that
            if not allow_unauthenticated_read:
                raise Forbidden(f"User {identity} is not authorized to access '{resource}'")

            if not Policy(_read_permissions).evaluate(context):
                raise Forbidden(f"User {identity} is not authorized to access '{resource}'")

        # Store the user in the global app/request context
        g.user = identity

    except Forbidden:
        raise
    except Exception as e:
        current_app.logger.info("Error in evaluating JWT permissions: \n" + str(e))
        raise BadRequest("Error in evaluating JWT permissions")
