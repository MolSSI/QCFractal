from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List
from urllib.parse import urlparse

from flask import request, g, current_app
from flask_jwt_extended import verify_jwt_in_request, get_jwt, get_jwt_identity
from flask_jwt_extended.exceptions import NoAuthorizationError
from werkzeug.exceptions import Forbidden, BadRequest

from qcfractal.auth_v1.policyuniverse import Policy
from qcfractal.flask_app import storage_socket

_read_permissions: Dict[str, Dict[str, List[Dict[str, str]]]] = {}


if TYPE_CHECKING:
    from typing import List, Optional, Tuple
    from qcportal.base_models import ProjURLParameters


def get_url_major_component(url: str):
    """
    Obtains the major parts of a URL's components

    For example, /api/v1/molecule/a/b/c -> /api/v1/molecule
    """

    components = urlparse(request.url).path.split("/")
    resource = "/".join(components[:4])

    # Force leading slash, but only one
    return "/" + resource.lstrip("/")


def prefix_projection(proj_params: ProjURLParameters, prefix: str) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    """
    Prefixes includes and excludes with a string

    This is used for mapping a set of includes/excludes to a relationship of an ORM. For example,
    you may have an endpoint for molecules of a computation (/record/1/molecule) which contains
    include/exclude in its url parameters. This function is used to map those includes/excludes to
    the "molecule" relationship of the record.
    """

    ch_includes = proj_params.include
    ch_excludes = proj_params.exclude

    base = prefix.strip(".")
    p = base + "."

    if ch_includes is None:
        # If nothing is specified, include the defaults of the child
        ch_includes = [base]
    else:
        # Otherwise, prefix all entries with whatever was specified
        ch_includes = [p + x for x in ch_includes]

    if ch_excludes:
        ch_excludes = [p + x for x in ch_excludes]

    return ch_includes, ch_excludes


def check_role_permissions(app, requested_action: str):
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
        try:
            # read JWT token from request headers
            verify_jwt_in_request(optional=False)
        except NoAuthorizationError as e:
            raise Forbidden(f"Missing authorization token. Server requires login. Are you logged in?")

    try:
        claims = get_jwt()
        permissions = claims.get("permissions", {})

        identity = get_jwt_identity()  # may be None

        # Pull the first part of the URL (ie, /api/v1/molecule/a/b/c -> /api/v1/molecule)
        resource = get_url_major_component(request.url)

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
