from __future__ import annotations

from typing import TYPE_CHECKING, List
from urllib.parse import urlparse

from flask import request, g, current_app
from flask_jwt_extended import verify_jwt_in_request, get_jwt, get_jwt_identity
from werkzeug.exceptions import BadRequest, Forbidden

from qcfractal.flask_app import storage_socket
from qcportal.exceptions import AuthorizationFailure

if TYPE_CHECKING:
    from typing import List, Optional, Tuple, Set
    from qcportal.base_models import ProjURLParameters

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
