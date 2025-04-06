from flask import jsonify, current_app, session
from flask_jwt_extended import (
    verify_jwt_in_request,
    get_jwt,
    get_jwt_identity,
)

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcportal.utils import time_based_cache


# TODO - much of this is duplicated from flask_app/helpers.py


@time_based_cache(seconds=5, maxsize=256)
def _cached_verify(user_id: int):
    return storage_socket.auth.verify(user_id=user_id)


@api_v1.route("/ping", methods=["GET"])
def ping():

    username = None
    role = None
    groups = []

    # Is the info stored in the session?
    if session and "user_id" in session:
        user_id = int(session["user_id"])  # may be a string? Just to make sure

        user_info, role_info = _cached_verify(user_id=user_id)
        username = user_info.username
        role = role_info.rolename
        groups = user_info.groups
    else:
        # Check for the JWT in the header
        # don't raise exception if no JWT is found
        verify_jwt_in_request(optional=True)
        user_id = get_jwt_identity()  # may be None

        if user_id is not None:
            # user_id is stored in the JWT as a string
            user_id = int(user_id)

            # Get from JWT in header
            # TODO - some of these may not be None in the future
            claims = get_jwt()
            username = claims.get("username", None)
            role = claims.get("role", None)
            groups = claims.get("groups", [])

    allow_unauth_read = current_app.config["QCFRACTAL_CONFIG"].allow_unauthenticated_read
    is_authorized = True if (user_id is not None or allow_unauth_read) else False
    ret = {"success": True, "authorized": is_authorized}

    if user_id is not None:
        ret["user_info"] = {
            "user_id": user_id,
            "username": username,
            "role": role,
            "groups": groups,
        }

    return jsonify(ret)
