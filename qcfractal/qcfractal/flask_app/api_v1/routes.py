from flask import jsonify, current_app, g

from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import no_permission_required


@api_v1.route("/ping", methods=["GET"])
@no_permission_required()
def ping():

    user_id = g.user_id if "user_id" in g else None

    allow_unauth_read = current_app.config["QCFRACTAL_CONFIG"].allow_unauthenticated_read
    is_authorized = True if (user_id is not None or allow_unauth_read) else False
    ret = {"success": True, "authorized": is_authorized}

    if user_id is not None:
        ret["user_info"] = {
            "user_id": user_id,
            "username": g.username,
            "role": g.role,
            "groups": g.groups,
        }

    return jsonify(ret)


@api_v1.route("/all_routes", methods=["GET"])
@no_permission_required()
def get_all_routes_v1():
    """
    Returns a dictionary of all endpoints and their required permissions.

    The return dictionary has keys as the endpoint name, and values representing
    the required permissions (http method, target, resource, action)
    """
    permissions = {}

    for rule in current_app.url_map.iter_rules():
        endpoint = rule.endpoint
        if endpoint == "static":
            continue

        view_func = current_app.view_functions[endpoint]
        if not hasattr(view_func, "_has_permission_check"):
            continue

        if hasattr(view_func, "_permissions_required"):
            resource, action = view_func._permissions_required
        else:
            resource, action = "none", "none"

        # The required permissions
        # http method, resource, action (read/write/modify/delete/etc)
        for method in rule.methods:
            if method.lower() in {"options", "head"}:
                continue

            permissions[str(rule)] = {
                "endpoint": endpoint,
                "method": method.lower(),
                "resource": resource,
                "action": action,
            }

    return jsonify(permissions)
