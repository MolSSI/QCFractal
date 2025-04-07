from flask import jsonify, current_app, g

from qcfractal.flask_app.api_v1.blueprint import api_v1


@api_v1.route("/ping", methods=["GET"])
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
