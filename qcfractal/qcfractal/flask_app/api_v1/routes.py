from flask import jsonify, g

from qcfractal.flask_app.api_v1.blueprint import api_v1


@api_v1.route("/ping", methods=["GET"])
def ping():
    return jsonify(
        success=True,
        user_id=g.user.id if "user_id" in g else None,
        user_name=g.user.name if "user_name" in g else None,
    )
