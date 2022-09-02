from flask import jsonify

from qcfractal.api_v1.blueprint import api_v1


@api_v1.route("/ping", methods=["GET"])
def ping():
    return jsonify(success=True)
