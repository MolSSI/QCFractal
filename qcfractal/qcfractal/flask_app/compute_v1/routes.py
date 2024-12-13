from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcfractal.flask_app.compute_v1.blueprint import compute_v1
from qcfractal.flask_app.helpers import get_public_server_information


@compute_v1.route("/information", methods=["GET"])
@wrap_route("READ")
def get_information():
    return get_public_server_information()
