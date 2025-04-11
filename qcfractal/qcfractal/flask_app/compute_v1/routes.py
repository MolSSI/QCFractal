from qcfractal.flask_app.compute_v1.blueprint import compute_v1
from qcfractal.flask_app.helpers import get_public_server_information
from qcfractal.flask_app.wrap_route import wrap_global_route


@compute_v1.route("/information", methods=["GET"])
@wrap_global_route("information", "read")
def get_information():
    return get_public_server_information()
