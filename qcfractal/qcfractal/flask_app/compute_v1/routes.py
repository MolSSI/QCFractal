from qcfractal.flask_app.compute_v1.blueprint import compute_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcfractal.flask_app.helpers import get_public_server_information


@compute_v1.route("/information", methods=["GET"])
@check_permissions("information", "read")
@serialization()
def get_information():
    return get_public_server_information()
