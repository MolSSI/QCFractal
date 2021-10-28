from typing import Optional

from flask import g

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.common_rest import CommonGetURLParameters


@main.route("/v1/wavefunction", methods=["GET"])
@main.route("/v1/wavefunction/<int:id>", methods=["GET"])
@wrap_route(None, CommonGetURLParameters)
@check_access
def get_wavefunction_v1(id: Optional[int] = None):
    return get_helper(id, g.validated_args.id, g.validated_args.missing_ok, storage_socket.wavefunctions.get)
