from typing import Optional

from flask import g

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.rest_models import SimpleGetParameters
from qcfractal.app.helpers import get_helper


@main.route("/v1/output", methods=["GET"])
@main.route("/v1/output/<int:id>", methods=["GET"])
@wrap_route(None, SimpleGetParameters)
@check_access
def get_output_v1(id: Optional[int] = None):
    return get_helper(id, g.validated_args.id, g.validated_args.missing_ok, storage_socket.outputstore.get)
