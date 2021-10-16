from typing import List, Optional

from flask import g

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.components.keywords import KeywordSet
from qcfractal.portal.common_rest import SimpleGetParameters, DeleteParameters


@main.route("/v1/keyword", methods=["GET"])
@main.route("/v1/keyword/<int:id>", methods=["GET"])
@wrap_route(None, SimpleGetParameters)
@check_access
def get_keywords_v1(id: Optional[int] = None):
    return get_helper(id, g.validated_args.id, g.validated_args.missing_ok, storage_socket.keywords.get)


@main.route("/v1/keyword", methods=["POST"])
@wrap_route(List[KeywordSet], None)
@check_access
def add_keywords_v1():
    return storage_socket.keywords.add(g.validated_data)


@main.route("/v1/keyword", methods=["DELETE"])
@main.route("/v1/keyword/<int:id>", methods=["DELETE"])
@wrap_route(None, DeleteParameters)
@check_access
def delete_keywords_v1(id: Optional[int] = None):
    return delete_helper(id, g.validated_args.id, storage_socket.keywords.delete)
