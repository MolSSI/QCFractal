from typing import List, Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.common_rest import CommonGetURLParameters, CommonDeleteURLParameters
from qcfractal.portal.components.keywords import KeywordSet


@main.route("/v1/keyword", methods=["GET"])
@main.route("/v1/keyword/<int:id>", methods=["GET"])
@wrap_route(None, CommonGetURLParameters)
@check_access
def get_keywords_v1(id: Optional[int] = None, *, url_params: CommonGetURLParameters):
    return get_helper(id, url_params.id, url_params.missing_ok, storage_socket.keywords.get)


@main.route("/v1/keyword", methods=["POST"])
@wrap_route(List[KeywordSet], None)
@check_access
def add_keywords_v1(body_data: List[KeywordSet]):
    return storage_socket.keywords.add(body_data)


@main.route("/v1/keyword", methods=["DELETE"])
@main.route("/v1/keyword/<int:id>", methods=["DELETE"])
@wrap_route(None, CommonDeleteURLParameters)
@check_access
def delete_keywords_v1(id: Optional[int] = None, *, url_params: CommonDeleteURLParameters):
    return delete_helper(id, url_params.id, storage_socket.keywords.delete)
