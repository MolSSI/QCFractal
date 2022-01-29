from typing import List, Optional

from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import wrap_route
from qcportal.base_models import CommonGetURLParameters, CommonDeleteURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.keywords import KeywordSet


@main.route("/v1/keyword", methods=["GET"])
@main.route("/v1/keyword/<keywords_id>", methods=["GET"])
@wrap_route(None, CommonGetURLParameters)
def get_keywords_v1(keywords_id: Optional[int] = None, *, url_params: CommonGetURLParameters):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_keywords
    if url_params.id is not None and len(url_params.id) > limit:
        raise LimitExceededError(f"Cannot get {len(url_params.id)} keyword records - limit is {limit}")

    return get_helper(keywords_id, url_params.id, None, None, url_params.missing_ok, storage_socket.keywords.get)


@main.route("/v1/keyword", methods=["POST"])
@wrap_route(List[KeywordSet], None)
def add_keywords_v1(body_data: List[KeywordSet]):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_keywords
    if len(body_data) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data)} keyword records - limit is {limit}")

    return storage_socket.keywords.add(body_data)


@main.route("/v1/keyword", methods=["DELETE"])
@main.route("/v1/keyword/<keywords_id>", methods=["DELETE"])
@wrap_route(None, CommonDeleteURLParameters)
def delete_keywords_v1(keywords_id: Optional[int] = None, *, url_params: CommonDeleteURLParameters):
    return delete_helper(keywords_id, url_params.id, storage_socket.keywords.delete)
