from typing import List

from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters, CommonBulkGetBody
from qcportal.exceptions import LimitExceededError
from qcportal.keywords import KeywordSet


@main.route("/v1/keywords/<int:keywords_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters)
def get_keywords_v1(keywords_id: int):
    return storage_socket.keywords.get([keywords_id])[0]


@main.route("/v1/keywords/bulkGet", methods=["POST"])
@wrap_route(CommonBulkGetBody, None)
def bulk_get_keywords_v1(body_data: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_keywords
    if len(body_data.id) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.id)} keywords - limit is {limit}")

    return storage_socket.keywords.get(body_data.id, body_data.missing_ok)


@main.route("/v1/keywords/bulkCreate", methods=["POST"])
@wrap_route(List[KeywordSet], None)
def add_keywords_v1(body_data: List[KeywordSet]):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_keywords
    if len(body_data) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data)} keyword records - limit is {limit}")

    return storage_socket.keywords.add(body_data)


@main.route("/v1/keywords/<int:keywords_id>", methods=["DELETE"])
@wrap_route(None, None)
def delete_keywords_v1(keywords_id: int):
    return storage_socket.keywords.delete([keywords_id])


@main.route("/v1/keywords/bulkDelete", methods=["POST"])
@wrap_route(List[int], None)
def bulk_delete_keywords_v1(body_data: List[int]):
    return storage_socket.keywords.delete(body_data)
