from qcfractal.app import storage_socket
from qcfractal.app.new_routes.helpers import parse_bodymodel, SerializedResponse, convert_post_response_metadata
from qcfractal.app.new_routes.main import main
from qcfractal.app.new_routes.permissions import check_access
from qcfractal.interface.models.rest_models import (
    KeywordGETBody,
    ResponseGETMeta,
    KeywordGETResponse,
    KeywordPOSTBody,
    KeywordPOSTResponse,
)


@main.route("/keyword", methods=["GET"])
@check_access
def query_keywords_v1():
    body = parse_bodymodel(KeywordGETBody)

    ret = storage_socket.keywords.get(body.data.id, missing_ok=True)
    missing_id = [x for x, y in zip(body.data.id, ret) if y is None]
    meta = ResponseGETMeta(n_found=len(ret), missing=missing_id, errors=[], error_description=False, success=True)
    response = KeywordGETResponse(meta=meta, data=ret)

    return SerializedResponse(response)


@main.route("/keyword", methods=["POST"])
@check_access
def add_keywords_v1():

    body = parse_bodymodel(KeywordPOSTBody)
    meta, ret = storage_socket.keywords.add(body.data)

    # Convert new metadata format to old
    duplicate_ids = [ret[i] for i in meta.existing_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)

    response = KeywordPOSTResponse(meta=meta_old, data=ret)
    return SerializedResponse(response)
