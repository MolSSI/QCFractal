from flask import request
from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import parse_bodymodel, SerializedResponse
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.interface.models import ObjectId
from qcfractal.interface.models.rest_models import KVStoreGETBody, ResponseGETMeta, KVStoreGETResponse
from qcfractal.portal.rest_models import SimpleGetParameters
from typing import Optional


@main.route("/kvstore", methods=["GET"])
@check_access
def query_kvstore():
    body = parse_bodymodel(KVStoreGETBody)
    ret = storage_socket.outputstore.get(body.data.id, missing_ok=True)

    # REST API currently expects a dict {id: KVStore dict}
    # But socket returns a list of KVStore dict
    ret_dict = {ObjectId(x["id"]): x for x in ret if x is not None}

    missing_id = [x for x, y in zip(body.data.id, ret) if y is None]

    # Transform to the old metadata format
    meta = ResponseGETMeta(n_found=len(ret), missing=missing_id, errors=[], error_description=False, success=True)
    response = KVStoreGETResponse(meta=meta, data=ret_dict)

    return SerializedResponse(response)


@main.route("/v1/outputstore", methods=["GET"])
@main.route("/v1/outputstore/<int:id>", methods=["GET"])
@wrap_route(None, SimpleGetParameters)
def get_outputstore_v1(id: Optional[int] = None):
    args = request.validated_args

    # If an id was specified in the url (molecule/1234) then use that
    # Otherwise, grab from the query parameters
    if id is not None:
        return storage_socket.outputstore.get([id], args.missing_ok)[0]
    else:
        return storage_socket.outputstore.get(args.id, args.missing_ok)
