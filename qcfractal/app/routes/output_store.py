from qcfractal.app import storage_socket
from qcfractal.app.routes.helpers import parse_bodymodel, SerializedResponse
from qcfractal.app.routes.main import main
from qcfractal.app.routes.permissions import check_access
from qcfractal.interface.models import ObjectId
from qcfractal.interface.models.rest_models import KVStoreGETBody, ResponseGETMeta, KVStoreGETResponse


@main.route("/kvstore", methods=["GET"])
@check_access
def query_kvstore_v1():
    body = parse_bodymodel(KVStoreGETBody)
    ret = storage_socket.output_store.get(body.data.id, missing_ok=True)

    # REST API currently expects a dict {id: KVStore dict}
    # But socket returns a list of KVStore dict
    ret_dict = {ObjectId(x["id"]): x for x in ret if x is not None}

    missing_id = [x for x, y in zip(body.data.id, ret) if y is None]

    # Transform to the old metadata format
    meta = ResponseGETMeta(n_found=len(ret), missing=missing_id, errors=[], error_description=False, success=True)
    response = KVStoreGETResponse(meta=meta, data=ret_dict)

    return SerializedResponse(response)
