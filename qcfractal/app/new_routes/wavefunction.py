from qcfractal.app import storage_socket
from qcfractal.app.new_routes.helpers import parse_bodymodel, SerializedResponse
from qcfractal.app.new_routes.main import main
from qcfractal.app.new_routes.permissions import check_access
from qcfractal.interface.models.rest_models import (
    WavefunctionStoreGETBody,
    ResponseGETMeta,
    WavefunctionStoreGETResponse,
)


@main.route("/wavefunctionstore", methods=["GET"])
@check_access
def get_wavefunction_v1():

    # NOTE - this only supports one wavefunction at a time
    body = parse_bodymodel(WavefunctionStoreGETBody)

    ret = storage_socket.wavefunction.get([body.data.id], include=body.meta.include, missing_ok=True)
    nfound = len(ret)
    if nfound > 0:
        meta_missing = []
        ret = ret[0]
    else:
        meta_missing = [body.data.id]

    meta = ResponseGETMeta(errors=[], success=True, error_description=False, missing=meta_missing, n_found=nfound)

    response = WavefunctionStoreGETResponse(meta=meta, data=ret)

    return SerializedResponse(response)
