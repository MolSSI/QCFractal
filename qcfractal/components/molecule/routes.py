from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import (
    parse_bodymodel,
    convert_get_response_metadata,
    SerializedResponse,
    convert_post_response_metadata,
)
from qcfractal.app.routes import check_access
from qcfractal.interface.models.rest_models import (
    MoleculeGETBody,
    MoleculeGETResponse,
    MoleculePOSTBody,
    MoleculePOSTResponse,
)


@main.route("/molecule", methods=["GET"])
@check_access
def query_molecule_v1():
    body = parse_bodymodel(MoleculeGETBody)
    meta, molecules = storage_socket.molecule.query(**body.data.dict(), **body.meta.dict())

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])
    response = MoleculeGETResponse(meta=meta_old, data=molecules)
    return SerializedResponse(response)


@main.route("/molecule", methods=["POST"])
@check_access
def add_molecule_v1():
    body = parse_bodymodel(MoleculePOSTBody)
    meta, ret = storage_socket.molecule.add(body.data)

    # Convert new metadata format to old
    duplicate_ids = [ret[i] for i in meta.existing_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)
    response = MoleculePOSTResponse(meta=meta_old, data=ret)
    return SerializedResponse(response)
