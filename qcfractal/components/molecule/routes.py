from flask import request
from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import (
    parse_bodymodel,
    convert_get_response_metadata,
    SerializedResponse,
    convert_post_response_metadata,
)
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.interface.models import InsertMetadata, ObjectId
from qcfractal.interface.models.rest_models import (
    MoleculePOSTBody,
    MoleculePOSTResponse,
)
from qcfractal.interface.models import Molecule
from qcfractal.portal.rest_models import GetParameters
from typing import List, Optional, Union, Tuple, Any, Dict


def str2bool(s):
    if isinstance(s, list):
        s = s[0]
    return s.lower() in ("1", "true", "t", "yes", "y")


@main.route("/molecule", methods=["GET"])
@check_access
def query_molecule():

    body = request.validated_data
    meta, molecules = storage_socket.molecules.query(**body.data.dict(), **body.meta.dict())

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])
    return {"meta": meta_old, "data": molecules}


@main.route("/v1/molecule", methods=["GET"])
@main.route("/v1/molecule/<int:id>", methods=["GET"])
@wrap_route(None, GetParameters)
def get_molecule_v1(id: Optional[int] = None):
    args = request.validated_args

    # If an id was specified in the url (molecule/1234) then use that
    # Otherwise, grab from the query parameters
    if id is not None:
        return storage_socket.molecules.get(
            [id], include=args.include, exclude=args.exclude, missing_ok=args.missing_ok
        )[0]
    else:
        ids = args.id
        if not ids:
            return []

        return storage_socket.molecules.get(ids, include=args.include, exclude=args.exclude, missing_ok=args.missing_ok)


@main.route("/v1/molecule", methods=["POST"])
@wrap_route(List[Molecule], None)
def add_molecule_v1():
    return storage_socket.molecules.add(request.validated_data)


@main.route("/molecule", methods=["POST"])
@check_access
def add_molecule():
    body = parse_bodymodel(MoleculePOSTBody)
    meta, ret = storage_socket.molecules.add(body.data)

    # Convert new metadata format to old
    duplicate_ids = [ret[i] for i in meta.existing_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)
    response = MoleculePOSTResponse(meta=meta_old, data=ret)
    return SerializedResponse(response)
