from typing import List, Optional

from flask import g

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.interface.models import Molecule
from qcfractal.portal.rest_models import SimpleGetParameters, DeleteParameters, MoleculeQueryBody, MoleculeModifyBody


@main.route("/v1/molecule", methods=["GET"])
@main.route("/v1/molecule/<int:id>", methods=["GET"])
@wrap_route(None, SimpleGetParameters)
@check_access
def get_molecules_v1(id: Optional[int] = None):
    return get_helper(id, g.validated_args.id, g.validated_args.missing_ok, storage_socket.molecules.get)


@main.route("/v1/molecule", methods=["DELETE"])
@main.route("/v1/molecule/<int:id>", methods=["DELETE"])
@wrap_route(None, DeleteParameters)
@check_access
def delete_molecules_v1(id: Optional[int] = None):
    return delete_helper(id, g.validated_args.id, storage_socket.molecules.delete)


@main.route("/v1/molecule/<int:id>", methods=["PATCH"])
@wrap_route(MoleculeModifyBody, None)
@check_access
def modify_molecules_v1(id: Optional[int] = None):
    return storage_socket.molecules.modify(
        id,
        name=g.validated_data.name,
        comment=g.validated_data.comment,
        identifiers=g.validated_data.identifiers,
        overwrite_identifiers=g.validated_data.overwrite_identifiers,
    )


@main.route("/v1/molecule", methods=["POST"])
@wrap_route(List[Molecule], None)
@check_access
def add_molecules_v1():
    return storage_socket.molecules.add(g.validated_data)


@main.route("/v1/molecule/query", methods=["POST"])
@wrap_route(MoleculeQueryBody, None)
@check_access
def query_molecules_v1():
    return storage_socket.molecules.query(
        id=g.validated_data.id,
        molecule_hash=g.validated_data.molecule_hash,
        molecular_formula=g.validated_data.molecular_formula,
        identifiers=g.validated_data.identifiers,
        include=g.validated_data.include,
        exclude=g.validated_data.exclude,
        limit=g.validated_data.limit,
        skip=g.validated_data.skip,
    )
