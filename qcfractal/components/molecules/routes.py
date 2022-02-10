from typing import List, Optional

from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, delete_helper
from qcfractal.app.routes import wrap_route
from qcportal.base_models import CommonBulkGetBody, CommonBulkDeleteBody
from qcportal.exceptions import LimitExceededError
from qcportal.molecules import Molecule, MoleculeQueryBody, MoleculeModifyBody
from qcportal.utils import calculate_limit


@main.route("/v1/molecule", methods=["GET"])
@main.route("/v1/molecule/<molecule_id>", methods=["GET"])
@wrap_route(None, CommonBulkGetBody)
def get_molecules_v1(molecule_id: Optional[int] = None, *, url_params: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_molecules
    if url_params.id is not None and len(url_params.id) > limit:
        raise LimitExceededError(f"Cannot get {len(url_params.id)} molecule records - limit is {limit}")

    return get_helper(molecule_id, url_params.id, None, None, url_params.missing_ok, storage_socket.molecules.get)


@main.route("/v1/molecule", methods=["DELETE"])
@main.route("/v1/molecule/<molecule_id>", methods=["DELETE"])
@wrap_route(None, CommonBulkDeleteBody)
def delete_molecules_v1(molecule_id: Optional[int] = None, *, url_params: CommonBulkDeleteBody):
    return delete_helper(molecule_id, url_params.id, storage_socket.molecules.delete)


@main.route("/v1/molecule/<molecule_id>", methods=["PATCH"])
@wrap_route(MoleculeModifyBody, None)
def modify_molecules_v1(molecule_id: int, *, body_data: MoleculeModifyBody):
    return storage_socket.molecules.modify(
        molecule_id=molecule_id,
        name=body_data.name,
        comment=body_data.comment,
        identifiers=body_data.identifiers,
        overwrite_identifiers=body_data.overwrite_identifiers,
    )


@main.route("/v1/molecule", methods=["POST"])
@wrap_route(List[Molecule], None)
def add_molecules_v1(body_data: List[Molecule]):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_molecules
    if len(body_data) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data)} molecule records - limit is {limit}")

    return storage_socket.molecules.add(body_data)


@main.route("/v1/molecule/query", methods=["POST"])
@wrap_route(MoleculeQueryBody, None)
def query_molecules_v1(body_data: MoleculeQueryBody):

    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_molecules

    return storage_socket.molecules.query(
        molecule_id=body_data.id,
        molecule_hash=body_data.molecule_hash,
        molecular_formula=body_data.molecular_formula,
        identifiers=body_data.identifiers,
        include=body_data.include,
        exclude=body_data.exclude,
        limit=calculate_limit(max_limit, body_data.limit),
        skip=body_data.skip,
    )
