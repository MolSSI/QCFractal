from typing import List, Tuple

from flask import current_app

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.wrap_route import wrap_global_route
from qcportal.base_models import CommonBulkGetBody, ProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.molecules import Molecule, MoleculeQueryFilters, MoleculeModifyBody, MoleculeUploadOptions
from qcportal.utils import calculate_limit


@api_v1.route("/molecules/<int:molecule_id>", methods=["GET"])
@wrap_global_route("records", "read")
def get_molecules_v1(molecule_id: int, url_params: ProjURLParameters):
    return storage_socket.molecules.get([molecule_id], url_params.include, url_params.exclude)[0]


@api_v1.route("/molecules/bulkGet", methods=["POST"])
@wrap_global_route("records", "read")
def bulk_get_molecules_v1(body_data: CommonBulkGetBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_molecules
    if len(body_data.ids) > limit:
        raise LimitExceededError(f"Cannot get {len(body_data.ids)} molecule records - limit is {limit}")

    return storage_socket.molecules.get(
        body_data.ids, body_data.include, body_data.exclude, missing_ok=body_data.missing_ok
    )


@api_v1.route("/molecules/<int:molecule_id>", methods=["DELETE"])
@wrap_global_route("records", "delete")
def delete_molecules_v1(molecule_id: int):
    return storage_socket.molecules.delete([molecule_id])


@api_v1.route("/molecules/bulkDelete", methods=["POST"])
@wrap_global_route("records", "delete")
def bulk_delete_molecules_v1(body_data: List[int]):
    return storage_socket.molecules.delete(body_data)


@api_v1.route("/molecules/<int:molecule_id>", methods=["PATCH"])
@wrap_global_route("records", "modify")
def modify_molecules_v1(molecule_id: int, body_data: MoleculeModifyBody):
    return storage_socket.molecules.modify(
        molecule_id=molecule_id,
        name=body_data.name,
        comment=body_data.comment,
        identifiers=body_data.identifiers,
        overwrite_identifiers=body_data.overwrite_identifiers,
    )


@api_v1.route("/molecules/bulkCreate", methods=["POST"])
@wrap_global_route("records", "add")
def add_molecules_v1(body_data: List[Molecule]):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_molecules
    if len(body_data) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data)} molecule records - limit is {limit}")

    return storage_socket.molecules.add(body_data)


allowed_extensions = [
    "xyz",
    "json",
    "zip",
    "tar.gz",
    "tgz",
    "tar.bz2",
    "tbz2",
    "tar.xz",
    "txz",
]


@api_v1.route("/molecules/fromFiles", methods=["POST"])
@wrap_global_route("records", "add", allowed_file_extensions=allowed_extensions)
def add_molecules_from_files_v1(body_data: MoleculeUploadOptions, files: List[Tuple[str, str]]):
    return storage_socket.molecules.add_from_files(files, body_data)


@api_v1.route("/molecules/query", methods=["POST"])
@wrap_global_route("records", "read")
def query_molecules_v1(body_data: MoleculeQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_molecules
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.molecules.query(body_data)
