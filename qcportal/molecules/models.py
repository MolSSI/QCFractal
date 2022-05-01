from typing import Optional, List, Dict

from qcelemental.models import Molecule
from qcelemental.models.molecule import Identifiers as MoleculeIdentifiers

from ..base_models import QueryProjModelBase, RestModelBase


class MoleculeQueryFilters(QueryProjModelBase):
    molecule_id: Optional[List[int]] = None
    molecule_hash: Optional[List[str]] = None
    molecular_formula: Optional[List[str]] = None
    identifiers: Optional[Dict[str, List[str]]] = None


class MoleculeModifyBody(RestModelBase):
    name: Optional[str] = None
    comment: Optional[str] = None
    identifiers: Optional[MoleculeIdentifiers] = None
    overwrite_identifiers: bool = False
