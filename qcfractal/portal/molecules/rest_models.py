from typing import Optional, List, Dict

from qcfractal.portal.common_rest import QueryProjParametersBase, RestModelBase
from .models import MoleculeIdentifiers


class MoleculeQueryBody(QueryProjParametersBase):
    id: Optional[List[int]] = None
    molecule_hash: Optional[List[str]] = None
    molecular_formula: Optional[List[str]] = None
    identifiers: Optional[Dict[str, List[str]]] = None


class MoleculeModifyBody(RestModelBase):
    name: Optional[str] = None
    comment: Optional[str] = None
    identifiers: Optional[MoleculeIdentifiers] = None
    overwrite_identifiers: bool = False
