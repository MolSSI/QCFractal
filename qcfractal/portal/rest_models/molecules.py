from typing import Optional, List, Dict

from .common import QueryParametersBase, BaseModel
from qcfractal.interface.models import MoleculeIdentifiers


class MoleculeQueryBody(QueryParametersBase):
    id: Optional[List[int]] = None
    molecule_hash: Optional[List[str]] = None
    molecular_formula: Optional[List[str]] = None
    identifiers: Optional[Dict[str, List[str]]] = None


class MoleculeModifyBody(BaseModel):
    name: Optional[str] = None
    comment: Optional[str] = None
    identifiers: Optional[MoleculeIdentifiers] = None
    overwrite_identifiers: bool = False
