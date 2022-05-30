from typing import Optional, List, Tuple, Dict, Any

from qcelemental.models import Molecule
from qcelemental.models.molecule import Identifiers as MoleculeIdentifiers

from ..base_models import QueryProjModelBase, RestModelBase, QueryIteratorBase
from ..metadata_models import QueryMetadata


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


class MoleculeQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: MoleculeQueryFilters):
        api_limit = client.api_limits["get_molecules"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[Molecule]]:
        return self.client._auto_request(
            "post",
            "v1/molecules/query",
            MoleculeQueryFilters,
            None,
            Tuple[Optional[QueryMetadata], List[Molecule]],
            self.query_filters,
            None,
        )
