from typing import Optional, List, Dict

from qcelemental.models import Molecule
from qcelemental.models.molecule import Identifiers as MoleculeIdentifiers

from ..base_models import QueryModelBase, RestModelBase, QueryIteratorBase


class MoleculeQueryFilters(QueryModelBase):
    molecule_id: Optional[List[int]] = None
    molecule_hash: Optional[List[str]] = None
    molecular_formula: Optional[List[str]] = None
    identifiers: Optional[Dict[str, List[str]]] = None


class MoleculeModifyBody(RestModelBase):
    name: Optional[str] = None
    comment: Optional[str] = None
    identifiers: Optional[MoleculeIdentifiers] = None
    overwrite_identifiers: bool = False


class MoleculeQueryIterator(QueryIteratorBase[Molecule]):
    """
    Iterator for molecule queries

    This iterator transparently handles batching and pagination over the results
    of a molecule query.
    """

    def __init__(self, client, query_filters: MoleculeQueryFilters):
        """
        Construct an iterator

        Parameters
        ----------
        client
            QCPortal client object used to contact/retrieve data from the server
        query_filters
            The actual query information to send to the server
        """

        api_limit = client.api_limits["get_molecules"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> List[Molecule]:
        molecule_ids = self._client.make_request(
            "post",
            "api/v1/molecules/query",
            List[int],
            body_model=MoleculeQueryFilters,
            body=self._query_filters,
        )

        molecules = self._client.get_molecules(molecule_ids)

        return molecules
