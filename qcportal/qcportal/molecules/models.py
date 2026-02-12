from ..base_models import QueryModelBase, RestModelBase, QueryIteratorBase
from ..qcschema_v1 import Molecule, MoleculeIdentifiers


class MoleculeQueryFilters(QueryModelBase):
    molecule_id: list[int] | None = None
    molecule_hash: list[str] | None = None
    molecular_formula: list[str] | None = None
    identifiers: dict[str, list[str]] | None = None


class MoleculeModifyBody(RestModelBase):
    name: str | None = None
    comment: str | None = None
    identifiers: MoleculeIdentifiers | None = None
    overwrite_identifiers: bool = False


class MoleculeUploadOptions(RestModelBase):
    dummy: bool = True


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

    def _request(self) -> list[Molecule]:
        molecule_ids = self._client.make_request(
            "post",
            "api/v1/molecules/query",
            list[int],
            body_model=MoleculeQueryFilters,
            body=self._query_filters,
        )

        molecules = self._client.get_molecules(molecule_ids)

        return molecules
