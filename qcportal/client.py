from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Sequence,
    Iterable,
)

from .base_models import CommonBulkGetNamesBody, CommonBulkGetBody, ProjURLParameters
from .cache import PortalCache
from .client_base import PortalClientBase
from .datasets import (
    dataset_from_datamodel,
    BaseDataset,
    AllDatasetDataModelTypes,
    DatasetQueryModel,
    DatasetQueryRecords,
    DatasetDeleteParams,
    DatasetAddBody,
)
from .managers import ManagerQueryFilters, ManagerQueryIterator, ComputeManager
from .metadata_models import QueryMetadata, UpdateMetadata, InsertMetadata, DeleteMetadata
from .molecules import Molecule, MoleculeIdentifiers, MoleculeModifyBody, MoleculeQueryIterator, MoleculeQueryFilters
from .permissions import (
    UserInfo,
    RoleInfo,
    is_valid_username,
    is_valid_password,
    is_valid_rolename,
)
from .records import (
    records_from_datamodels,
    RecordStatusEnum,
    PriorityEnum,
    RecordQueryFilters,
    RecordModifyBody,
    RecordDeleteBody,
    RecordRevertBody,
    AllRecordDataModelTypes,
    BaseRecord,
    RecordQueryIterator,
)
from .records.gridoptimization import (
    GridoptimizationKeywords,
    GridoptimizationAddBody,
    GridoptimizationRecord,
    GridoptimizationQueryFilters,
)
from .records.manybody import (
    ManybodyKeywords,
    ManybodyRecord,
    ManybodyAddBody,
    ManybodyQueryFilters,
)
from .records.optimization import (
    OptimizationProtocols,
    OptimizationRecord,
    OptimizationQueryFilters,
    OptimizationSpecification,
    OptimizationAddBody,
)
from .records.reaction import (
    ReactionAddBody,
    ReactionRecord,
    ReactionKeywords,
    ReactionQueryFilters,
)
from .records.singlepoint import (
    QCSpecification,
    SinglepointRecord,
    SinglepointAddBody,
    SinglepointQueryFilters,
    SinglepointDriver,
    SinglepointProtocols,
)
from .records.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveAddBody,
    TorsiondriveRecord,
    TorsiondriveQueryFilters,
)

from . records.neb import (
    NEBKeywords,
    NEBAddBody,
    NEBQueryFilters,
    NEBRecord,
)

from .serverinfo import (
    AccessLogQueryFilters,
    AccessLogSummaryFilters,
    AccessLogSummaryEntry,
    AccessLogSummary,
    AccessLogQueryIterator,
    ErrorLogQueryFilters,
    ErrorLogQueryIterator,
    ServerStatsQueryFilters,
    ServerStatsQueryIterator,
    DeleteBeforeDateBody,
)
from .utils import make_list, make_str


# TODO : built-in query limit chunking, progress bars, fs caching and invalidation
class PortalClient(PortalClientBase):
    def __init__(
        self,
        address: str = "https://api.qcarchive.molssi.org",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        cache: Optional[Union[str, Path]] = None,
        max_memcache_size: Optional[int] = 1000000,
    ) -> None:
        """Initializes a PortalClient instance from an address and verification information.

        Parameters
        ----------
        address
            The IP and port of the FractalServer instance ("192.168.1.1:8888")
        username
            The username to authenticate with.
        password
            The password to authenticate with.
        verify
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided a SSL certificate and defaults back to self-signed
            SSL keys.
        cache
            Path to directory to use for cache.
            If None, only in-memory caching used.
        max_memcache_size
            Number of items to hold in client's memory cache.
            Increase this value to improve performance for repeated calls,
            at the cost of higher memory usage.
        """

        PortalClientBase.__init__(self, address, username, password, verify)
        self._cache = PortalCache(self, cachedir=cache, max_memcache_size=max_memcache_size)

    def __repr__(self) -> str:
        """A short representation of the current PortalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "PortalClient(server_name='{}', address='{}', username='{}', cache='{}')".format(
            self.server_name, self.address, self.username, self.cache
        )
        return ret

    def _repr_html_(self) -> str:

        output = f"""
        <h3>PortalClient</h3>
        <ul>
          <li><b>Server:   &nbsp; </b>{self.server_name}</li>
          <li><b>Address:  &nbsp; </b>{self.address}</li>
          <li><b>Username: &nbsp; </b>{self.username}</li>
          <li><b>Cache: &nbsp; </b>{self.cache}</li>
        </ul>
        """

        # postprocess due to raw spacing above
        return "\n".join([substr.strip() for substr in output.split("\n")])

    @property
    def cache(self):
        if self._cache.cachedir is not None:
            return os.path.relpath(self._cache.cachedir)
        else:
            return None

    def _get_with_cache(self, func, id, missing_ok, entity_type, include=None):
        str_id = make_str(id)
        ids = make_list(str_id)

        # pass through the cache first
        # remove any ids that were found in cache
        # if `include` filters passed, don't use cache, just query DB, as it's often faster
        # for a few fields
        if include is None:
            cached = self._cache.get(ids, entity_type=entity_type)
        else:
            cached = {}

        for i in cached:
            ids.remove(i)

        # if all ids found in cache, no need to go further
        if len(ids) == 0:
            if isinstance(id, list):
                return [cached[i] for i in str_id]
            else:
                return cached[str_id]

        # molecule getting does *not* support "include"
        if include is None:
            payload = {
                "data": {"ids": ids},
            }
        else:
            if "ids" not in include:
                include.append("ids")

            payload = {
                "meta": {"includes": include},
                "data": {"ids": ids},
            }

        results, to_cache = func(payload)

        # we only cache if no field filtering was done
        if include is None:
            self._cache.put(to_cache, entity_type=entity_type)

        # combine cached records with queried results
        results.update(cached)

        # check that we have results for all ids asked for
        missing = set(make_list(str_id)) - set(results.keys())

        if missing and not missing_ok:
            raise KeyError(f"No objects found for `id`: {missing}")

        # order the results by input id list
        if isinstance(id, list):
            ordered = [results.get(i, None) for i in str_id]
        else:
            ordered = results.get(str_id, None)

        return ordered

    # TODO - needed?
    def _query_cache(self):
        pass

    def get_server_information(self) -> Dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self._auto_request("get", "v1/information", None, None, Dict[str, Any], None, None)

    ##############################################################
    # Datasets
    ##############################################################
    def list_datasets(self):
        return self._auto_request(
            "get",
            f"v1/datasets",
            None,
            None,
            List[Dict[str, Any]],
            None,
            None,
        )

    def get_dataset(self, dataset_type: str, dataset_name: str):

        payload = DatasetQueryModel(dataset_name=dataset_name, dataset_type=dataset_type)

        ds = self._auto_request(
            "post",
            f"v1/datasets/query",
            DatasetQueryModel,
            None,
            AllDatasetDataModelTypes,
            payload,
            None,
        )

        return dataset_from_datamodel(ds, self)

    def query_dataset_records(
        self,
        record_id: Union[int, Iterable[int]],
        dataset_type: Optional[Iterable[str]] = None,
    ):

        payload = {
            "record_id": make_list(record_id),
            "dataset_type": dataset_type,
        }

        return self._auto_request(
            "post",
            f"v1/datasets/queryrecords",
            DatasetQueryRecords,
            None,
            List[Dict],
            payload,
            None,
        )

    def get_dataset_by_id(self, dataset_id: int):

        ds = self._auto_request(
            "get",
            f"v1/datasets/{dataset_id}",
            None,
            None,
            AllDatasetDataModelTypes,
            None,
            None,
        )

        return dataset_from_datamodel(ds, self)

    def get_dataset_status_by_id(self, dataset_id: int) -> Dict[str, Dict[RecordStatusEnum, int]]:

        return self._auto_request(
            "get",
            f"v1/datasets/{dataset_id}/status",
            None,
            ProjURLParameters,
            Dict[str, Dict[RecordStatusEnum, int]],
            None,
            None,
        )

    def add_dataset(
        self,
        dataset_type: str,
        name: str,
        description: Optional[str] = None,
        tagline: Optional[str] = None,
        tags: Optional[List[str]] = None,
        group: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None,
        visibility: bool = True,
        default_tag: str = "*",
        default_priority: PriorityEnum = PriorityEnum.normal,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BaseDataset:

        if description is None:
            description = ""
        if tagline is None:
            tagline = ""
        if tags is None:
            tags = []
        if group is None:
            group = "default"
        if provenance is None:
            provenance = {}
        if metadata is None:
            metadata = {}

        payload = DatasetAddBody(
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
            group=group,
            provenance=provenance,
            visibility=visibility,
            default_tag=default_tag,
            default_priority=default_priority,
            metadata=metadata,
        )

        ds_id = self._auto_request("post", f"v1/datasets/{dataset_type}", DatasetAddBody, None, int, payload, None)

        return self.get_dataset_by_id(ds_id)

    def delete_dataset(self, dataset_id: int, delete_records: bool):
        params = DatasetDeleteParams(delete_records=delete_records)

        return self._auto_request("delete", f"v1/datasets/{dataset_id}", None, DatasetDeleteParams, Any, None, params)

    ##############################################################
    # Molecules
    ##############################################################

    def get_molecules(
        self,
        molecule_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
    ) -> Union[Optional[Molecule], List[Optional[Molecule]]]:
        """Obtains molecules with the specified IDs from the server

        Parameters
        ----------
        molecule_ids
            A single molecule ID, or a list (or other sequence) of molecule IDs
        missing_ok
            If set to True, then missing molecules will be tolerated, and the returned list of
            Molecules will contain None for the corresponding IDs that were not found.

        Returns
        -------
        :
            Molecules, in the same order as the requested ids.
            If given a sequence of ids, the return value will be a list.
            Otherwise, it will be a single Molecule.
        """

        is_single = not isinstance(molecule_ids, Sequence)

        molecule_ids = make_list(molecule_ids)
        if not molecule_ids:
            return []

        body_data = CommonBulkGetBody(ids=molecule_ids, missing_ok=missing_ok)
        mols = self._auto_request(
            "post", "v1/molecules/bulkGet", CommonBulkGetBody, None, List[Optional[Molecule]], body_data, None
        )

        if is_single:
            return mols[0]
        else:
            return mols

    # TODO: we would like more fields to be queryable via the REST API for mols
    #       e.g. symbols/elements. Unless these are indexed might not be performant.
    # TODO: what was paginate: bool = False for?
    def query_molecules(
        self,
        molecule_hash: Optional[Union[str, Iterable[str]]] = None,
        molecular_formula: Optional[Union[str, Iterable[str]]] = None,
        identifiers: Optional[Dict[str, Union[str, Iterable[str]]]] = None,
        limit: Optional[int] = None,
    ) -> MoleculeQueryIterator:
        """Query molecules by attributes.

        Do not count on the returned molecules being in any particular order.

        Parameters
        ----------
        molecule_hash
            Queries molecules by hash
        molecular_formula
            Queries molecules by molecular formula.
            Molecular formulas are not order-sensitive (e.g. "H2O == OH2 != Oh2").
        identifiers
            Additional identifiers to search for (smiles, etc)
        limit
            The maximum number of Molecules to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "molecule_hash": make_list(molecule_hash),
            "molecular_formula": make_list(molecular_formula),
            "limit": limit,
        }

        if identifiers is not None:
            filter_dict["identifiers"] = {k: make_list(v) for k, v in identifiers.items()}

        filter_data = MoleculeQueryFilters(**filter_dict)
        return MoleculeQueryIterator(self, filter_data)

    def add_molecules(self, molecules: Sequence[Molecule]) -> Tuple[InsertMetadata, List[int]]:
        """Add molecules to the server database

        If the same molecule (defined by having the same hash) already exists, then the existing
        molecule is kept and that particular molecule is not added.

        Parameters
        ----------
        molecules
            A list of Molecules to add to the server.

        Returns
        -------
        :
            Metadata about what was inserted, and a list of IDs of the molecules
            in the same order as the `molecules` parameter.
        """

        if not molecules:
            return InsertMetadata(), []

        if len(molecules) > self.api_limits["add_molecules"]:
            raise RuntimeError(
                f"Cannot add {len(molecules)} molecules - over the limit of {self.api_limits['add_molecules']}"
            )

        mols = self._auto_request(
            "post",
            "v1/molecules/bulkCreate",
            List[Molecule],
            None,
            Tuple[InsertMetadata, List[int]],
            make_list(molecules),
            None,
        )
        return mols

    def modify_molecule(
        self,
        molecule_id: int,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        identifiers: Optional[Union[Dict[str, Any], MoleculeIdentifiers]] = None,
        overwrite_identifiers: bool = False,
    ) -> UpdateMetadata:
        """
        Modify molecules on the server

        This is only capable of updating the name, comment, and identifiers fields (except molecule_hash
        and molecular formula).

        If a molecule with that id does not exist, an exception is raised

        Parameters
        ----------
        molecule_id
            ID of the molecule to modify
        name
            New name for the molecule. If None, name is not changed.
        comment
            New comment for the molecule. If None, comment is not changed
        identifiers
            A new set of identifiers for the molecule
        overwrite_identifiers
            If True, the identifiers of the molecule are set to be those given exactly (ie, identifiers
            that exist in the DB but not in the new set will be removed). Otherwise, the new set of
            identifiers is merged into the existing ones. Note that molecule_hash and molecular_formula
            are never removed.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        body = {
            "name": name,
            "comment": comment,
            "identifiers": identifiers,
            "overwrite_identifiers": overwrite_identifiers,
        }

        return self._auto_request(
            "patch", f"v1/molecules/{molecule_id}", MoleculeModifyBody, None, UpdateMetadata, body, None
        )

    def delete_molecules(self, molecule_ids: Union[int, Sequence[int]]) -> DeleteMetadata:
        """Deletes molecules from the server

        This will not delete any molecules that are in use

        Parameters
        ----------
        molecule_ids
            An id or list of ids to query.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        molecule_ids = make_list(molecule_ids)
        if not molecule_ids:
            return DeleteMetadata()

        return self._auto_request(
            "post", "v1/molecules/bulkDelete", List[int], None, DeleteMetadata, molecule_ids, None
        )

    ##############################################################
    # General record functions
    ##############################################################

    def get_records(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[List[Optional[BaseRecord]], Optional[BaseRecord]]:
        """
        Obtain records of all types with specified IDs

        This function will return record objects of the given ID no matter
        what the type is. All records are unique by ID (ie, an optimization will never
        have the same ID as a singlepoint).

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = BaseRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[AllRecordDataModelTypes]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_records(
        self,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        record_type: Optional[Union[str, Iterable[str]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        limit: int = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Query records of all types based on common fields

        This is a general query of all record types, so it can only filter by fields
        that are common among all records.

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        record_type
            Query records whose type is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "record_id": make_list(record_id),
            "record_type": make_list(record_type),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = BaseRecord.transform_includes(include)

        filter_data = RecordQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, None)

    def reset_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Resets running or errored records to be waiting again
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.waiting)
        return self._auto_request("patch", "v1/records", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def cancel_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Marks running, waiting, or errored records as cancelled

        A cancelled record will not be picked up by a manager.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.cancelled)
        return self._auto_request("patch", "v1/records", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def invalidate_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Marks a completed record as invalid

        An invalid record is one that supposedly successfully completed. However, after review,
        is not correct.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.invalid)
        return self._auto_request("patch", "v1/records", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def delete_records(
        self, record_ids: Union[int, Sequence[int]], soft_delete=True, delete_children: bool = True
    ) -> DeleteMetadata:
        """
        Delete records from the database

        If soft_delete is True, then the record is just marked as deleted and actually deletion may
        happen later. Soft delete can be undone with undelete

        Parameters
        ----------
        record_ids
            Reset the status of these record ids
        soft_delete
            Don't actually delete the record, just mark it for later deletion
        delete_children
            If True, attempt to delete child records as well
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return DeleteMetadata()

        body_data = RecordDeleteBody(record_ids=record_ids, soft_delete=soft_delete, delete_children=delete_children)
        return self._auto_request(
            "post", "v1/records/bulkDelete", RecordDeleteBody, None, DeleteMetadata, body_data, None
        )

    def uninvalidate_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Undo the invalidation of records
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.invalid)
        return self._auto_request("post", "v1/records/revert", RecordRevertBody, None, UpdateMetadata, body_data, None)

    def uncancel_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Undo the cancellation of records
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.cancelled)
        return self._auto_request("post", "v1/records/revert", RecordRevertBody, None, UpdateMetadata, body_data, None)

    def undelete_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Undo the (soft) deletion of records
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.deleted)
        return self._auto_request("post", "v1/records/revert", RecordRevertBody, None, UpdateMetadata, body_data, None)

    def modify_records(
        self,
        record_ids: Union[int, Sequence[int]],
        new_tag: Optional[str] = None,
        new_priority: Optional[PriorityEnum] = None,
    ) -> UpdateMetadata:
        """
        Modify the tag or priority of a record
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()
        if new_tag is None and new_priority is None:
            return UpdateMetadata()

        body_data = RecordModifyBody(record_ids=record_ids, tag=new_tag, priority=new_priority)
        return self._auto_request("patch", "v1/records", RecordModifyBody, None, UpdateMetadata, body_data, None)

    def add_comment(self, record_ids: Union[int, Sequence[int]], comment: str) -> UpdateMetadata:
        """
        Adds a comment to records

        Parameters
        ----------
        record_ids
            The record or records to add the comments to

        comment
            The comment string to add. Your username will be added automatically

        Returns
        -------
        :
            Metadata about which records were updated
        """
        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body_data = RecordModifyBody(record_ids=record_ids, comment=comment)
        return self._auto_request("patch", "v1/records", RecordModifyBody, None, UpdateMetadata, body_data, None)

    ##############################################################
    # Singlepoint calculations
    ##############################################################

    def add_singlepoints(
        self,
        molecules: Union[int, Molecule, List[Union[int, Molecule]]],
        program: str,
        driver: str,
        method: str,
        basis: Optional[str],
        keywords: Optional[Dict[str, Any]] = None,
        protocols: Optional[Union[SinglepointProtocols, Dict[str, Any]]] = None,
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds new singlepoint computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per molecule.

        Parameters
        ----------
        molecules
            The Molecules or Molecule ids to compute with the above methods
        program
            The computational program to execute the result with (e.g., "rdkit", "psi4").
        driver
            The primary result that the compute will acquire {"energy", "gradient", "hessian", "properties"}
        method
            The computational method to use (e.g., "B3LYP", "PBE")
        basis
            The basis to apply to the computation (e.g., "cc-pVDZ", "6-31G")
        keywords
            The program-specific keywords for the computation
        protocols
            Protocols for storing more/less data for each computation
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority of the job (high, normal, low). Default is normal.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        molecules = make_list(molecules)
        if not molecules:
            return InsertMetadata(), []

        if len(molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data = {
            "molecules": molecules,
            "specification": {
                "program": program,
                "driver": driver,
                "method": method,
                "basis": basis,
            },
            "tag": tag,
            "priority": priority,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        return self._auto_request(
            "post",
            "v1/records/singlepoint/bulkCreate",
            SinglepointAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_singlepoints(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[SinglepointRecord], List[Optional[SinglepointRecord]]]:
        """
        Obtain singlepoint records with the specified IDs.

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = SinglepointRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/singlepoint/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[SinglepointRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_singlepoints(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        driver: Optional[Iterable[SinglepointDriver]] = None,
        method: Optional[Iterable[str]] = None,
        basis: Optional[Iterable[Optional[str]]] = None,
        molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Queries singlepoint records on the server

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose program is in the given list
        driver
            Query records whose driver is in the given list
        method
            Query records whose method is in the given list
        basis
            Query records whose basis is in the given list
        molecule_id
            Query records whose molecule (id) is in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        # Note - singlepoints don't have any children
        filter_dict = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "program": make_list(program),
            "driver": make_list(driver),
            "method": make_list(method),
            "basis": make_list(basis),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = SinglepointRecord.transform_includes(include)

        filter_data = SinglepointQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, "singlepoint")

    ##############################################################
    # Optimization calculations
    ##############################################################

    def add_optimizations(
        self,
        initial_molecules: Union[int, Molecule, List[Union[int, Molecule]]],
        program: str,
        qc_specification: QCSpecification,
        keywords: Optional[Dict[str, Any]] = None,
        protocols: Optional[OptimizationProtocols] = None,
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds new geometry optimization calculations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial molecule.

        Parameters
        ----------
        initial_molecules
            Initial molecule/geometry to optimize
        program
            Which program to use for the optimization (ie, geometric)
        qc_specification
            The method, basis, etc, to optimize the geometry with
        keywords
            Program-specific keywords for the optimization program (not the qc program)
        protocols
            Protocols for storing more/less data for each computation (for the optimization)
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority of the job (high, normal, low). Default is normal.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len('initial_molecules')} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "qc_specification": qc_specification,
            },
            "tag": tag,
            "priority": priority,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        return self._auto_request(
            "post",
            "v1/records/optimization/bulkCreate",
            OptimizationAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_optimizations(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[OptimizationRecord], List[Optional[OptimizationRecord]]]:
        """
        Obtain optimization records with the specified IDs.

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = OptimizationRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/optimization/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[OptimizationRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_optimizations(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        child_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        final_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Queries optimization records on the server

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (singlepoint calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        initial_molecule_id
            Query records whose initial molecule (id) is in the given list
        final_molecule_id
            Query records whose final molecule (id) is in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "final_molecule_id": make_list(final_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = OptimizationRecord.transform_includes(include)

        filter_data = OptimizationQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, "optimization")

    ##############################################################
    # Torsiondrive calculations
    ##############################################################

    def add_torsiondrives(
        self,
        initial_molecules: List[List[Union[int, Molecule]]],
        program: str,
        optimization_specification: OptimizationSpecification,
        keywords: Union[TorsiondriveKeywords, Dict[str, Any]],
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds new torsiondrive computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per set of molecules

        Parameters
        ----------
        initial_molecules
            Molecules to start the torsiondrives. Each torsiondrive can start with
            multiple molecules, so this is a nested list
        program
            The program to run the torsiondrive computation with ("torsiondrive")
        optimization_specification
            Specification of how each optimization of the torsiondrive should be run
        keywords
            The torsiondrive keywords for the computation
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority of the job (high, normal, low). Default is normal.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "as_service": True,
            "tag": tag,
            "priority": priority,
        }

        return self._auto_request(
            "post",
            "v1/records/torsiondrive/bulkCreate",
            TorsiondriveAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_torsiondrives(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[TorsiondriveRecord], List[Optional[TorsiondriveRecord]]]:
        """
        Obtain torsiondrive records with the specified IDs.

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = TorsiondriveRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/torsiondrive/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[TorsiondriveRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_torsiondrives(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        child_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        optimization_program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Queries torsiondrive records on the server

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose torsiondrive program is in the given list
        optimization_program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        initial_molecule_id
            Query records whose initial molecule (id) is in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = TorsiondriveRecord.transform_includes(include)

        filter_data = TorsiondriveQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, "torsiondrive")

    ##############################################################
    # Grid optimization calculations
    ##############################################################

    def add_gridoptimizations(
        self,
        initial_molecules: Union[int, Molecule, Sequence[Union[int, Molecule]]],
        program: str,
        optimization_specification: OptimizationSpecification,
        keywords: Union[GridoptimizationKeywords, Dict[str, Any]],
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds new gridoptimization computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial molecule

        Parameters
        ----------
        initial_molecules
            Molecules to start the gridoptimizations. Each gridoptimization starts with
             a single molecule.
        program
            The program to run the gridoptimization computation with ("gridoptimization")
        optimization_specification
            Specification of how each optimization of the gridoptimization should be run
        keywords
            The gridoptimization keywords for the computation
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority of the job (high, normal, low). Default is normal.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "tag": tag,
            "priority": priority,
        }

        return self._auto_request(
            "post",
            "v1/records/gridoptimization/bulkCreate",
            GridoptimizationAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_gridoptimizations(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[GridoptimizationRecord], List[Optional[GridoptimizationRecord]]]:
        """
        Obtain gridoptimization records with the specified IDs.

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = GridoptimizationRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/gridoptimization/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[GridoptimizationRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_gridoptimizations(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        child_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        optimization_program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Queries gridoptimization records on the server

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose gridoptimization program is in the given list
        optimization_program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        initial_molecule_id
            Query records whose initial molecule (id) is in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "optimization_program": make_list(optimization_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = GridoptimizationRecord.transform_includes(include)

        filter_data = GridoptimizationQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, "gridoptimization")

    ##############################################################
    # Reactions
    ##############################################################

    def add_reactions(
        self,
        stoichiometries: Sequence[Sequence[Sequence[float, Union[int, Molecule]]]],
        program: str,
        singlepoint_specification: Optional[QCSpecification],
        optimization_specification: Optional[OptimizationSpecification],
        keywords: ReactionKeywords,
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds new reaction computations to the server

        Reactions can have a singlepoint specification, optimization specification,
        or both; at least one must be specified. If both are specified, an optimization
        is done, followed by a singlepoint computation. Otherwise, only the specification
        that is specified is used.

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per set of molecules

        Parameters
        ----------
        stoichiometries
            Coefficients and molecules of the reaction. Each reaction has multiple
            molecules/coefficients, so this is a nested list
        program
            The program for running the reaction computation ("reaction")
        singlepoint_specification
            The specification for singlepoint energy calculations
        optimization_specification
            The specification for optimization calculations
        keywords
            The keywords for the reaction calculation/service
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority of the job (high, normal, low). Default is normal.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if not stoichiometries:
            return InsertMetadata(), []

        if len(stoichiometries) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(stoichiometries)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data = {
            "stoichiometries": stoichiometries,
            "specification": {
                "program": program,
                "singlepoint_specification": singlepoint_specification,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "tag": tag,
            "priority": priority,
        }

        return self._auto_request(
            "post",
            "v1/records/reaction/bulkCreate",
            ReactionAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_reactions(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[ReactionRecord], List[Optional[ReactionRecord]]]:
        """
        Obtain reaction records with the specified IDs.

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = ReactionRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/reaction/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[ReactionRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_reactions(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        child_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        optimization_program: Optional[Iterable[Optional[str]]] = None,
        molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Queries reaction records on the server

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose reaction program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        optimization_program
            Query records whose optimization program is in the given list
        molecule_id
            Query reactions that contain a molecule (id) is in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "optimization_program": make_list(optimization_program),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = ReactionRecord.transform_includes(include)

        meta, record_data = self._auto_request(
            "post",
            "v1/records/reaction/query",
            ReactionQueryFilters,
            None,
            Tuple[QueryMetadata, List[ReactionRecord._DataModel]],
            filter_dict,
            None,
        )

        filter_data = ReactionQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, "reaction")

    ##############################################################
    # Manybody calculations
    ##############################################################

    def add_manybodys(
        self,
        initial_molecules: Sequence[Union[int, Molecule]],
        program: str,
        singlepoint_specification: QCSpecification,
        keywords: ManybodyKeywords,
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds new manybody expansion computations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per initial molecule.

        Parameters
        ----------
        initial_molecules
            Initial molecules for the manybody expansion. Must have > 1 fragments.
        program
            The program to run the manybody computation with ("manybody")
        singlepoint_specification
            Specification for the singlepoint calculations done in the expansion
        keywords
            The keywords for the manybody program
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority of the job (high, normal, low). Default is normal.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        initial_molecules = make_list(initial_molecules)
        if not initial_molecules:
            return InsertMetadata(), []

        if len(initial_molecules) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(initial_molecules)} records - over the limit of {self.api_limits['add_records']}"
            )

        body_data = {
            "initial_molecules": initial_molecules,
            "specification": {
                "program": program,
                "singlepoint_specification": singlepoint_specification,
                "keywords": keywords,
            },
            "tag": tag,
            "priority": priority,
        }

        return self._auto_request(
            "post",
            "v1/records/manybody/bulkCreate",
            ManybodyAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_manybodys(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[ManybodyRecord], List[Optional[ManybodyRecord]]]:
        """
        Obtain manybody records with the specified IDs.

        Records will be returned in the same order as the record ids.

        Parameters
        ----------
        record_ids
            Single ID or sequence/list of records to obtain
        missing_ok
            If set to True, then missing records will be tolerated, and the returned
            records will contain None for the corresponding IDs that were not found.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            If a single ID was specified, returns just that record. Otherwise, returns
            a list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = ManybodyRecord.transform_includes(include)

        record_data = self._auto_request(
            "post",
            "v1/records/manybody/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[ManybodyRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_manybodys(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        child_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        initial_molecule_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator:
        """
        Queries reaction records on the server

        Do not count on the returned records being in any particular order.

        Parameters
        ----------
        record_id
            Query records whose ID is in the given list
        manager_name
            Query records that were completed (or are currently runnning) on a manager is in the given list
        status
            Query records whose status is in the given list
        dataset_id
            Query records that are part of a dataset is in the given list
        parent_id
            Query records that have a parent is in the given list
        child_id
            Query records that have a child (optimization calculation) is in the given list
        created_before
            Query records that were created before the given date/time
        created_after
            Query records that were created after the given date/time
        modified_before
            Query records that were modified before the given date/time
        modified_after
            Query records that were modified after the given date/time
        program
            Query records whose reaction program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose qc method is in the given list
        qc_basis
            Query records whose qc basis is in the given list
        initial_molecule_id
            Query manybody calculations that contain an initial molecule (id) is in the given list
        limit
            The maximum number of records to return. Note that the server limit is always obeyed.
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_molecule_id": make_list(initial_molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = ManybodyRecord.transform_includes(include)

        filter_data = ManybodyQueryFilters(**filter_dict)

        return RecordQueryIterator(self, filter_data, "manybody")

    ##############################################################
    # NEB
    ##############################################################

    def add_nebs(
        self,
        initial_chains: List[List[Union[int, Molecule]]],
        program: str,
        singlepoint_specification: QCSpecification,
        keywords: Union[NEBKeywords, Dict[str, Any]],
        tag: str = "*",
        priority: PriorityEnum = PriorityEnum.normal,
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds neb calculations to the server
        """
        if not initial_chains:
            return InsertMetadata(), []

        body_data = {
            "initial_chains": initial_chains,
            "specification": {
                "program": program,
                "singlepoint_specification": singlepoint_specification,
                "keywords": keywords,
            },
            "tag": tag,
            "priority": priority,
        }

        if len(body_data["initial_chains"]) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(body_data['initial_chains'])} records - over the limit of {self.api_limits['add_records']}"
            )

        return self._auto_request(
            "post",
            "v1/records/neb/bulkCreate",
            NEBAddBody,
            None,
            Tuple[InsertMetadata, List[int]],
            body_data,
            None,
        )

    def get_nebs(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[NEBRecord], List[Optional[NEBRecord]]]:

        is_single = not isinstance(record_ids, Sequence)

        record_ids = make_list(record_ids)
        if not record_ids:
            return []

        if len(record_ids) > self.api_limits["get_records"]:
            raise RuntimeError(
                f"Cannot get {len(record_ids)} records - over the limit of {self.api_limits['get_records']}"
            )

        body_data = {"ids": record_ids, "missing_ok": missing_ok}

        if include:
            body_data["include"] = SinglepointRecord.transform_includes(include)


        record_data = self._auto_request(
            "post",
            "v1/records/neb/bulkGet",
            CommonBulkGetBody,
            None,
            List[Optional[NEBRecord._DataModel]],
            body_data,
            None,
        )

        records = records_from_datamodels(record_data, self)

        if is_single:
            return records[0]
        else:
            return records

    def query_nebs(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        dataset_id: Optional[Iterable[int]] = None,
        parent_id: Optional[Iterable[int]] = None,
        child_id: Optional[Iterable[int]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        program: Optional[Iterable[str]] = None,
        neb_program: Optional[Iterable[str]] = None,
        qc_program: Optional[Iterable[str]] = None,
        qc_method: Optional[Iterable[str]] = None,
        qc_basis: Optional[Iterable[Optional[str]]] = None,
        initial_chain_id: Optional[Iterable[int]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        include_task: bool = False,
        include_service: bool = False,
        include_outputs: bool = False,
        include_comments: bool = False,
        include_initial_chain: bool = False,
        include_singlepoints: bool = False,
    ) -> Tuple[QueryMetadata, List[NEBRecord]]:
        """Queries neb records from the server."""

        if limit is not None and limit > self.api_limits["get_records"]:
            warnings.warn(f"Specified limit of {limit} is over the server limit. Server limit will be used")
            limit = min(limit, self.api_limits["get_records"])

        query_data = {
            "record_id": make_list(record_id),
            "manager_name": make_list(manager_name),
            "status": make_list(status),
            "dataset_id": make_list(dataset_id),
            "parent_id": make_list(parent_id),
            "child_id": make_list(child_id),
            "program": make_list(program),
            "neb_program": make_list(neb_program),
            "qc_program": make_list(qc_program),
            "qc_method": make_list(qc_method),
            "qc_basis": make_list(qc_basis),
            "initial_chain_id": make_list(initial_chain_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        include = set()

        # We must add '*' so that all the default fields are included
        if include_task:
            include |= {"*", "task"}
        if include_service:
            include |= {"*", "service"}
        if include_outputs:
            include |= {"*", "compute_history.*", "compute_history.outputs"}
        if include_comments:
            include |= {"*", "comments"}
        if include_initial_chain:
            include |= {"*", "initial_chain"}
        if include_singlepoints:
            include |= {"*", "singlepoints.*", "singlepoints.singlepoint_record"}

        if include:
            query_data["include"] = include

        meta, record_data = self._auto_request(
            "post",
            "v1/records/neb/query",
            NEBQueryFilters,
            None,
            Tuple[QueryMetadata, List[NEBRecord._DataModel]],
            query_data,
            None,
        )

        return meta, records_from_datamodels(record_data, self)


    ##############################################################
    # Managers
    ##############################################################

    def get_managers(
        self,
        names: Union[str, Sequence[str]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[ComputeManager], List[Optional[ComputeManager]]]:
        """Obtain manager information from the server with the specified names

        Parameters
        ----------
        names
            A manager name or list of names
        missing_ok
            If set to True, then missing managers will be tolerated, and the returned
            managers will contain None for the corresponding managers that were not found.
        include
            Additional fields to include in the returned managers

        Returns
        -------
        :
            If a single name was specified, returns just that manager. Otherwise, returns
            a list of managers.  If missing_ok was specified, None will be substituted for a manager
            that was not found.
        """

        is_single = isinstance(names, str)

        names = make_list(names)
        if not names:
            return []

        body_data = CommonBulkGetNamesBody(names=names, missing_ok=missing_ok)

        if include:
            body_data.include = ComputeManager.transform_includes(include)

        managers = self._auto_request(
            "post", "v1/managers/bulkGet", CommonBulkGetNamesBody, None, List[Optional[ComputeManager]], body_data, None
        )

        if is_single:
            return managers[0]
        else:
            return managers

    def query_managers(
        self,
        manager_ids: Optional[Union[int, Iterable[int]]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        cluster: Optional[Union[str, Iterable[str]]] = None,
        hostname: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        limit: Optional[int] = None,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> ManagerQueryIterator:
        """
        Queries manager information on the server

        Parameters
        ----------
        manager_ids
            ID assigned to the manager (this is not the UUID. This should be used very rarely).
        name
            Queries managers whose name is in the given list
        cluster
            Queries managers whose assigned cluster is in the given list
        hostname
            Queries managers whose hostname is in the given list
        status
            Queries managers whose status is in the given list
        modified_before
            Query for managers last modified before a certain time
        modified_after
            Query for managers last modified after a certain time
        limit
            The maximum number of managers to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "manager_id": make_list(manager_ids),
            "name": make_list(name),
            "cluster": make_list(cluster),
            "hostname": make_list(hostname),
            "status": make_list(status),
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        if include:
            filter_dict["include"] = ["*", "log"]

        filter_data = ManagerQueryFilters(**filter_dict)
        return ManagerQueryIterator(self, filter_data)

    ##############################################################
    # Server statistics and logs
    ##############################################################

    def query_server_stats(
        self,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> ServerStatsQueryIterator:
        """
        Query server statistics

        These statistics are captured at certain times, and are available for querying (as long
        as they are not deleted)

        Parameters
        ----------
        before
            Return statistics captured before the specified date/time
        after
            Return statistics captured after the specified date/time
        limit
            The maximum number of statistics entries to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_data = ServerStatsQueryFilters(before=before, after=after, limit=limit)
        return ServerStatsQueryIterator(self, filter_data)

    def delete_server_stats(self, before: datetime) -> int:
        """
        Delete server statistics from the server

        Parameters
        ----------
        before
            Delete statistics captured before the given date/time

        Returns
        -------
        :
            The number of statistics entries deleted from the server
        """

        body_data = DeleteBeforeDateBody(before=before)
        return self._auto_request(
            "post", "v1/server_stats/bulkDelete", DeleteBeforeDateBody, None, int, body_data, None
        )

    def query_access_log(
        self,
        access_type: Optional[Union[str, Iterable[str]]] = None,
        access_method: Optional[Union[str, Iterable[str]]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> AccessLogQueryIterator:
        """
        Query the server access log

        This log contains information about who accessed the server, and when.

        Parameters
        ----------
        access_type
            Return log entries whose access_type is in the given list
        access_method
            Return log entries whose access_method is in the given list
        before
            Return log entries captured before the specified date/time
        after
            Return log entries captured after the specified date/time
        limit
            The maximum number of log entries to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_data = AccessLogQueryFilters(
            access_type=make_list(access_type),
            access_method=make_list(access_method),
            before=before,
            after=after,
            limit=limit,
        )

        return AccessLogQueryIterator(self, filter_data)

    def delete_access_log(self, before: datetime) -> int:
        """
        Delete access log entries from the server

        Parameters
        ----------
        before
            Delete access log entries captured before the given date/time

        Returns
        -------
        :
            The number of access log entries deleted from the server
        """

        body_data = DeleteBeforeDateBody(before=before)
        return self._auto_request("post", "v1/access_logs/bulkDelete", DeleteBeforeDateBody, None, int, body_data, None)

    def query_error_log(
        self,
        error_id: Optional[Union[int, Iterable[int]]] = None,
        username: Optional[Union[str, Iterable[str]]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> ErrorLogQueryIterator:
        """
        Query the server's internal error log

        This log contains internal errors that are not always passed to the user.

        Parameters
        ----------
        error_id
            Return error log entries whose id is in the list
        username
            Return error log entries whose username is in the list
        before
            Return error log entries captured before the specified date/time
        after
            Return error log entries captured after the specified date/time
        limit
            The maximum number of log entries to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_data = ErrorLogQueryFilters(
            error_id=make_list(error_id),
            username=make_list(username),
            before=before,
            after=after,
            limit=limit,
        )

        return ErrorLogQueryIterator(self, filter_data)

    def delete_error_log(self, before: datetime) -> int:
        """
        Delete error log entries from the server

        Parameters
        ----------
        before
            Delete error log entries captured before the given date/time

        Returns
        -------
        :
            The number of error log entries deleted from the server
        """
        body_data = DeleteBeforeDateBody(before=before)
        return self._auto_request(
            "post", "v1/server_errors/bulkDelete", DeleteBeforeDateBody, None, int, body_data, None
        )

    def query_access_summary(
        self,
        group_by: str = "day",
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
    ) -> AccessLogSummary:
        """Obtains summaries of access data

        This aggregate data is created on the server, so you don't need to download all the
        log entries and do it yourself.

        Parameters
        ----------
        group_by
            How to group the data. Valid options are "user", "hour", "day", "country", "subdivision"
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        """

        url_params = {
            "group_by": group_by,
            "before": before,
            "after": after,
        }

        entries = self._auto_request(
            "get",
            "v1/access_logs/summary",
            None,
            AccessLogSummaryFilters,
            Dict[str, List[AccessLogSummaryEntry]],
            None,
            url_params,
        )

        return AccessLogSummary(entries=entries)

    ##############################################################
    # User & role management
    ##############################################################

    def list_roles(self) -> List[RoleInfo]:
        """
        List all user roles on the server
        """

        return self._auto_request("get", "v1/roles", None, None, List[RoleInfo], None, None)

    def get_role(self, rolename: str) -> RoleInfo:
        """
        Get information about a role on the server
        """

        is_valid_rolename(rolename)
        return self._auto_request("get", f"v1/roles/{rolename}", None, None, RoleInfo, None, None)

    def add_role(self, role_info: RoleInfo) -> None:
        """
        Adds a role with permissions to the server

        If not successful, an exception is raised.
        """

        is_valid_rolename(role_info.rolename)
        return self._auto_request("post", "v1/roles", RoleInfo, None, None, role_info, None)

    def modify_role(self, role_info: RoleInfo) -> RoleInfo:
        """
        Modifies the permissions of a role on the server

        If not successful, an exception is raised.

        Returns
        -------
        :
            A copy of the role as it now appears on the server
        """

        is_valid_rolename(role_info.rolename)
        return self._auto_request("put", f"v1/roles/{role_info.rolename}", RoleInfo, None, RoleInfo, role_info, None)

    def delete_role(self, rolename: str) -> None:
        """
        Deletes a role from the server

        This will not delete any role to which a user is assigned

        Will raise an exception on error

        Parameters
        ----------
        rolename
            Name of the role to delete

        """
        is_valid_rolename(rolename)
        return self._auto_request("delete", f"v1/roles/{rolename}", None, None, None, None, None)

    def list_users(self) -> List[UserInfo]:
        """
        List all user roles on the server
        """

        return self._auto_request("get", "v1/users", None, None, List[UserInfo], None, None)

    def get_user(self, username: Optional[str] = None, as_admin: bool = False) -> UserInfo:
        """
        Get information about a user on the server

        If the username is not supplied, then info about the currently logged-in user is obtained

        Parameters
        ----------
        username
            The username to get info about
        as_admin
            If True, then fetch the user from the admin user management endpoint. This is the default
            if requesting a user other than the currently logged-in user

        Returns
        -------
        :
            Information about the user
        """

        if username is None:
            username = self.username

        if username is None:
            raise RuntimeError("Cannot get user - not logged in?")

        # Check client side so we can bail early
        is_valid_username(username)

        if username != self.username:
            as_admin = True

        if as_admin is False:
            # For the currently logged-in user, use the "me" endpoint. The other endpoint is
            # restricted to admins
            uinfo = self._auto_request("get", f"v1/me", None, None, UserInfo, None, None)

            if uinfo.username != self.username:
                raise RuntimeError(
                    f"Inconsistent username - client is {self.username} but logged in as {uinfo.username}"
                )
        else:
            uinfo = self._auto_request("get", f"v1/users/{username}", None, None, UserInfo, None, None)

        return uinfo

    def add_user(self, user_info: UserInfo, password: Optional[str] = None) -> str:
        """
        Adds a user to the server

        Parameters
        ----------
        user_info
            Info about the user to add
        password
            The user's password. If None, then one will be generated

        Returns
        -------
        :
            The password of the user (either the same as the supplied password, or the
            server-generated one)

        """

        is_valid_username(user_info.username)
        is_valid_rolename(user_info.role)

        if password is not None:
            is_valid_password(password)

        if user_info.id is not None:
            raise RuntimeError("Cannot add user when user_info contains an id")

        return self._auto_request(
            "post", "v1/users", Tuple[UserInfo, Optional[str]], None, str, (user_info, password), None
        )

    def modify_user(self, user_info: UserInfo, as_admin: bool = False) -> UserInfo:
        """
        Modifies a user on the server

        The user is determined by the username field of the input UserInfo, although the id
        and username are checked for consistency.

        Depending on the current user's permissions, some fields may not be updatable.



        Parameters
        ----------
        user_info
            Updated information for a user
        as_admin
            If True, then attempt to modify fields that are only modifiable by an admin (enabled, role).
            This is the default if requesting a user other than the currently logged-in user.

        Returns
        -------
        :
            The updated user information as it appears on the server
        """

        is_valid_username(user_info.username)
        is_valid_rolename(user_info.role)

        if as_admin or (user_info.username != self.username):
            url = f"v1/users/{user_info.username}"
        else:
            url = "v1/me"

        return self._auto_request("put", url, UserInfo, None, UserInfo, user_info, None)

    def change_user_password(self, username: Optional[str] = None, new_password: Optional[str] = None) -> str:
        """
        Change a users password

        If the username is not specified, then the current logged-in user is used.

        If the password is not specified, then one is automatically generated by the server.

        Parameters
        ----------
        username
            The name of the user whose password to change. If None, then use the currently logged-in user
        new_password
            Password to change to. If None, let the server generate one.

        Returns
        -------
        :
            The new password (either the same as the supplied one, or the server generated one
        """

        if username is None:
            username = self.username

        is_valid_username(username)

        if new_password is not None:
            is_valid_password(new_password)

        if username == self.username:
            url = "v1/me/password"
        else:
            url = f"v1/users/{username}/password"

        return self._auto_request("put", url, Optional[str], None, str, new_password, None)

    def delete_user(self, username: str) -> None:
        """
        Delete a user from the server
        """

        is_valid_username(username)

        if username == self.username:
            raise RuntimeError("Cannot delete your own user!")

        return self._auto_request("delete", f"v1/users/{username}", None, None, None, None, None)
