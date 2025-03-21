from __future__ import annotations

import logging
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union, Sequence, Iterable, TypeVar, Type, Literal

from tabulate import tabulate

from qcportal.cache import DatasetCache, read_dataset_metadata
from qcportal.external_files import ExternalFile
from qcportal.gridoptimization import (
    GridoptimizationKeywords,
    GridoptimizationAddBody,
    GridoptimizationRecord,
    GridoptimizationQueryFilters,
)
from qcportal.manybody import (
    BSSECorrectionEnum,
    ManybodyRecord,
    ManybodyAddBody,
    ManybodyKeywords,
    ManybodyQueryFilters,
)
from qcportal.neb import (
    NEBKeywords,
    NEBAddBody,
    NEBQueryFilters,
    NEBRecord,
)
from qcportal.optimization import (
    OptimizationProtocols,
    OptimizationRecord,
    OptimizationQueryFilters,
    OptimizationSpecification,
    OptimizationAddBody,
)
from qcportal.reaction import (
    ReactionAddBody,
    ReactionRecord,
    ReactionKeywords,
    ReactionQueryFilters,
)
from qcportal.services.models import (  # noqa
    ServiceSubtaskRecord,
)
from qcportal.singlepoint import (
    QCSpecification,
    SinglepointRecord,
    SinglepointAddBody,
    SinglepointQueryFilters,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveAddBody,
    TorsiondriveRecord,
    TorsiondriveQueryFilters,
)
from .auth import (
    UserInfo,
    RoleInfo,
    GroupInfo,
    is_valid_username,
    is_valid_password,
    is_valid_rolename,
    is_valid_groupname,
)
from .base_models import CommonBulkGetNamesBody, CommonBulkGetBody
from .cache import PortalCache
from .client_base import PortalClientBase
from .dataset_models import (
    BaseDataset,
    DatasetQueryModel,
    DatasetQueryRecords,
    DatasetDeleteParams,
    DatasetCloneBody,
    DatasetAddBody,
    dataset_from_dict,
    load_dataset_view,  # noqa
    create_dataset_view,
)
from .internal_jobs import InternalJob, InternalJobQueryFilters, InternalJobQueryIterator, InternalJobStatusEnum
from .managers import ManagerQueryFilters, ManagerQueryIterator, ComputeManager
from .metadata_models import UpdateMetadata, InsertMetadata, DeleteMetadata
from .molecules import Molecule, MoleculeIdentifiers, MoleculeModifyBody, MoleculeQueryIterator, MoleculeQueryFilters
from .record_models import (
    RecordStatusEnum,
    PriorityEnum,
    RecordQueryFilters,
    RecordModifyBody,
    RecordDeleteBody,
    RecordRevertBody,
    BaseRecord,
    RecordQueryIterator,
    records_from_dicts,
)
from .serverinfo import (
    AccessLogQueryFilters,
    AccessLogSummaryFilters,
    AccessLogSummaryEntry,
    AccessLogSummary,
    AccessLogQueryIterator,
    ErrorLogQueryFilters,
    ErrorLogQueryIterator,
    DeleteBeforeDateBody,
)
from .utils import make_list, chunk_iterable, process_chunk_iterable

_T = TypeVar("_T", bound=BaseRecord)


class PortalClient(PortalClientBase):
    """
    Main class for interacting with a QCArchive server
    """

    def __init__(
        self,
        address: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        show_motd: bool = True,
        *,
        cache_dir: Optional[str] = None,
        cache_max_size: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        address
            The host or IP address of the FractalServer instance, including protocol and port if necessary
            ("https://ml.qcarchive.molssi.org", "http://192.168.1.10:8888")
        username
            The username to authenticate with.
        password
            The password to authenticate with.
        verify
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided an SSL certificate and defaults back to self-signed
            SSL keys.
        show_motd
            If a Message-of-the-Day is available, display it
        cache_dir
            Directory to store an internal cache of records and other data
        cache_max_size
            Maximum size of the cache directory
        """

        PortalClientBase.__init__(self, address, username, password, verify, show_motd)
        self._logger = logging.getLogger("PortalClient")
        self.cache = PortalCache(address, cache_dir, cache_max_size)

    def __repr__(self) -> str:
        """A short representation of the current PortalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "PortalClient(server_name='{}', address='{}', username='{}')".format(
            self.server_name, self.address, self.username
        )
        return ret

    def _repr_html_(self) -> str:
        output = f"""
        <h3>PortalClient</h3>
        <ul>
          <li><b>Server:   &nbsp; </b>{self.server_name}</li>
          <li><b>Address:  &nbsp; </b>{self.address}</li>
          <li><b>Username: &nbsp; </b>{self.username}</li>
        </ul>
        """

        # postprocess due to raw spacing above
        return "\n".join([substr.strip() for substr in output.split("\n")])

    def get_server_information(self) -> Dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self.make_request("get", "api/v1/information", Dict[str, Any])

    #################################################
    # Message-of-the-Day (MOTD)
    #################################################
    def get_motd(self) -> str:
        """
        Gets the Message-of-the-Day (MOTD) from the server
        """

        return self.make_request("get", "api/v1/motd", str)

    def set_motd(self, new_motd: str) -> str:
        """
        Sets the Message-of-the-Day (MOTD) on the server
        """

        return self.make_request("put", "api/v1/motd", None, body=new_motd)

    ##############################################################
    # Datasets
    ##############################################################
    def list_datasets(self):
        return self.make_request("get", f"api/v1/datasets", List[Dict[str, Any]])

    def list_datasets_table(self) -> str:
        ds_list = self.list_datasets()

        # older servers don't have record_count
        if all("record_count" in x for x in ds_list):
            headers = ["id", "type", "record_count", "name"]
            table = [(x["id"], x["dataset_type"], x["record_count"], x["dataset_name"]) for x in ds_list]
        else:
            headers = ["id", "type", "name"]
            table = [(x["id"], x["dataset_type"], x["dataset_name"]) for x in ds_list]

        return tabulate(table, headers=headers)

    def print_datasets_table(self) -> None:
        print(self.list_datasets_table())

    def get_dataset(self, dataset_type: str, dataset_name: str):
        body = DatasetQueryModel(dataset_name=dataset_name, dataset_type=dataset_type)
        ds = self.make_request("post", f"api/v1/datasets/query", Dict[str, Any], body=body)

        return dataset_from_dict(ds, self)

    def query_dataset_records(
        self,
        record_id: Union[int, Iterable[int]],
        dataset_type: Optional[Iterable[str]] = None,
    ):
        body = DatasetQueryRecords(record_id=make_list(record_id), dataset_type=dataset_type)
        return self.make_request("post", f"api/v1/datasets/queryrecords", List[Dict], body=body)

    def get_dataset_by_id(self, dataset_id: int):
        ds = self.make_request("get", f"api/v1/datasets/{dataset_id}", Dict[str, Any])
        return dataset_from_dict(ds, self)

    def dataset_from_cache(self, file_path: str) -> BaseDataset:
        ds_meta = read_dataset_metadata(file_path)
        ds_type = BaseDataset.get_subclass(ds_meta["dataset_type"])
        ds_cache = DatasetCache(file_path, False, ds_type)

        ds = dataset_from_dict(ds_meta, self, cache_data=ds_cache)

        # Check to make sure we are connected to the same server
        cache_address = ds_cache.get_metadata("client_address")
        if cache_address != self.address:
            raise RuntimeError(f"Cache file comes from {cache_address}, but currently connected to {self.address}")

        try:
            self.get_dataset_by_id(ds.id)
        except:
            self._logger.warning(f"Dataset {ds.id} could not be found on the server. Marking as read-only cache")
            ds_cache.read_only = True

        return ds

    def create_dataset_view(
        self, dataset_id: int, file_path: str, include: Optional[Iterable[str]] = None, overwrite: bool = False
    ):
        return create_dataset_view(self, dataset_id, file_path, include, overwrite)

    def get_dataset_status_by_id(self, dataset_id: int) -> Dict[str, Dict[RecordStatusEnum, int]]:
        return self.make_request("get", f"api/v1/datasets/{dataset_id}/status", Dict[str, Dict[RecordStatusEnum, int]])

    def add_dataset(
        self,
        dataset_type: str,
        name: str,
        description: Optional[str] = None,
        tagline: Optional[str] = None,
        tags: Optional[List[str]] = None,
        group: Optional[str] = None,
        provenance: Optional[Dict[str, Any]] = None,
        visibility: bool = None,
        default_tag: str = "*",
        default_priority: PriorityEnum = PriorityEnum.normal,
        extras: Optional[Dict[str, Any]] = None,
        owner_group: Optional[str] = None,
        existing_ok: bool = False,
        **kwargs,  # For deprecated parameters
    ) -> BaseDataset:

        # TODO - DEPRECATED - Remove eventually
        if group is not None:
            self._logger.warning(f"'group' parameter has been deprecated and will be removed in a future version")
        if visibility is not None:
            self._logger.warning(f"'visibility' parameter has been deprecated and will be removed in a future version")

        if description is None:
            description = ""
        if tagline is None:
            tagline = ""
        if tags is None:
            tags = []
        if provenance is None:
            provenance = {}
        if extras is None:
            extras = {}

        if "metadata" in kwargs:
            self._logger.warning(
                f"'metadata' parameter has been deprecated and will be removed in a future version. Use 'extras' instead"
            )
            extras.update(kwargs["metadata"])

        body = DatasetAddBody(
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
            provenance=provenance,
            default_tag=default_tag,
            default_priority=default_priority,
            extras=extras,
            owner_group=owner_group,
            existing_ok=existing_ok,
        )

        ds_id = self.make_request("post", f"api/v1/datasets/{dataset_type}", int, body=body)
        return self.get_dataset_by_id(ds_id)

    def delete_dataset(self, dataset_id: int, delete_records: bool):
        params = DatasetDeleteParams(delete_records=delete_records)
        return self.make_request("delete", f"api/v1/datasets/{dataset_id}", None, url_params=params)

    def clone_dataset(self, source_dataset_id: int, new_dataset_name: str):
        body = DatasetCloneBody(source_dataset_id=source_dataset_id, new_dataset_name=new_dataset_name)
        new_id = self.make_request("post", f"api/v1/datasets/clone", int, body=body)
        return self.get_dataset_by_id(new_id)

    ##############################################################
    # External files
    ##############################################################
    def download_external_file(self, file_id: int, destination_path: str, overwrite: bool = False) -> Tuple[int, str]:
        """
        Downloads an external file to the given path

        The file size and checksum will be checked against the metadata stored on the server

        Parameters
        ----------
        file_id
            ID of the file to obtain
        destination_path
            Full path to the destination file (including filename)
        overwrite
            If True, allow for overwriting an existing file. If False, and a file already exists at the given
            destination path, an exception will be raised.

        Returns
        -------
        :
            A tuple of file size and sha256 checksum.

        """
        meta_url = f"api/v1/external_files/{file_id}"
        download_url = f"api/v1/external_files/{file_id}/download"

        # Check for local file existence before doing any requests
        if os.path.exists(destination_path) and not overwrite:
            raise RuntimeError(f"File already exists at {destination_path}. To overwrite, use `overwrite=True`")

        # First, get the metadata
        file_info = self.make_request("get", meta_url, ExternalFile)

        # Now actually download the file
        file_size, file_sha256 = self.download_file(download_url, destination_path, overwrite=overwrite)

        if file_size != file_info.file_size:
            raise RuntimeError(f"Inconsistent file size. Expected {file_info.file_size}, got {file_size}")

        if file_sha256 != file_info.sha256sum:
            raise RuntimeError(f"Inconsistent file checksum. Expected {file_info.sha256sum}, got {file_sha256}")

        return file_size, file_sha256

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

        batch_size = self.api_limits["get_molecules"] // 4
        all_molecules = []

        for mol_id_batch in chunk_iterable(molecule_ids, batch_size):
            body = CommonBulkGetBody(ids=mol_id_batch, missing_ok=missing_ok)
            mol_batch = self.make_request("post", "api/v1/molecules/bulkGet", List[Optional[Molecule]], body=body)
            all_molecules.extend(mol_batch)

        if is_single:
            return all_molecules[0]
        else:
            return all_molecules

    def query_molecules(
        self,
        *,
        molecule_hash: Optional[Union[str, Iterable[str]]] = None,
        molecular_formula: Optional[Union[str, Iterable[str]]] = None,
        identifiers: Optional[Dict[str, Union[str, Iterable[str]]]] = None,
        limit: Optional[int] = None,
    ) -> MoleculeQueryIterator:
        """Query molecules by attributes.

        Do not rely on the returned molecules being in any particular order.

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

        mols = self.make_request(
            "post", "api/v1/molecules/bulkCreate", Tuple[InsertMetadata, List[int]], body=make_list(molecules)
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

        body = MoleculeModifyBody(
            name=name, comment=comment, identifiers=identifiers, overwrite_identifiers=overwrite_identifiers
        )

        return self.make_request("patch", f"api/v1/molecules/{molecule_id}", UpdateMetadata, body=body)

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

        return self.make_request("post", "api/v1/molecules/bulkDelete", DeleteMetadata, body=molecule_ids)

    ##############################################################
    # General record functions
    ##############################################################

    def _fetch_records(
        self,
        record_type: Optional[Type[_T]],
        record_ids: Sequence[int],
        missing_ok: bool = False,
        include: Optional[Iterable[str]] = None,
    ) -> List[Optional[_T]]:
        """
        Fetches records of a particular type with the specified IDs from the remove server.

        Records will be returned in the same order as the record ids. This function always returns a list.

        This function only fetches the top-level records - it does not fetch the children of the records. It also
        does not use caching at all.

        Parameters
        ----------
        record_type
            The type of record to fetch
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

        if not record_ids:
            return []

        if include is not None:
            # Always include the base stuff
            include = list(include) + ["*"]

        if record_type is None:
            endpoint = "api/v1/records/bulkGet"
        else:
            # A little hacky
            record_type_str = record_type.__fields__["record_type"].default
            endpoint = f"api/v1/records/{record_type_str}/bulkGet"

        max_batch_size = self.api_limits["get_records"]
        initial_batch_size = math.ceil(max_batch_size // 10)

        def _download_chunk(id_chunk: List[int]):
            body = CommonBulkGetBody(ids=id_chunk, include=include, missing_ok=missing_ok)
            return self.make_request("post", endpoint, List[Optional[Dict[str, Any]]], body=body)

        all_records = []
        for record_dicts in process_chunk_iterable(
            _download_chunk,
            record_ids,
            self.download_target_time,
            max_batch_size,
            initial_batch_size,
            self.n_download_threads,
            keep_order=True,
        ):
            if record_type is None:
                all_records.extend(records_from_dicts(record_dicts, self))
            else:
                all_records.extend([record_type(self, **r) if r is not None else None for r in record_dicts])

        # Just to really make sure the process_chunk_iterable code is correct
        assert all((x is None or x.id == rid) for x, rid in zip(all_records, record_ids))
        return all_records

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
            A list of records.  If missing_ok was specified, None will be substituted for a record
            that was not found.
        """

        return self._get_records_by_type(None, record_ids, missing_ok, include)

    def _get_records_by_type(
        self,
        record_type: Optional[Type[_T]],
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[_T], List[Optional[_T]]]:
        """
        Obtain records of a particular type with the specified IDs from the server.

        Records will be returned in the same order as the record ids.

        This function will fetch the children of the records if enough information
        is fetched of the parent record. This is handled by the various fetch_children_multi
        class functions of the record types.

        This function does not use the cache.

        Parameters
        ----------
        record_type
            The type of record to fetch
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
        all_records = self._fetch_records(record_type, record_ids, missing_ok, include)

        # We always force fetch here. Given that this record is not part of the cache, it shouldn't be using any
        # cache anyway. But the semantics of this function is that is always fetches everything
        if record_type is None:
            # Handle disparate record types
            record_groups = {}
            for r in all_records:
                if r is not None:
                    record_groups.setdefault(r.record_type, [])
                    record_groups[r.record_type].append(r)
            for v in record_groups.values():
                v[0].fetch_children_multi(v, include, force_fetch=True)
        else:
            record_type.fetch_children_multi(all_records, include, force_fetch=True)

        if is_single:
            return all_records[0]
        else:
            return all_records

    def query_records(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        record_type: Optional[Union[str, Iterable[str]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: int = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[BaseRecord]:
        """
        Query records of all types based on common fields

        This is a general query of all record types, so it can only filter by fields
        that are common among all records.

        Do not rely on the returned records being in any particular order.

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
        owner_user
            Query records owned by a user in the given list (usernames or IDs)
        owner_group
            Query records owned by a group in the given list (group names or IDS)
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
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = RecordQueryFilters(**filter_dict)

        return RecordQueryIterator[BaseRecord](self, filter_data, None, include)

    def reset_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Resets running or errored records to be waiting again
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.waiting)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

    def cancel_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Marks running, waiting, or errored records as cancelled

        A cancelled record will not be picked up by a manager.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.cancelled)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

    def invalidate_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Marks a completed record as invalid

        An invalid record is one that supposedly successfully completed. However, after review,
        is not correct.
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordModifyBody(record_ids=record_ids, status=RecordStatusEnum.invalid)
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

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

        body = RecordDeleteBody(record_ids=record_ids, soft_delete=soft_delete, delete_children=delete_children)
        return self.make_request("post", "api/v1/records/bulkDelete", DeleteMetadata, body=body)

    def uninvalidate_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Undo the invalidation of records
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.invalid)
        return self.make_request("post", "api/v1/records/revert", UpdateMetadata, body=body)

    def uncancel_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Undo the cancellation of records
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.cancelled)
        return self.make_request("post", "api/v1/records/revert", UpdateMetadata, body=body)

    def undelete_records(self, record_ids: Union[int, Sequence[int]]) -> UpdateMetadata:
        """
        Undo the (soft) deletion of records
        """

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()

        body = RecordRevertBody(record_ids=record_ids, revert_status=RecordStatusEnum.deleted)
        return self.make_request("post", "api/v1/records/revert", UpdateMetadata, body=body)

    def modify_records(
        self,
        record_ids: Union[int, Sequence[int]],
        new_compute_tag: Optional[str] = None,
        new_compute_priority: Optional[PriorityEnum] = None,
        **kwargs,  # For deprecated parameters
    ) -> UpdateMetadata:
        """
        Modify the compute tag or compute priority of a record
        """

        if "new_tag" in kwargs:
            self._logger.warning("'new_tag' is deprecated; use 'new_compute_tag' instead")
            new_compute_tag = kwargs["new_tag"]
        if "new_priority" in kwargs:
            self._logger.warning("'new_priority' is deprecated; use 'new_compute_priority' instead")
            new_compute_priority = kwargs["new_priority"]

        record_ids = make_list(record_ids)
        if not record_ids:
            return UpdateMetadata()
        if new_compute_tag is None and new_compute_priority is None:
            return UpdateMetadata()

        body = RecordModifyBody(
            record_ids=record_ids, compute_tag=new_compute_tag, compute_priority=new_compute_priority
        )
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body)

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
        return self.make_request("patch", "api/v1/records", UpdateMetadata, body=body_data)

    def get_waiting_reason(self, record_id: int) -> Dict[str, Any]:
        """
        Get the reason a record is in the waiting status

        The return is a dictionary, with a 'reason' key containing the overall reason the record is
        waiting. If appropriate, there is a 'details' key that contains information for each
        active compute manager on why that manager is not able to pick up the record's task.

        Parameters
        ----------
        record_id
            The record ID to test

        Returns
        -------
        :
            A dictionary containing information about why the record is not being picked up by compute managers
        """
        return self.make_request("get", f"api/v1/records/{record_id}/waiting_reason", Dict[str, Any])

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
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        body = SinglepointAddBody(**body_data)
        return self.make_request(
            "post", "api/v1/records/singlepoint/bulkCreate", Tuple[InsertMetadata, List[int]], body=body
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

        return self._get_records_by_type(SinglepointRecord, record_ids, missing_ok, include)

    def query_singlepoints(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        driver: Optional[Union[SinglepointDriver, Iterable[SinglepointDriver]]] = None,
        method: Optional[Union[str, Iterable[str]]] = None,
        basis: Optional[Union[str, Iterable[Optional[str]]]] = None,
        keywords: Optional[Union[Dict[str, Any], Iterable[Dict[str, Any]]]] = None,
        molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[SinglepointRecord]:
        """
        Queries singlepoint records on the server

        Do not rely on the returned records being in any particular order.

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
        keywords
            Query records with these keywords (exact match)
        molecule_id
            Query records whose molecule (id) is in the given list
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "keywords": make_list(keywords),
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = SinglepointQueryFilters(**filter_dict)
        return RecordQueryIterator[SinglepointRecord](self, filter_data, SinglepointRecord, include)

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
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        # If these are None, then let the pydantic models handle the defaults
        if keywords is not None:
            body_data["specification"]["keywords"] = keywords
        if protocols is not None:
            body_data["specification"]["protocols"] = protocols

        body_data = OptimizationAddBody(**body_data)

        return self.make_request(
            "post",
            "api/v1/records/optimization/bulkCreate",
            Tuple[InsertMetadata, List[int]],
            body=body_data,
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

        return self._get_records_by_type(OptimizationRecord, record_ids, missing_ok, include)

    def query_optimizations(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        qc_program: Optional[Union[str, Iterable[str]]] = None,
        qc_method: Optional[Union[str, Iterable[str]]] = None,
        qc_basis: Optional[Union[str, Iterable[Optional[str]]]] = None,
        initial_molecule_id: Optional[Union[int, Iterable[int]]] = None,
        final_molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[OptimizationRecord]:
        """
        Queries optimization records on the server

        Do not rely on the returned records being in any particular order.

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
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = OptimizationQueryFilters(**filter_dict)
        return RecordQueryIterator[OptimizationRecord](self, filter_data, OptimizationRecord, include)

    ##############################################################
    # Torsiondrive calculations
    ##############################################################

    def add_torsiondrives(
        self,
        initial_molecules: List[List[Union[int, Molecule]]],
        program: str,
        optimization_specification: OptimizationSpecification,
        keywords: Union[TorsiondriveKeywords, Dict[str, Any]],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        body = TorsiondriveAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/torsiondrive/bulkCreate", Tuple[InsertMetadata, List[int]], body=body
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

        return self._get_records_by_type(TorsiondriveRecord, record_ids, missing_ok, include)

    def query_torsiondrives(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        optimization_program: Optional[Union[str, Iterable[str]]] = None,
        qc_program: Optional[Union[str, Iterable[str]]] = None,
        qc_method: Optional[Union[str, Iterable[str]]] = None,
        qc_basis: Optional[Union[str, Iterable[str]]] = None,
        initial_molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[TorsiondriveRecord]:
        """
        Queries torsiondrive records on the server

        Do not rely on the returned records being in any particular order.

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
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = TorsiondriveQueryFilters(**filter_dict)
        return RecordQueryIterator[TorsiondriveRecord](self, filter_data, TorsiondriveRecord, include)

    ##############################################################
    # Grid optimization calculations
    ##############################################################

    def add_gridoptimizations(
        self,
        initial_molecules: Union[int, Molecule, Sequence[Union[int, Molecule]]],
        program: str,
        optimization_specification: OptimizationSpecification,
        keywords: Union[GridoptimizationKeywords, Dict[str, Any]],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        body = GridoptimizationAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/gridoptimization/bulkCreate", Tuple[InsertMetadata, List[int]], body=body
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

        return self._get_records_by_type(GridoptimizationRecord, record_ids, missing_ok, include)

    def query_gridoptimizations(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        optimization_program: Optional[Union[str, Iterable[str]]] = None,
        qc_program: Optional[Union[str, Iterable[str]]] = None,
        qc_method: Optional[Union[str, Iterable[str]]] = None,
        qc_basis: Optional[Union[str, Iterable[Optional[str]]]] = None,
        initial_molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[GridoptimizationRecord]:
        """
        Queries gridoptimization records on the server

        Do not rely on the returned records being in any particular order.

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
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = GridoptimizationQueryFilters(**filter_dict)
        return RecordQueryIterator[GridoptimizationRecord](self, filter_data, GridoptimizationRecord, include)

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
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        body = ReactionAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/reaction/bulkCreate", Tuple[InsertMetadata, List[int]], body=body
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

        return self._get_records_by_type(ReactionRecord, record_ids, missing_ok, include)

    def query_reactions(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        optimization_program: Optional[Iterable[Optional[str]]] = None,
        qc_program: Optional[Union[str, Iterable[str]]] = None,
        qc_method: Optional[Union[str, Iterable[str]]] = None,
        qc_basis: Optional[Union[str, Iterable[str]]] = None,
        molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[ReactionRecord]:
        """
        Queries reaction records on the server

        Do not rely on the returned records being in any particular order.

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
        optimization_program
            Query records whose optimization program is in the given list
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        molecule_id
            Query reactions that contain a molecule (id) is in the given list
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = ReactionQueryFilters(**filter_dict)
        return RecordQueryIterator[ReactionRecord](self, filter_data, ReactionRecord, include)

    ##############################################################
    # Manybody calculations
    ##############################################################

    def add_manybodys(
        self,
        initial_molecules: Sequence[Union[int, Molecule]],
        program: str,
        levels: Dict[Union[int, Literal["supersystem"]], QCSpecification],
        bsse_correction: Union[BSSECorrectionEnum, Sequence[BSSECorrectionEnum]],
        keywords: Union[ManybodyKeywords, Dict[str, Any]],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
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
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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
                "levels": levels,
                "bsse_correction": make_list(bsse_correction),
                "keywords": keywords,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        body = ManybodyAddBody(**body_data)

        return self.make_request(
            "post", "api/v1/records/manybody/bulkCreate", Tuple[InsertMetadata, List[int]], body=body
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

        return self._get_records_by_type(ManybodyRecord, record_ids, missing_ok, include)

    def query_manybodys(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        qc_program: Optional[Union[str, Iterable[str]]] = None,
        qc_method: Optional[Union[str, Iterable[str]]] = None,
        qc_basis: Optional[Union[str, Iterable[str]]] = None,
        initial_molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[ManybodyRecord]:
        """
        Queries reaction records on the server

        Do not rely on the returned records being in any particular order.

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
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = ManybodyQueryFilters(**filter_dict)
        return RecordQueryIterator[ManybodyRecord](self, filter_data, ManybodyRecord, include)

    ##############################################################
    # NEB
    ##############################################################

    def add_nebs(
        self,
        initial_chains: List[List[Union[int, Molecule]]],
        program: str,
        singlepoint_specification: QCSpecification,
        optimization_specification: Optional[OptimizationSpecification],
        keywords: Union[NEBKeywords, Dict[str, Any]],
        compute_tag: str = "*",
        compute_priority: PriorityEnum = PriorityEnum.normal,
        owner_group: Optional[str] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
    ) -> Tuple[InsertMetadata, List[int]]:
        """
        Adds neb calculations to the server

        This checks if the calculations already exist in the database. If so, it returns
        the existing id, otherwise it will insert it and return the new id.

        This will add one record per set of molecules

        Parameters
        ----------
        initial_chains
            The initial chains to run the NEB calculations on . Each NEB calculation starts with a single
            chain (list of molecules), so this is a nested list
        program
            The program to run the neb computation with ("geometric")
        singlepoint_specification
            Specification of how each singlepoint (gradient/hessian) should be run
        optimization_specification
            Specification of how any optimizations of the torsiondrive should be run
        keywords
            The torsiondrive keywords for the computation
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority of the job (high, normal, low). Default is normal.
        owner_group
            Group with additional permission for these records
        find_existing
            If True, search for existing records and return those. If False, always add new records

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the input molecules
        """

        if "tag" in kwargs:
            self._logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            self._logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

        if not initial_chains:
            return InsertMetadata(), []

        body_data = {
            "initial_chains": initial_chains,
            "specification": {
                "program": program,
                "singlepoint_specification": singlepoint_specification,
                "optimization_specification": optimization_specification,
                "keywords": keywords,
            },
            "compute_tag": compute_tag,
            "compute_priority": compute_priority,
            "owner_group": owner_group,
            "find_existing": find_existing,
        }

        if len(body_data["initial_chains"]) > self.api_limits["add_records"]:
            raise RuntimeError(
                f"Cannot add {len(body_data['initial_chains'])} records - over the limit of {self.api_limits['add_records']}"
            )

        body = NEBAddBody(**body_data)

        return self.make_request(
            "post",
            "api/v1/records/neb/bulkCreate",
            Tuple[InsertMetadata, List[int]],
            body=body,
        )

    def get_nebs(
        self,
        record_ids: Union[int, Sequence[int]],
        missing_ok: bool = False,
        *,
        include: Optional[Iterable[str]] = None,
    ) -> Union[Optional[NEBRecord], List[Optional[NEBRecord]]]:
        """
        Obtain NEB records with the specified IDs.

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

        return self._get_records_by_type(NEBRecord, record_ids, missing_ok, include)

    def query_nebs(
        self,
        *,
        record_id: Optional[Union[int, Iterable[int]]] = None,
        manager_name: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        dataset_id: Optional[Union[int, Iterable[int]]] = None,
        parent_id: Optional[Union[int, Iterable[int]]] = None,
        child_id: Optional[Union[int, Iterable[int]]] = None,
        created_before: Optional[Union[datetime, str]] = None,
        created_after: Optional[Union[datetime, str]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        program: Optional[Union[str, Iterable[str]]] = None,
        qc_program: Optional[Union[str, Iterable[str]]] = None,
        qc_method: Optional[Union[str, Iterable[str]]] = None,
        qc_basis: Optional[Union[str, Iterable[str]]] = None,
        molecule_id: Optional[Union[int, Iterable[int]]] = None,
        owner_user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        owner_group: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
        include: Optional[Iterable[str]] = None,
    ) -> RecordQueryIterator[NEBRecord]:
        """
        Queries neb records from the server

        Do not rely on the returned records being in any particular order.

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
        qc_program
            Query records whose qc program is in the given list
        qc_method
            Query records whose method is in the given list
        qc_basis
            Query records whose basis is in the given list
        molecule_id
            Query records whose initial chains contain a molecule (id) that is in the given list
        owner_user
            Query records owned by a user in the given list
        owner_group
            Query records owned by a group in the given list
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
            "molecule_id": make_list(molecule_id),
            "created_before": created_before,
            "created_after": created_after,
            "modified_before": modified_before,
            "modified_after": modified_after,
            "owner_user": make_list(owner_user),
            "owner_group": make_list(owner_group),
            "limit": limit,
        }

        filter_data = NEBQueryFilters(**filter_dict)
        return RecordQueryIterator[NEBRecord](self, filter_data, NEBRecord, include)

    ##############################################################
    # Managers
    ##############################################################

    def get_managers(
        self,
        names: Union[str, Sequence[str]],
        missing_ok: bool = False,
    ) -> Union[Optional[ComputeManager], List[Optional[ComputeManager]]]:
        """Obtain manager information from the server with the specified names

        Parameters
        ----------
        names
            A manager name or list of names
        missing_ok
            If set to True, then missing managers will be tolerated, and the returned
            managers will contain None for the corresponding managers that were not found.

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

        body = CommonBulkGetNamesBody(names=names, missing_ok=missing_ok)

        managers = self.make_request("post", "api/v1/managers/bulkGet", List[Optional[ComputeManager]], body=body)

        for m in managers:
            if m is not None:
                m.propagate_client(self)

        if is_single:
            return managers[0]
        else:
            return managers

    def query_managers(
        self,
        *,
        manager_id: Optional[Union[int, Iterable[int]]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        cluster: Optional[Union[str, Iterable[str]]] = None,
        hostname: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        modified_before: Optional[Union[datetime, str]] = None,
        modified_after: Optional[Union[datetime, str]] = None,
        limit: Optional[int] = None,
    ) -> ManagerQueryIterator:
        """
        Queries manager information on the server

        Parameters
        ----------
        manager_id
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
            "manager_id": make_list(manager_id),
            "name": make_list(name),
            "cluster": make_list(cluster),
            "hostname": make_list(hostname),
            "status": make_list(status),
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
        }

        filter_data = ManagerQueryFilters(**filter_dict)
        return ManagerQueryIterator(self, filter_data)

    def query_access_log(
        self,
        *,
        module: Optional[Union[str, Iterable[str]]] = None,
        method: Optional[Union[str, Iterable[str]]] = None,
        before: Optional[Union[datetime, str]] = None,
        after: Optional[Union[datetime, str]] = None,
        user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        limit: Optional[int] = None,
    ) -> AccessLogQueryIterator:
        """
        Query the server access log

        This log contains information about who accessed the server, and when.

        Parameters
        ----------
        module
            Return log entries whose module is in the given list
        method
            Return log entries whose access_method is in the given list
        before
            Return log entries captured before the specified date/time
        after
            Return log entries captured after the specified date/time
        user
            User name or ID associated with the log entry
        limit
            The maximum number of log entries to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_data = AccessLogQueryFilters(
            module=make_list(module),
            method=make_list(method),
            before=before,
            after=after,
            user=make_list(user),
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

        body = DeleteBeforeDateBody(before=before)
        return self.make_request("post", "api/v1/access_logs/bulkDelete", int, body=body)

    def query_error_log(
        self,
        *,
        error_id: Optional[Union[int, Iterable[int]]] = None,
        user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        before: Optional[Union[datetime, str]] = None,
        after: Optional[Union[datetime, str]] = None,
        limit: Optional[int] = None,
    ) -> ErrorLogQueryIterator:
        """
        Query the server's internal error log

        This log contains internal errors that are not always passed to the user.

        Parameters
        ----------
        error_id
            Return error log entries whose id is in the list
        user
            Return error log entries whose user name or ID is in the list
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
            user=make_list(user),
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
        body = DeleteBeforeDateBody(before=before)
        return self.make_request("post", "api/v1/server_errors/bulkDelete", int, body=body)

    def get_internal_job(self, job_id: int) -> InternalJob:
        """
        Gets information about an internal job on the server
        """

        ij_dict = self.make_request("get", f"api/v1/internal_jobs/{job_id}", Dict[str, Any])
        return InternalJob(client=self, **ij_dict)

    def query_internal_jobs(
        self,
        *,
        job_id: Optional[int, Iterable[int]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        user: Optional[Union[int, str, Iterable[Union[int, str]]]] = None,
        runner_hostname: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[InternalJobStatusEnum, Iterable[InternalJobStatusEnum]]] = None,
        last_updated_before: Optional[Union[datetime, str]] = None,
        last_updated_after: Optional[Union[datetime, str]] = None,
        added_before: Optional[Union[datetime, str]] = None,
        added_after: Optional[Union[datetime, str]] = None,
        scheduled_before: Optional[Union[datetime, str]] = None,
        scheduled_after: Optional[Union[datetime, str]] = None,
        limit: Optional[int] = None,
    ) -> InternalJobQueryIterator:
        """
        Queries the internal job queue on the server

        Parameters
        ----------
        job_id
            ID assigned to the job
        name
            Queries jobs whose name is in the given list
        user
            User name or ID associated with the log entry
        runner_hostname
            Queries jobs that were run/are running on a given host
        status
            Queries jobs whose status is in the given list
        last_updated_before
            Query for jobs last updated before a certain time
        last_updated_after
            Query for jobs last updated after a certain time
        added_before
            Query for jobs added before a certain time
        added_after
            Query for jobs added after a certain time
        scheduled_before
            Query for jobs scheduled to run before a certain time
        scheduled_after
            Query for jobs scheduled to run after a certain time
        limit
            The maximum number of jobs to return. Note that the server limit is always obeyed.

        Returns
        -------
        :
            An iterator that can be used to retrieve the results of the query
        """

        filter_dict = {
            "job_id": make_list(job_id),
            "name": make_list(name),
            "runner_hostname": make_list(runner_hostname),
            "status": make_list(status),
            "user": make_list(user),
            "last_updated_before": last_updated_before,
            "last_updated_after": last_updated_after,
            "added_before": added_before,
            "added_after": added_after,
            "scheduled_before": scheduled_before,
            "scheduled_after": scheduled_after,
            "limit": limit,
        }

        filter_data = InternalJobQueryFilters(**filter_dict)
        return InternalJobQueryIterator(self, filter_data)

    def cancel_internal_job(self, job_id: int):
        """
        Cancels (to the best of our ability) an internal job
        """

        return self.make_request(
            "put", f"api/v1/internal_jobs/{job_id}/status", None, body=InternalJobStatusEnum.cancelled
        )

    def delete_internal_job(self, job_id: int):
        return self.make_request("delete", f"api/v1/internal_jobs/{job_id}", None)

    def query_access_summary(
        self,
        *,
        group_by: str = "day",
        before: Optional[Union[datetime, str]] = None,
        after: Optional[Union[datetime, str]] = None,
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

        url_params = AccessLogSummaryFilters(group_by=group_by, before=before, after=after)

        entries = self.make_request(
            "get", "api/v1/access_logs/summary", Dict[str, List[AccessLogSummaryEntry]], url_params=url_params
        )

        return AccessLogSummary(entries=entries)

    ##############################################################
    # User, group, & role management
    ##############################################################

    def list_roles(self) -> List[RoleInfo]:
        """
        List all user roles on the server
        """

        return self.make_request("get", "api/v1/roles", List[RoleInfo])

    def get_role(self, rolename: str) -> RoleInfo:
        """
        Get information about a role on the server
        """

        is_valid_rolename(rolename)
        return self.make_request("get", f"api/v1/roles/{rolename}", RoleInfo)

    def add_role(self, role_info: RoleInfo) -> None:
        """
        Adds a role with permissions to the server

        If not successful, an exception is raised.
        """

        is_valid_rolename(role_info.rolename)
        return self.make_request("post", "api/v1/roles", None, body=role_info)

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
        return self.make_request("put", f"api/v1/roles/{role_info.rolename}", RoleInfo, body=role_info)

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
        return self.make_request("delete", f"api/v1/roles/{rolename}", None)

    def list_groups(self) -> List[GroupInfo]:
        """
        List all user groups on the server
        """

        return self.make_request("get", "api/v1/groups", List[GroupInfo])

    def get_group(self, groupname_or_id: Union[int, str]) -> GroupInfo:
        """
        Get information about a group on the server
        """

        if isinstance(groupname_or_id, str):
            is_valid_groupname(groupname_or_id)

        return self.make_request("get", f"api/v1/groups/{groupname_or_id}", GroupInfo)

    def add_group(self, group_info: GroupInfo) -> None:
        """
        Adds a group with permissions to the server

        If not successful, an exception is raised.
        """

        if group_info.id is not None:
            raise RuntimeError("Cannot add group when group_info contains an id")

        return self.make_request("post", "api/v1/groups", None, body=group_info)

    def delete_group(self, groupname_or_id: Union[int, str]):
        """
        Deletes a group on the server

        Deleted groups will be removed from all users groups list
        """

        if isinstance(groupname_or_id, str):
            is_valid_groupname(groupname_or_id)

        return self.make_request("delete", f"api/v1/groups/{groupname_or_id}", None)

    def list_users(self) -> List[UserInfo]:
        """
        List all user roles on the server
        """

        return self.make_request("get", "api/v1/users", List[UserInfo])

    def get_user(self, username_or_id: Optional[Union[int, str]] = None) -> UserInfo:
        """
        Get information about a user on the server

        If the username is not supplied, then info about the currently logged-in user is obtained.

        Parameters
        ----------
        username_or_id
            The username or ID to get info about

        Returns
        -------
        :
            Information about the user
        """

        if username_or_id is None:
            username_or_id = self.username

        if isinstance(username_or_id, str):
            is_valid_username(username_or_id)

        if username_or_id is None:
            raise RuntimeError("Cannot get user - not logged in?")

        return self.make_request("get", f"api/v1/users/{username_or_id}", UserInfo)

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

        if password is not None:
            is_valid_password(password)

        if user_info.id is not None:
            raise RuntimeError("Cannot add user when user_info contains an id")

        return self.make_request("post", "api/v1/users", str, body=(user_info, password))

    def modify_user(self, user_info: UserInfo) -> UserInfo:
        """
        Modifies a user on the server

        The user is determined by the id field of the input UserInfo, although the id
        and username are checked for consistency.

        Depending on the current user's permissions, some fields may not be updatable.

        Parameters
        ----------
        user_info
            Updated information for a user

        Returns
        -------
        :
            The updated user information as it appears on the server
        """

        return self.make_request("patch", f"api/v1/users", UserInfo, body=user_info)

    def change_user_password(
        self, username_or_id: Optional[Union[int, str]] = None, new_password: Optional[str] = None
    ) -> str:
        """
        Change a users password

        If the username is not specified, then the current logged-in user is used.

        If the password is not specified, then one is automatically generated by the server.

        Parameters
        ----------
        username_or_id
            The name or ID of the user whose password to change. If None, then use the currently logged-in user
        new_password
            Password to change to. If None, let the server generate one.

        Returns
        -------
        :
            The new password (either the same as the supplied one, or the server generated one
        """

        if username_or_id is None:
            username_or_id = self.username

        if username_or_id is None:
            raise RuntimeError("Cannot change user - not logged in?")

        if not isinstance(username_or_id, int):
            is_valid_username(username_or_id)

        if new_password is not None:
            is_valid_password(new_password)

        return self.make_request(
            "put", f"api/v1/users/{username_or_id}/password", str, body_model=Optional[str], body=new_password
        )

    def delete_user(self, username_or_id: Union[int, str]) -> None:
        """
        Delete a user from the server
        """

        if not isinstance(username_or_id, int):
            is_valid_username(username_or_id)

        return self.make_request("delete", f"api/v1/users/{username_or_id}", None)
