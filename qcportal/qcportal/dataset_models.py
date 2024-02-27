from __future__ import annotations

import math
import os
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Optional,
    Dict,
    Any,
    List,
    Iterable,
    Type,
    Tuple,
    Union,
    Callable,
    ClassVar,
    Sequence,
    Mapping,
)

import pandas as pd

try:
    import pydantic.v1 as pydantic
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr, Field
except ImportError:
    import pydantic
    from pydantic import BaseModel, Extra, validator, PrivateAttr, Field
from qcelemental.models.types import Array
from tabulate import tabulate
from tqdm import tqdm

from qcportal.base_models import RestModelBase, validate_list_to_single, CommonBulkGetBody
from qcportal.metadata_models import DeleteMetadata
from qcportal.metadata_models import InsertMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, BaseRecord
from qcportal.utils import make_list, chunk_iterable
from qcportal.cache import DatasetCache, read_dataset_metadata

if TYPE_CHECKING:
    from qcportal.client import PortalClient


class Citation(BaseModel):
    """A literature citation."""

    class Config:
        extra = Extra.forbid
        allow_mutation = False

    acs_citation: Optional[str] = None  # hand-formatted citation in ACS style
    bibtex: Optional[str] = None  # bibtex blob for later use with bibtex-renderer
    doi: Optional[str] = None
    url: Optional[str] = None

    def to_acs(self) -> str:
        """Returns an ACS-formatted citation"""
        return self.acs_citation


class ContributedValues(BaseModel):
    class Config:
        extra = Extra.forbid
        allow_mutation = False

    name: str
    values: Any
    index: Array[str]
    values_structure: Dict[str, Any] = {}

    theory_level: Union[str, Dict[str, str]]
    units: str
    theory_level_details: Optional[Union[str, Dict[str, Optional[str]]]] = None

    citations: Optional[List[Citation]] = None
    external_url: Optional[str] = None
    doi: Optional[str] = None

    comments: Optional[str] = None


class BaseDataset(BaseModel):
    class Config:
        extra = Extra.forbid
        allow_mutation = True
        validate_assignment = True

    id: int
    dataset_type: str
    name: str
    description: str
    tagline: str
    tags: List[str]
    group: str
    visibility: bool
    provenance: Dict[str, Any]

    default_tag: str
    default_priority: PriorityEnum

    owner_user: Optional[str]
    owner_group: Optional[str]

    metadata: Dict[str, Any]
    extras: Dict[str, Any]

    ########################################
    # Caches of information
    ########################################
    _entry_names: List[str] = PrivateAttr([])
    _specification_names: List[str] = PrivateAttr([])

    # All local cache data. May be backed by memory or disk
    _cache_data: DatasetCache = PrivateAttr()

    # Values computed outside QCA
    _contributed_values: Optional[Dict[str, ContributedValues]] = PrivateAttr(None)

    #############################
    # Private non-pydantic fields
    #############################
    _client: Any = PrivateAttr(None)

    # To be overridden by the derived classes
    _entry_type: ClassVar[Optional[Type]] = None
    _new_entry_type: ClassVar[Optional[Type]] = None
    _specification_type: ClassVar[Optional[Type]] = None
    _record_item_type: ClassVar[Optional[Type]] = None
    _record_type: ClassVar[Optional[Type]] = None

    # A dictionary of all subclasses (dataset types) to the actual class type
    _all_subclasses: ClassVar[Dict[str, Type[BaseDataset]]] = {}

    # Some dataset options
    auto_fetch_missing: bool = True  # Automatically fetch missing records from the server

    def __init__(self, client: Optional[PortalClient] = None, cache_data: Optional[DatasetCache] = None, **kwargs):
        BaseModel.__init__(self, **kwargs)

        # Calls derived class propagate_client
        # which should filter down to the ones in this (BaseDataset) class
        self.propagate_client(client)

        assert self._client is client, "Client not set in base dataset class?"

        if cache_data is not None:
            self._cache_data = cache_data

        elif client and client.cache.enabled:
            cache_dir = client.cache.cache_dir
            cache_path = os.path.join(cache_dir, f"dataset_{self.id}.sqlite")
            self._cache_data = DatasetCache(cache_path, False, type(self))

            # All fields that aren't private
            self._cache_data.update_metadata("dataset_metadata", self)

            # Add metadata to cache file (in case the user wants to share it)
            if self._client is not None:
                # Only address, not username/password
                self._cache_data.update_metadata("client_address", self._client.address)
        else:
            self._cache_data = DatasetCache(None, False, type(self))

    def __init_subclass__(cls):
        """
        Register derived classes for later use
        """

        # Get the dataset type. This is kind of ugly, but works.
        # We could use ClassVar, but in my tests it doesn't work for
        # disambiguating (ie, via parse_obj_as)
        dataset_type = cls.__fields__["dataset_type"].default

        cls._all_subclasses[dataset_type] = cls

    @classmethod
    def get_subclass(cls, dataset_type: str):
        subcls = cls._all_subclasses.get(dataset_type)
        if subcls is None:
            raise RuntimeError(f"Cannot find subclass for record type {dataset_type}")
        return subcls

    def propagate_client(self, client):
        """
        Propagates a client to this record to any fields within this record that need it

        This may also be called from derived class propagate_client functions as well
        """
        self._client = client

    def _add_entries(self, entries: Union[BaseModel, Sequence[BaseModel]]) -> InsertMetadata:
        """
        Internal function for adding entries to a dataset

        This function handles batching and some type checking

        Parameters
        ----------
        entries
            Entries to add. May be just a single entry or a sequence of entries
        """

        entries = make_list(entries)
        if len(entries) == 0:
            return InsertMetadata()

        assert all(isinstance(x, self._new_entry_type) for x in entries), "Incorrect entry type"
        uri = f"api/v1/datasets/{self.dataset_type}/{self.id}/entries/bulkCreate"

        batch_size: int = math.ceil(self._client.api_limits["get_dataset_entries"] / 4)
        n_batches = math.ceil(len(entries) / batch_size)

        all_meta: List[InsertMetadata] = []
        for entry_batch in tqdm(chunk_iterable(entries, batch_size), total=n_batches, disable=None):
            meta = self._client.make_request("post", uri, InsertMetadata, body=entry_batch)

            # If entry names have been fetched, add the new entry names
            # This should still be ok if there are no entries - they will be fetched if the list is empty
            added_names = [x.name for x in entry_batch]

            all_meta.append(meta)
            self._internal_fetch_entries(added_names)

        return InsertMetadata.merge(all_meta)

    def _add_specifications(self, specifications: Union[BaseModel, Sequence[BaseModel]]) -> InsertMetadata:
        """
        Internal function for adding specifications to a dataset

        Parameters
        ----------
        specifications
            Specifications to add. May be just a single specification or a sequence of entries
        """

        specifications = make_list(specifications)
        if len(specifications) == 0:
            return InsertMetadata()

        assert all(isinstance(x, self._specification_type) for x in specifications), "Incorrect specification type"
        uri = f"api/v1/datasets/{self.dataset_type}/{self.id}/specifications"

        ret = self._client.make_request("post", uri, InsertMetadata, body=specifications)

        added_names = [x.name for x in specifications]
        self._internal_fetch_specifications(added_names)

        return ret

    def _update_metadata(self, **kwargs):
        self.assert_online()

        new_body = {
            "name": self.name,
            "description": self.description,
            "tagline": self.tagline,
            "tags": self.tags,
            "group": self.group,
            "visibility": self.visibility,
            "provenance": self.provenance,
            "default_tag": self.default_tag,
            "default_priority": self.default_priority,
            "metadata": self.metadata,
        }

        new_body.update(**kwargs)
        body = DatasetModifyMetadata(**new_body)
        self._client.make_request("patch", f"api/v1/datasets/{self.dataset_type}/{self.id}", None, body=body)

        self.name = body.name
        self.description = body.description
        self.tagline = body.tagline
        self.tags = body.tags
        self.group = body.group
        self.visibility = body.visibility
        self.provenance = body.provenance
        self.default_tag = body.default_tag
        self.default_priority = body.default_priority
        self.metadata = body.metadata

        self._cache_data.update_metadata("dataset_metadata", self)

    def submit(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        tag: Optional[str] = None,
        priority: PriorityEnum = None,
        find_existing: bool = True,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        # Do automatic batching here
        # (will be removed when we move to async)
        if entry_names is None:
            entry_names = self.entry_names
        if specification_names is None:
            specification_names = self.specification_names

        batch_size = math.ceil(self._client.api_limits["get_records"] / 4)
        n_batches = math.ceil(len(entry_names) / batch_size)

        for spec in specification_names:
            for entry_batch in tqdm(chunk_iterable(entry_names, batch_size), total=n_batches, disable=None):
                body_data = DatasetSubmitBody(
                    entry_names=entry_batch,
                    specification_names=[spec],
                    tag=tag,
                    priority=priority,
                    find_existing=find_existing,
                )

                self._client.make_request(
                    "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/submit", Any, body=body_data
                )

    #########################################
    # Various properties and getters/setters
    #########################################

    @property
    def is_view(self) -> bool:
        return self._cache_data is not None and self._cache_data.read_only

    def status(self) -> Dict[str, Any]:
        self.assert_online()

        return self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/status",
            Dict[str, Dict[RecordStatusEnum, int]],
        )

    def status_table(self) -> str:
        """
        Returns the status of the dataset's computations as a table (in a string)
        """

        ds_status = self.status()
        all_status = {x for y in ds_status.values() for x in y}
        ordered_status = RecordStatusEnum.make_ordered_status(all_status)
        headers = ["specification"] + [x.value for x in ordered_status]

        table = []
        for spec, spec_statuses in sorted(ds_status.items()):
            row = [spec]
            row.extend(spec_statuses.get(s, "") for s in ordered_status)
            table.append(row)

        return tabulate(table, headers=headers, stralign="right")

    def print_status(self) -> None:
        print(self.status_table())

    def detailed_status(self) -> List[Tuple[str, str, RecordStatusEnum]]:
        self.assert_online()

        return self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/detailed_status",
            List[Tuple[str, str, RecordStatusEnum]],
        )

    @property
    def offline(self) -> bool:
        return self._client is None

    def assert_online(self):
        if self.offline:
            raise RuntimeError("Dataset is not connected to a QCFractal server")

    @property
    def record_count(self) -> int:
        self.assert_online()

        return self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/record_count",
            int,
        )

    @property
    def computed_properties(self):
        self.assert_online()

        return self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/computed_properties",
            Dict[str, List[str]],
        )

    def assert_is_not_view(self):
        if self.is_view:
            raise RuntimeError("Dataset loaded from an offline view")

    def set_name(self, new_name: str):
        self._update_metadata(name=new_name)

    def set_description(self, new_description: str):
        self._update_metadata(description=new_description)

    def set_visibility(self, new_visibility: bool):
        self._update_metadata(visibility=new_visibility)

    def set_group(self, new_group: str):
        self._update_metadata(group=new_group)

    def set_tags(self, new_tags: List[str]):
        self._update_metadata(tags=new_tags)

    def set_tagline(self, new_tagline: str):
        self._update_metadata(tagline=new_tagline)

    def set_provenance(self, new_provenance: Dict[str, Any]):
        self._update_metadata(provenance=new_provenance)

    def set_metadata(self, new_metadata: Dict[str, Any]):
        self._update_metadata(metadata=new_metadata)

    def set_default_tag(self, new_default_tag: str):
        self._update_metadata(default_tag=new_default_tag)

    def set_default_priority(self, new_default_priority: PriorityEnum):
        self._update_metadata(default_priority=new_default_priority)

    ###################################
    # Specifications
    ###################################
    def fetch_specification_names(self) -> None:
        """
        Fetch all entry names from the remote server

        These are fetched and then stored internally, and not returned.
        """
        self.assert_is_not_view()
        self.assert_online()

        self._specification_names = self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/specification_names",
            List[str],
        )

    def _internal_fetch_specifications(
        self,
        specification_names: Iterable[str],
    ) -> None:
        """
        Fetches specification information from the remote server, storing it internally

        This does not do any checking for existing specifications, but is used to actually
        request the data from the server.

        Note: This function does not do any batching w.r.t. server API limits. It is expected that is done
        before this function is called.

        Parameters
        ----------
        specification_names
            Names of the specifications to fetch
        """

        if not specification_names:
            return

        fetched_specifications = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/specifications/bulkFetch",
            Dict[str, self._specification_type],
            body=DatasetFetchSpecificationBody(names=specification_names),
        )

        # The specifications contain their own names, so we don't need the keys
        self._cache_data.update_specifications(fetched_specifications.values())

        if self._specification_names is None:
            self._specification_names = list(fetched_specifications.keys())
        else:
            self._specification_names.extend(
                x for x in fetched_specifications.keys() if x not in self._specification_names
            )

    def fetch_specifications(
        self, specification_names: Optional[Union[str, Iterable[str]]] = None, force_refetch: bool = False
    ) -> None:
        """
        Fetch specifications from the remote server, storing them internally

        Parameters
        ----------
        specification_names
            Names of specifications to fetch. If None, fetch all specifications
        force_refetch
            If true, fetch data from the server even if it already exists locally
        """
        self.assert_is_not_view()
        self.assert_online()

        if force_refetch:
            self.fetch_specification_names()

        # we make copies because _internal_fetch_specifications modifies _specification_names
        if specification_names is None:
            specification_names = self.specification_names.copy()
        else:
            specification_names = make_list(specification_names).copy()

        # Strip out existing specifications if we aren't forcing refetching
        if force_refetch:
            specifications_tofetch = specification_names
        else:
            cached_specifications = set(self._cache_data.get_specification_names())
            specifications_tofetch = set(specification_names) - cached_specifications

        batch_size: int = math.ceil(self._client.api_limits["get_dataset_entries"] / 4)

        for specification_names_batch in chunk_iterable(specifications_tofetch, batch_size):
            self._internal_fetch_specifications(specification_names_batch)

    @property
    def specification_names(self) -> List[str]:
        if not self._specification_names:
            self._specification_names = self._cache_data.get_specification_names()

        if not self._specification_names and not self.is_view:
            self.fetch_specification_names()

        return self._specification_names

    @property
    def specifications(self) -> Mapping[str, Any]:
        specs = self._cache_data.get_all_specifications()

        if not specs and not self.is_view:
            self.fetch_specifications()
            specs = self._cache_data.get_all_specifications()

        return {s.name: s for s in specs}

    def rename_specification(self, old_name: str, new_name: str):
        self.assert_is_not_view()
        self.assert_online()

        if old_name == new_name:
            return

        name_map = {old_name: new_name}

        self._client.make_request(
            "patch", f"api/v1/datasets/{self.dataset_type}/{self.id}/specifications", None, body=name_map
        )

        # rename locally cached entries and stuff
        self._specification_names = [name_map.get(x, x) for x in self._specification_names]

        self._cache_data.rename_specification(old_name, new_name)

    def delete_specification(self, name: str, delete_records: bool = False) -> DeleteMetadata:
        self.assert_is_not_view()
        self.assert_online()

        body = DatasetDeleteStrBody(names=[name], delete_records=delete_records)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/specifications/bulkDelete",
            DeleteMetadata,
            body=body,
        )

        # Delete locally-cached stuff
        self._specification_names = [x for x in self._specification_names if x != name]
        self._cache_data.delete_specification(name)

        return ret

    ###################################
    # Entries
    ###################################
    def fetch_entry_names(self) -> None:
        """
        Fetch all entry names from the remote server

        These are fetched and then stored internally, and not returned.
        """
        self.assert_is_not_view()
        self.assert_online()

        self._entry_names = self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/entry_names",
            List[str],
        )

    def _internal_fetch_entries(
        self,
        entry_names: Iterable[str],
    ) -> None:
        """
        Fetches entry information from the remote server, storing it internally

        This does not do any checking for existing entries, but is used to actually
        request the data from the server.

        Note: This function does not do any batching w.r.t. server API limits. It is expected that is done
        before this function is called.

        Parameters
        ----------
        entry_names
            Names of the entries to fetch
        """

        if not entry_names:
            return

        body = DatasetFetchEntryBody(names=entry_names)

        fetched_entries = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/entries/bulkFetch",
            Dict[str, self._entry_type],
            body=body,
        )

        # The entries contain their own names, so we don't need the keys
        self._cache_data.update_entries(fetched_entries.values())

        if self._entry_names is None:
            self._entry_names = list(fetched_entries.keys())
        else:
            self._entry_names.extend(x for x in fetched_entries.keys() if x not in self._entry_names)

    def fetch_entries(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        force_refetch: bool = False,
    ) -> None:
        """
        Fetches entry information from the remote server, storing it internally

        By default, already-fetched entries will not be fetched again, unless
        `force_refetch` is True.

        Parameters
        ----------
        entry_names
            Names of entries to fetch. If None, fetch all entries
        force_refetch
            If true, fetch data from the server even if it already exists locally
        """

        self.assert_is_not_view()
        self.assert_online()

        # Reload entry names if we are forcing refetching
        if force_refetch:
            self.fetch_entry_names()

        # if not specified, do all entries
        # we make copies because _internal_fetch_entries modifies _entry_names
        if entry_names is None:
            entry_names = self.entry_names.copy()
        else:
            entry_names = make_list(entry_names).copy()

        # Strip out existing entries if we aren't forcing refetching
        if force_refetch:
            entries_tofetch = entry_names
        else:
            cached_entries = set(self._cache_data.get_entry_names())
            entries_tofetch = set(entry_names) - cached_entries

        batch_size: int = math.ceil(self._client.api_limits["get_dataset_entries"] / 4)

        for entry_names_batch in chunk_iterable(entries_tofetch, batch_size):
            self._internal_fetch_entries(entry_names_batch)

    def get_entry(
        self,
        entry_name: str,
        force_refetch: bool = False,
    ) -> Optional[Any]:
        """
        Obtain entry information

        The entry will be automatically fetched from the remote server if needed.
        """

        entry = self._cache_data.get_entry(entry_name)

        if entry is None and not self.is_view:
            self.fetch_entries(entry_name, force_refetch=force_refetch)
            entry = self._cache_data.get_entry(entry_name)

        return entry

    def iterate_entries(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        force_refetch: bool = False,
    ):
        """
        Iterate over all entries

        This is used as a generator, and automatically fetches entries as needed

        Parameters
        ----------
        entry_names
            Names of entries to iterate over. If None, iterate over all entries
        force_refetch
            If true, fetch data from the server even if it already exists locally
        """

        #########################################################
        # We duplicate a little bit of fetch_entries here, since
        # we want to yield in the middle
        #########################################################

        # Reload entry names if we are forcing refetching
        # Nothing to fetch if this is a view
        if force_refetch and not self.is_view:
            self.fetch_entry_names()

        # if not specified, do all entries
        # we make copies because fetching records can modify _entry_names
        if entry_names is None:
            entry_names = self.entry_names.copy()
        else:
            entry_names = make_list(entry_names).copy()

        if self.is_view:
            # Go one at a time. No need to "fetch"
            for entry_name in entry_names:
                entry = self._cache_data.get_entry(entry_name)
                if entry is not None:
                    yield entry
        else:
            # Check local cache, but fetch from server
            batch_size: int = math.ceil(self._client.api_limits["get_dataset_entries"] / 4)

            # What we have cached already
            cached_entries = set(self._cache_data.get_entry_names())

            for entry_names_batch in chunk_iterable(entry_names, batch_size):
                # If forcing refetching, then use the whole batch. Otherwise, strip out
                # any existing entries
                if force_refetch:
                    entries_tofetch = entry_names_batch
                else:
                    # get what we have in the local cache
                    entries_tofetch = set(entry_names_batch) - cached_entries

                if entries_tofetch:
                    self._internal_fetch_entries(entries_tofetch)

                # Loop over the whole batch (not just what we fetched)
                entry_data = self._cache_data.get_entries(entry_names_batch)

                for entry in entry_data:
                    yield entry

    @property
    def entry_names(self) -> List[str]:
        if not self._entry_names:
            self._entry_names = self._cache_data.get_entry_names()

        if not self._entry_names and not self.is_view:
            self.fetch_entry_names()

        return self._entry_names

    def rename_entries(self, name_map: Dict[str, str]):
        self.assert_is_not_view()
        self.assert_online()

        # Remove renames which aren't actually different
        name_map = {old_name: new_name for old_name, new_name in name_map.items() if old_name != new_name}

        self._client.make_request(
            "patch", f"api/v1/datasets/{self.dataset_type}/{self.id}/entries", None, body=name_map
        )

        # rename locally cached entries and stuff
        self._entry_names = [name_map.get(x, x) for x in self._entry_names]

        for old_name, new_name in name_map.items():
            self._cache_data.rename_entry(old_name, new_name)

    def delete_entries(self, names: Union[str, Iterable[str]], delete_records: bool = False) -> DeleteMetadata:
        self.assert_is_not_view()
        self.assert_online()

        names = make_list(names)
        body = DatasetDeleteStrBody(names=names, delete_records=delete_records)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/entries/bulkDelete",
            DeleteMetadata,
            body=body,
        )

        # Delete locally-cached stuff
        self._entry_names = [x for x in self._entry_names if x not in names]
        for entry_name in names:
            self._cache_data.delete_entry(entry_name)

        return ret

    ###########################
    # Records
    ###########################
    def _internal_fetch_records(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]],
        include: Optional[Iterable[str]],
    ) -> None:
        """
        Fetches record information from the remote server, storing it internally

        This does not do any checking for existing records, but is used to actually
        request the data from the server.

        Note: This function does not do any batching w.r.t. server API limits. It is expected that is done
        before this function is called.

        Parameters
        ----------
        entry_names
            Names of the entries whose records to fetch. If None, fetch all entries
        specification_names
            Names of the specifications whose records to fetch. If None, fetch all specifications
        status
            Fetch only records with these statuses
        include
            Additional fields/data to include when fetch the entry
        """

        if not (entry_names and specification_names):
            return

        # First, we need the corresponding entries and specifications
        self.fetch_entries(entry_names)
        self.fetch_specifications(specification_names)

        body = DatasetFetchRecordsBody(entry_names=entry_names, specification_names=specification_names, status=status)

        record_info = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/bulkFetch",
            List[Tuple[str, str, int]],  # (entry_name, spec_name, record_id)
            body=body,
        )

        record_ids = [x[2] for x in record_info]
        records = self._client._get_records_by_type(self._record_type, record_ids, include=include)

        # Update the locally-stored records
        # zip(record_info, records) = ((entry_name, spec_name, record_id), record)
        update_info = [(ename, sname, r) for (ename, sname, _), r in zip(record_info, records)]
        self._cache_data.update_dataset_records(update_info)

    def _internal_update_records(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]],
        include: Optional[Iterable[str]],
    ):
        """
        Update local record information if it has been modified on the server

        Note: This function does not do any batching w.r.t. server API limits. It is expected that is done
        before this function is called.

        Parameters
        ----------
        entry_names
            Names of the entries whose records to update. If None, fetch all entries
        specification_names
            Names of the specifications whose records to update. If None, fetch all specifications
        status
            Update records that have this status on the server. If None, update records with any status on the server
        include
            Additional fields/data to include when fetch the entry
        """

        if not (entry_names and specification_names):
            return

        # Only update records if the record in the local cache as one of the following statuses
        updateable_statuses = (RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.error)

        # Returns list of tuple (entry name, spec_name, id, status, modified_on) of records
        # we have our local cache
        updateable_record_info = self._cache_data.get_dataset_record_info(
            entry_names, specification_names, updateable_statuses
        )
        # print(f"UPDATEABLE RECORDS: {len(updateable_record_info)}")
        if not updateable_record_info:
            return

        batch_size = math.ceil(self._client.api_limits["get_records"] / 4)
        record_modified_map: Dict[int, datetime] = {}  # record_id -> modified time

        for record_info_batch in chunk_iterable(updateable_record_info, batch_size):
            record_id_batch = [x[2] for x in record_info_batch]

            # Do a raw call to the records/bulkGet endpoint. This allows us to only get
            # the 'modified_on' and 'status' fields
            server_record_info = self._client.make_request(
                "post",
                f"api/v1/records/bulkGet",
                List[Dict[str, Any]],
                body=CommonBulkGetBody(ids=record_id_batch, include=["id", "modified_on", "status"]),
            )

            # Too lazy to look up how pydantic stores datetime, so use pydantic to parse it
            for sri in server_record_info:
                # Only store if the status on the server matches what the caller wants
                if status is None or sri["status"] in status:
                    record_modified_map[sri["id"]] = pydantic.parse_obj_as(datetime, sri["modified_on"])

        # Which ones need to be fully updated
        need_updating: Dict[str, List[str]] = {}  # key is specification, value is list of entry names
        for entry_name, spec_name, record_id, _, local_mtime in updateable_record_info:
            server_mtime = record_modified_map.get(record_id, None)

            # Perhaps the record doesn't exist on the server anymore or something
            if server_mtime is None:
                continue

            if local_mtime < server_mtime:
                need_updating.setdefault(spec_name, [])
                need_updating[spec_name].append(entry_name)

        # Update from the server one spec at a time
        # print(f"Updated on server: {len(needs_updating)}")
        for spec_name, entries_to_update in need_updating.items():
            for entries_batch in chunk_iterable(entries_to_update, batch_size):
                self._internal_fetch_records(entries_batch, [spec_name], None, include)

    def fetch_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        include: Optional[Iterable[str]] = None,
        fetch_updated: bool = True,
        force_refetch: bool = False,
    ):
        """
        Fetches record information from the remote server, storing it internally

        By default, this function will only fetch records that have not been fetch previously.
        If `force_refetch` is True, then this will always fetch the records.

        Parameters
        ----------
        entry_names
            Names of the entries whose records to fetch. If None, fetch all entries
        specification_names
            Names of the specifications whose records to fetch. If None, fetch all specifications
        status
            Fetch only records with these statuses
        include
            Additional fields to include in the returned record
        fetch_updated
            Fetch any records that exist locally but have been updated on the server
        force_refetch
            If true, fetch data from the server even if it already exists locally
        """

        self.assert_is_not_view()
        self.assert_online()

        # Reload entry names if we are forcing refetching
        if force_refetch:
            self.fetch_entry_names()
            self.fetch_specifications()

        status = make_list(status)

        # if not specified, do all entries and specs
        # we make copies because fetching records can modify _specification_names and _entry_names members
        if entry_names is None:
            entry_names = self.entry_names.copy()
        else:
            entry_names = make_list(entry_names).copy()

        if specification_names is None:
            specification_names = self.specification_names.copy()
        else:
            specification_names = make_list(specification_names).copy()

        # Determine the number of entries in each batch
        # Assume there are many more entries than specifications, and that
        # everything has been submitted
        # Divide by 4 to go easy on the server
        batch_size: int = math.ceil(self._client.api_limits["get_records"] / 4)

        # Do all entries for one spec. This simplifies things, especially with handling
        # existing or update-able records
        for spec_name in specification_names:
            for entry_names_batch in chunk_iterable(entry_names, batch_size):
                # Handle existing records that need to be updated
                if fetch_updated and not force_refetch:
                    self._internal_update_records(entry_names_batch, [spec_name], status, include)

                if force_refetch:
                    batch_tofetch = entry_names_batch
                else:
                    # Filter if they already exist in the local cache
                    cached_records = self._cache_data.get_existing_dataset_records(entry_names_batch, [spec_name])
                    cached_record_entries = [x[0] for x in cached_records]
                    batch_tofetch = [x for x in entry_names_batch if x not in cached_record_entries]

                if batch_tofetch:
                    self._internal_fetch_records(entry_names_batch, [spec_name], status, include)

    def get_record(
        self,
        entry_name: str,
        specification_name: str,
        include: Optional[Iterable[str]] = None,
        fetch_updated: bool = True,
        force_refetch: bool = False,
    ) -> Optional[BaseRecord]:
        """
        Obtain a calculation record related to this dataset

        The record will be automatically fetched from the remote server if needed.
        If a record does not exist for this entry and specification, None is returned
        """

        if self.is_view:
            fetch_updated = False
            force_refetch = False

        if force_refetch:
            self._internal_fetch_records([entry_name], [specification_name], None, include)
        elif fetch_updated:
            self._internal_update_records([entry_name], [specification_name], None, include)

        record = self._cache_data.get_dataset_record(entry_name, specification_name)

        if record is None and not self.is_view:
            self.fetch_records(
                entry_name,
                specification_name,
                include=include,
                fetch_updated=fetch_updated,
                force_refetch=force_refetch,
            )
            record = self._cache_data.get_dataset_record(entry_name, specification_name)

        if record is not None and self._client is not None:
            record.propagate_client(self._client)

        return record

    def iterate_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        status: Optional[Union[RecordStatusEnum, Iterable[RecordStatusEnum]]] = None,
        include: Optional[Iterable[str]] = None,
        fetch_updated: bool = True,
        force_refetch: bool = False,
    ):
        #########################################################
        # We duplicate a little bit of fetch_records here, since
        # we want to yield in the middle
        #########################################################

        if self.is_view:
            fetch_updated = False
            force_refetch = False

        # Get an up-to-date list of entry names and specifications
        # Nothing to fetch if this is a view
        if force_refetch:
            self.fetch_entry_names()
            self.fetch_specifications()

        status = make_list(status)

        # if not specified, do all entries and specs
        # we make copies because fetching records can modify _specification_names and _entry_names members
        if entry_names is None:
            entry_names = self.entry_names.copy()
        else:
            entry_names = make_list(entry_names).copy()

        if specification_names is None:
            specification_names = self.specification_names.copy()
        else:
            specification_names = make_list(specification_names).copy()

        if self.is_view:
            for spec_name in specification_names:
                for entry_names_batch in chunk_iterable(entry_names, 125):
                    record_data = self._cache_data.get_dataset_records(entry_names_batch, [spec_name])

                    for e, s, r in record_data:
                        if status is None or r.status in status:
                            yield e, s, r
        else:
            # Smaller fetch limit for iteration (than in fetch_records)
            batch_size: int = math.ceil(self._client.api_limits["get_records"] / 10)

            for spec_name in specification_names:
                for entry_names_batch in chunk_iterable(entry_names, batch_size):
                    # Handle existing records that need to be updated
                    if fetch_updated and not force_refetch:
                        self._internal_update_records(entry_names_batch, [spec_name], status, include)

                    if force_refetch:
                        batch_tofetch = entry_names_batch
                    else:
                        # Filter if they already exist in the local cache
                        existing_records = self._cache_data.get_existing_dataset_records(entry_names_batch, [spec_name])
                        existing_entries = [x[0] for x in existing_records]
                        batch_tofetch = [x for x in entry_names_batch if x not in existing_entries]

                    if batch_tofetch:
                        self._internal_fetch_records(batch_tofetch, [spec_name], status, include)

                    # Now lookup the just-fetched records and yield them
                    record_data = self._cache_data.get_dataset_records(entry_names_batch, [spec_name])

                    if self._client is not None:
                        for _, _, r in record_data:
                            r.propagate_client(self._client)

                    for e, s, r in record_data:
                        if status is None or r.status in status:
                            yield e, s, r

    def remove_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        delete_records: bool = False,
    ) -> DeleteMetadata:
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRemoveRecordsBody(
            entry_names=entry_names,
            specification_names=specification_names,
            delete_records=delete_records,
        )

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/bulkDelete",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        return ret

    def modify_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        new_tag: Optional[str] = None,
        new_priority: Optional[PriorityEnum] = None,
        new_comment: Optional[str] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRecordModifyBody(
            entry_names=entry_names,
            specification_names=specification_names,
            tag=new_tag,
            priority=new_priority,
            comment=new_comment,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

        return ret

    def reset_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRecordModifyBody(
            entry_names=entry_names,
            specification_names=specification_names,
            status=RecordStatusEnum.waiting,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

        return ret

    def cancel_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRecordModifyBody(
            entry_names=entry_names,
            specification_names=specification_names,
            status=RecordStatusEnum.cancelled,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

        return ret

    def uncancel_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRecordRevertBody(
            entry_names=entry_names,
            specification_names=specification_names,
            revert_status=RecordStatusEnum.cancelled,
        )

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/revert",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

        return ret

    def invalidate_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRecordModifyBody(
            entry_names=entry_names,
            specification_names=specification_names,
            status=RecordStatusEnum.invalid,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

        return ret

    def uninvalidate_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        body = DatasetRecordRevertBody(
            entry_names=entry_names,
            specification_names=specification_names,
            revert_status=RecordStatusEnum.invalid,
        )

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/revert",
            None,
            body=body,
        )

        self._cache_data.delete_dataset_records(entry_names, specification_names)

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

        return ret

    def compile_values(
        self,
        value_call: Callable,
        value_names: Union[Sequence[str], str] = "value",
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        unpack: bool = False,
    ) -> pd.DataFrame:
        """
        Compile values from records into a pandas DataFrame.

        Parameters
        -----------
        value_call
            Function to call on each record to extract the desired value. Must return a scalar value or
            a sequence of values if 'unpack' is set to True.

        value_names
            Column name(s) for the extracted value(s). If a string is provided and multiple values are
            returned by 'value_call', columns are named by appending an index to this string. If a list
            of strings is provided, it must match the length of the sequence returned by 'value_call'.
            Default is "value".

        entry_names
            Entry names to filter records. If not provided, considers all entries.

        specification_names
            Specification names to filter records. If not provided, considers all specifications.

        unpack
            If True, unpack the sequence of values returned by 'value_call' into separate columns.
            Default is False.

        Returns
        --------
        pd.DataFrame
            A multi-index DataFrame where each row corresponds to an entry. Each column corresponds has a top level
            index as a specification, and a second level index as the appropriate value name.
            Values are extracted from records using 'value_call'.

        Raises
        -------
        ValueError
            If the length of 'value_names' does not match the number of values returned by 'value_call' when
            'unpack' is set to True.

        Notes
        ------
        1. The DataFrame is structured such that the rows are entries and columns are specifications.
        2. If 'unpack' is True, the function assumes 'value_call' returns a sequence of values that need
        to be distributed across columns in the resulting DataFrame. 'value_call' should always return the
        same number of values for each record if unpack is True.
        """

        def _data_generator(unpack=False):
            for entry_name, spec_name, record in self.iterate_records(
                entry_names=entry_names,
                specification_names=specification_names,
                status=RecordStatusEnum.complete,
                fetch_updated=True,
                force_refetch=False,
            ):
                if unpack:
                    yield entry_name, spec_name, *value_call(record)
                else:
                    yield entry_name, spec_name, value_call(record)

        def _check_first():
            gen = _data_generator()
            _, _, first_value = next(gen)

            return first_value

        first_value = _check_first()

        if unpack and isinstance(first_value, Sequence) and not isinstance(first_value, str):
            if isinstance(value_names, str):
                column_names = [value_names + str(i) for i in range(len(first_value))]
            else:
                if len(first_value) != len(value_names):
                    raise ValueError(
                        "Number of column names must match number of values returned by provided function."
                    )
                column_names = value_names

            df = pd.DataFrame(_data_generator(unpack=True), columns=("entry", "specification", *column_names))

        else:
            column_names = [value_names]
            df = pd.DataFrame(_data_generator(), columns=("entry", "specification", value_names))

        return_val = df.pivot(index="entry", columns="specification", values=column_names)

        # Make specification top level index.
        return return_val.swaplevel(axis=1)

    def get_properties_df(self, properties_list: Sequence[str]) -> pd.DataFrame:
        """
        Retrieve a DataFrame populated with the specified properties from dataset records.

        This function uses the provided list of property names to extract corresponding
        values from each record's properties. It returns a DataFrame where rows represent
        each record. Each column corresponds has a top level index as a specification,
        and a second level index as the appropriate value name. Columns with all
        NaN values are dropped.

        Parameters:
        -----------
        properties_list
            List of property names to retrieve from the records.

        Returns:
        --------
        pd.DataFrame
            A DataFrame populated with the specified properties for each record.
        """

        # create lambda function to get all properties at once
        extract_properties = lambda x: [x.properties.get(property_name) for property_name in properties_list]

        # retrieve values.
        result = self.compile_values(extract_properties, value_names=properties_list, unpack=True)

        # Drop columns with all nan  values. This will occur if a property that is not part of a
        # specification is requested.
        result.dropna(how="all", axis=1, inplace=True)
        return result

    ##############################
    # Caching
    ##############################
    def refresh_cache(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
    ):
        """
        Refreshes some information in the cache with information on the server

        This can be used to fix some inconsistencies in the cache without deleting and starting over.
        For example, this can fix instances where the record attached to a given entry & specification
        has changed (new record id) due to renaming specifications and entries, or via remove_records followed
        by a submit without duplicate checking.

        This will also fetch any updated records

        Parameters
        ----------
        entry_names
            Names of the entries whose records to fetch. If None, fetch all entries
        specification_names
            Names of the specifications whose records to fetch. If None, fetch all specifications
        """

        self.assert_is_not_view()
        self.assert_online()

        # Reload all entry names and specifications
        self.fetch_entry_names()
        self.fetch_specification_names()

        # Delete anything in the cache that doesn't correspond to these entries/specs
        local_specifications = self._cache_data.get_specification_names()
        local_entries = self._cache_data.get_entry_names()

        deleted_specifications = set(self.specification_names) - set(local_specifications)
        deleted_entries = set(self.entry_names) - set(local_entries)

        for spec_name in deleted_specifications:
            self._cache_data.delete_specification(spec_name)
        for entry_name in deleted_entries:
            self._cache_data.delete_entry(entry_name)

        ###############################
        # Now for the actual fetching
        # if not specified, do all entries and specs
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        if specification_names is None:
            specification_names = self.specification_names
        else:
            specification_names = make_list(specification_names)

        # Determine the number of entries in each batch
        # Assume there are many more entries than specifications, and that
        # everything has been submitted
        # Divide by 4 to go easy on the server
        batch_size: int = math.ceil(self._client.api_limits["get_records"] / 4)

        # Do all entries for one spec. This simplifies things, especially with handling
        # existing or update-able records
        for spec_name in specification_names:
            # Fetch the specification itself
            self.fetch_specifications(spec_name)

            for entry_names_batch in chunk_iterable(entry_names, batch_size):
                # Fetch the entries themselves
                self.fetch_entries(entry_names_batch, force_refetch=True)

                # What info do we have stored locally
                # (entry_name, spec_name, record_id)
                cached_records = self._cache_data.get_dataset_record_info(entry_names_batch, [spec_name], None)

                # Get the record info corresponding to this specification & these entries
                body = DatasetFetchRecordsBody(entry_names=entry_names_batch, specification_names=specification_names)

                server_ds_records = self._client.make_request(
                    "post",
                    f"api/v1/datasets/{self.dataset_type}/{self.id}/records/bulkFetch",
                    List[Tuple[str, str, int]],  # (entry_name, spec_name, record_id)
                    body=body,
                )

                # Also get basic information about the records themselves
                server_ds_records_map = {(e, s): rid for e, s, rid in server_ds_records}
                server_record_ids = list(set(server_ds_records_map.values()))

                # Do a raw call to the records/bulkGet endpoint. This allows us to only get
                # the 'modified_on' and 'status' fields
                server_record_info = self._client.make_request(
                    "post",
                    f"api/v1/records/bulkGet",
                    List[Dict[str, Any]],
                    body=CommonBulkGetBody(ids=server_record_ids, include=["modified_on", "status"]),
                )
                server_record_info_map = {r["id"]: r for r in server_record_info}

                # Check for any different record_ids, or for deleted records
                records_tofetch = []
                for ename, sname, record_id, status, modified_on in cached_records:
                    server_ds_record_id = server_ds_records_map.get((ename, sname), None)

                    # If record does not exist on the server or has a different id, delete it locally from the cache
                    if server_ds_record_id is None or record_id != server_ds_record_id:
                        self._cache_data.delete_dataset_record(ename, sname)
                        records_tofetch.append(server_ds_record_id)
                        continue

                    # This is guaranteed to exist, right?
                    rinfo = server_record_info_map[record_id]
                    rinfo_modified = pydantic.parse_obj_as(datetime, rinfo["mod"])
                    if rinfo_modified > modified_on or rinfo["status"] != status:
                        records_tofetch.append(record_id)  # same as server_ds_record_id

    ##############################
    # Contributed values
    ##############################

    def fetch_contributed_values(self):
        self.assert_is_not_view()
        self.assert_online()

        self._contributed_values = self._client.make_request(
            "get",
            f"api/v1/datasets/{self.id}/contributed_values",
            Optional[Dict[str, ContributedValues]],
        )

    @property
    def contributed_values(self) -> Dict[str, ContributedValues]:
        if not self.contributed_values:
            self.fetch_contributed_values()

        return self.contributed_values


class DatasetAddBody(RestModelBase):
    name: str
    description: str
    tagline: str
    tags: List[str]
    group: str
    provenance: Dict[str, Any]
    visibility: bool
    default_tag: str
    default_priority: PriorityEnum
    metadata: Dict[str, Any]
    owner_group: Optional[str]
    existing_ok: bool = False


class DatasetModifyMetadata(RestModelBase):
    name: str
    description: str
    tags: List[str]
    tagline: str
    group: str
    visibility: bool
    provenance: Optional[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]]

    default_tag: str
    default_priority: PriorityEnum


class DatasetQueryModel(RestModelBase):
    dataset_type: Optional[str] = None
    dataset_name: Optional[str] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class DatasetFetchSpecificationBody(RestModelBase):
    names: List[str]
    missing_ok: bool = False


class DatasetFetchEntryBody(RestModelBase):
    names: List[str]
    missing_ok: bool = False


class DatasetDeleteStrBody(RestModelBase):
    names: List[str]
    delete_records: bool = False


class DatasetRemoveRecordsBody(RestModelBase):
    entry_names: List[str]
    specification_names: List[str]
    delete_records: bool = False


class DatasetDeleteParams(RestModelBase):
    delete_records: bool = False

    @validator("delete_records", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class DatasetFetchRecordsBody(RestModelBase):
    entry_names: List[str]
    specification_names: List[str]
    status: Optional[List[RecordStatusEnum]] = None


class DatasetSubmitBody(RestModelBase):
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    tag: Optional[str] = None
    priority: Optional[PriorityEnum] = None
    owner_group: Optional[str] = None
    find_existing: bool = True


class DatasetRecordModifyBody(RestModelBase):
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    status: Optional[RecordStatusEnum] = None
    priority: Optional[PriorityEnum] = None
    tag: Optional[str] = None
    comment: Optional[str] = None


class DatasetRecordRevertBody(RestModelBase):
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    revert_status: RecordStatusEnum = None


class DatasetQueryRecords(RestModelBase):
    record_id: List[int]
    dataset_type: Optional[List[str]] = None


class DatasetDeleteEntryBody(RestModelBase):
    names: List[str]
    delete_records: bool = False


class DatasetDeleteSpecificationBody(RestModelBase):
    names: List[str]
    delete_records: bool = False


def dataset_from_dict(data: Dict[str, Any], client: Any, cache_data: Optional[DatasetCache] = None) -> BaseDataset:
    """
    Create a dataset object from a datamodel

    This determines the appropriate dataset class (deriving from BaseDataset)
    and creates an instance of that class.

    This works if the data is a datamodel object already or a dictionary
    """

    dataset_type = data["dataset_type"]
    cls = BaseDataset.get_subclass(dataset_type)
    return cls(client=client, cache_data=cache_data, **data)


def dataset_from_cache(file_path: str) -> BaseDataset:
    # Reads this as a read-only "view"
    ds_meta = read_dataset_metadata(file_path)
    ds_type = BaseDataset.get_subclass(ds_meta["dataset_type"])
    ds_cache = DatasetCache(file_path, True, ds_type)

    # Views never have a client attached
    return dataset_from_dict(ds_meta, None, cache_data=ds_cache)
