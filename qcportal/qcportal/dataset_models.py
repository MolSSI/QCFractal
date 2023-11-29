from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any, List, Iterable, Type, Tuple, Union, Callable, ClassVar, Sequence

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
from qcportal.dataset_view import DatasetViewWrapper
from qcportal.metadata_models import DeleteMetadata
from qcportal.metadata_models import InsertMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, BaseRecord
from qcportal.utils import make_list, chunk_iterable


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
    entry_names_: List[str] = Field([], alias="entry_names")

    # To be overridden by the derived class with more specific types
    specifications_: Dict[str, Any] = {}
    entries_: Dict[str, Any] = {}
    record_map_: Dict[Tuple[str, str], Any] = {}

    # Values computed outside QCA
    contributed_values_: Optional[Dict[str, ContributedValues]] = None

    #############################
    # Private non-pydantic fields
    #############################
    _client: Any = PrivateAttr(None)
    _view_data: Optional[DatasetViewWrapper] = PrivateAttr(None)

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

    def __init__(self, client=None, view_data=None, **kwargs):
        BaseModel.__init__(self, **kwargs)

        # Calls derived class propagate_client
        # which should filter down to the ones in this (BaseDataset) class
        self.propagate_client(client)

        assert self._client is client, "Client not set in base dataset class?"

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
        for record in self.record_map_.values():
            record.populate_client(client)

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

        n_batches = len(entries) // 1000 + 1
        all_meta: List[InsertMetadata] = []
        for entry_batch in tqdm(chunk_iterable(entries, 1000), total=n_batches, disable=None):
            meta = self._client.make_request("post", uri, InsertMetadata, body=entry_batch)

            # If entry names have been fetched, add the new entry names
            # This should still be ok if there are no entries - they will be fetched if the list is empty
            if self.entry_names_:
                self.entry_names_.extend(x.name for x in entry_batch if x not in self.entry_names_)

            all_meta.append(meta)

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

        # TODO - very inefficient for lots of specs
        self.fetch_specifications()

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

        n_batches = len(entry_names) // 1000 + 1
        for spec in specification_names:
            for entry_batch in tqdm(chunk_iterable(entry_names, 1000), total=n_batches, disable=None):
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
            raise RuntimeError("Dataset does not connected to a QCFractal server")

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

    @property
    def is_view(self) -> bool:
        return self._view_data is not None

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
    @property
    def specifications(self) -> Dict[str, Any]:
        if self.is_view:
            return self._view_data.get_specifications(self._specification_type)
        else:
            if not self.specifications_:
                self.fetch_specifications()

            return self.specifications_

    @property
    def specification_names(self) -> List[str]:
        return list(self.specifications.keys())

    def fetch_specifications(self) -> None:
        """
        Fetch all specifications from the remote server

        These are fetched and then stored internally, and not returned.
        """
        self.assert_is_not_view()
        self.assert_online()

        self.specifications_ = self._client.make_request(
            "get",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/specifications",
            Dict[str, self._specification_type],
        )

    def rename_specification(self, old_name: str, new_name: str):
        self.assert_is_not_view()
        self.assert_online()

        name_map = {old_name: new_name}

        self._client.make_request(
            "patch", f"api/v1/datasets/{self.dataset_type}/{self.id}/specifications", None, body=name_map
        )

        self.specifications_ = {name_map.get(k, k): v for k, v in self.specifications_.items()}

        # Renames the specifications in the record map
        self.record_map_ = {(e, name_map.get(s, s)): r for (e, s), r in self.record_map_.items()}

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
        self.specifications_.pop(name, None)
        self.record_map_ = {(e, s): r for (e, s), r in self.record_map_.items() if s != name}

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

        self.entry_names_ = self._client.make_request(
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

        self.entries_.update(fetched_entries)

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
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        # Strip out existing entries if we aren't forcing refetching
        if not force_refetch:
            entry_names = [x for x in entry_names if x not in self.entries_]

        batch_size: int = self._client.api_limits["get_dataset_entries"] // 4
        for entry_names_batch in chunk_iterable(entry_names, batch_size):
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

        if self.is_view:
            entry_dict = self._view_data.get_entries(self._entry_type, [entry_name])
            return entry_dict.get(entry_name)
        else:
            self.fetch_entries(entry_name, force_refetch=force_refetch)
            return self.entries_.get(entry_name, None)

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
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        if self.is_view:
            # Go one at a time. No need to "fetch"
            for entry_name in entry_names:
                entry = self.get_entry(entry_name)
                if entry is not None:
                    yield entry
        else:
            # Fetch from server
            batch_size: int = self._client.api_limits["get_dataset_entries"] // 4

            if self.entries_ is None:
                self.entries_ = {}

            for entry_names_batch in chunk_iterable(entry_names, batch_size):
                # If forcing refetching, then use the whole batch. Otherwise, strip out
                # any existing entries
                if force_refetch:
                    names_tofetch = entry_names_batch
                else:
                    names_tofetch = [x for x in entry_names_batch if x not in self.entries_]

                self._internal_fetch_entries(names_tofetch)

                # Loop over the whole batch (not just what we fetched)
                for entry_name in entry_names_batch:
                    entry = self.entries_.get(entry_name, None)

                    if entry is not None:
                        yield entry

    @property
    def entry_names(self) -> List[str]:
        if self.is_view:
            return self._view_data.get_entry_names()

        if not self.entry_names_:
            self.fetch_entry_names()

        return self.entry_names_

    def rename_entries(self, name_map: Dict[str, str]):
        self.assert_is_not_view()
        self.assert_online()

        self._client.make_request(
            "patch", f"api/v1/datasets/{self.dataset_type}/{self.id}/entries", None, body=name_map
        )

        # rename locally cached entries and stuff
        self.entry_names_ = [name_map.get(x, x) for x in self.entry_names_]
        self.entries_ = {name_map.get(k, k): v for k, v in self.entries_.items()}

        # Renames the entries in the record map
        self.record_map_ = {(name_map.get(e, e), s): r for (e, s), r in self.record_map_.items()}

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
        self.entry_names_ = [x for x in self.entry_names_ if x not in names]
        self.entries_ = {x: y for x, y in self.entries_.items() if x not in names}
        self.record_map_ = {(e, s): r for (e, s), r in self.record_map_.items() if e not in names}

        return ret

    ###########################
    # Records
    ###########################
    def _lookup_record(self, entry_name: str, specification_name: str):
        return self.record_map_.get((entry_name, specification_name), None)

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

        body = DatasetFetchRecordsBody(entry_names=entry_names, specification_names=specification_names, status=status)

        record_info = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/bulkFetch",
            List[Tuple[str, str, int]],  # (entry_name, spec_name, record_id)
            body=body,
        )

        record_ids = [x[2] for x in record_info]
        records = self._client.get_records(record_ids, include=include)

        # Update the locally-stored records
        for rec_item, rec in zip(record_info, records):
            assert rec_item[2] == rec.id
            self.record_map_[(rec_item[0], rec_item[1])] = rec

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

        # Get all the record ids that we store that correspond to the entries/specs
        existing_record_info = [
            (e, s, r) for (e, s), r in self.record_map_.items() if e in entry_names and s in specification_names
        ]

        # Subset that we should check for updated records on the server
        # (completed and invalid rarely change)
        updateable_record_ids = [
            r.id
            for e, s, r in existing_record_info
            if r.status not in (RecordStatusEnum.complete, RecordStatusEnum.invalid)
        ]

        # Do a raw call to the records/bulkGet endpoint. This allows us to only get
        # the 'modified_on' and 'status' fields
        batch_size = self._client.api_limits["get_records"] // 4
        minfo_dict: Dict[int, datetime] = {}  # record_id -> modified time

        for record_id_batch in chunk_iterable(updateable_record_ids, batch_size):
            body = CommonBulkGetBody(ids=record_id_batch, include=["modified_on", "status"])

            modified_info = self._client.make_request(
                "post",
                f"api/v1/records/bulkGet",
                List[Dict[str, Any]],
                body=body,
            )

            # Too lazy to look up how pydantic stores datetime, so use pydantic to parse it
            for m in modified_info:
                # Only store if the status on the server matches what the caller wants
                if status is None or m["status"] in status:
                    minfo_dict[m["id"]] = pydantic.parse_obj_as(datetime, m["modified_on"])

        # Which ones need to be fully updated
        need_updating = []
        for entry_name, spec_name, existing_record in existing_record_info:
            server_mtime = minfo_dict.get(existing_record.id, None)

            # Perhaps a status mismatch (status on server isn't one we want)
            if server_mtime is None:
                continue

            if existing_record.modified_on < server_mtime:
                need_updating.append((entry_name, spec_name))

        # Go via one spec at a time
        for spec_name in specification_names:
            entries_to_update = [x[0] for x in need_updating if x[1] == spec_name]

            for entries_to_update_batch in chunk_iterable(entries_to_update, batch_size):
                self._internal_fetch_records(entries_to_update_batch, [spec_name], None, include)

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
        batch_size: int = self._client.api_limits["get_records"] // 4

        # Do all entries for one spec. This simplifies things, especially with handling
        # existing or update-able records
        for spec_name in specification_names:
            for entry_names_batch in chunk_iterable(entry_names, batch_size):
                # Handle existing records that need to be updated
                if fetch_updated and not force_refetch:
                    self._internal_update_records(entry_names_batch, [spec_name], status, include)

                # Prune records that already exist, and then fetch them
                if not force_refetch:
                    entry_names_batch = [x for x in entry_names_batch if (x, spec_name) not in self.record_map_]

                if entry_names_batch:
                    self._internal_fetch_records(entry_names_batch, [spec_name], status, include)

    def get_record(
        self,
        entry_name: str,
        specification_name: str,
        include: Optional[Iterable[str]] = None,
        force_refetch: bool = False,
    ) -> Optional[BaseRecord]:
        """
        Obtain a calculation record related to this dataset

        The record will be automatically fetched from the remote server if needed.
        """

        if self.is_view:
            record_item = self._view_data.get_record_item(self._record_item_type, entry_name, specification_name)
            if record_item is None:
                return None
            else:
                return record_item.record
        else:
            self.fetch_records(entry_name, specification_name, include=include, force_refetch=force_refetch)
            return self._lookup_record(entry_name, specification_name)

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

        # Get an up-to-date list of entry names and specifications
        # Nothing to fetch if this is a view
        if force_refetch and not self.is_view:
            self.fetch_entry_names()
            self.fetch_specifications()

        status = make_list(status)

        # if not specified, do all entries and specs
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        if specification_names is None:
            specification_names = self.specification_names
        else:
            specification_names = make_list(specification_names)

        if self.is_view:
            for spec_name in specification_names:
                for entry_name in entry_names:
                    rec = self.get_record(entry_name, spec_name)

                    if rec is None:
                        continue

                    if status is None or rec.status in status:
                        yield entry_name, spec_name, rec
        else:
            # Smaller fetch limit for iteration (than in fetch_records)
            fetch_limit: int = self._client.api_limits["get_records"] // 10

            n_entries = len(entry_names)

            for spec_name in specification_names:
                for start_idx in range(0, n_entries, fetch_limit):
                    entries_batch = entry_names[start_idx : start_idx + fetch_limit]

                    # Handle existing records that need to be updated
                    if fetch_updated and not force_refetch:
                        existing_batch = [x for x in entries_batch if (x, spec_name) in self.record_map_]
                        self._internal_update_records(existing_batch, [spec_name], status, include)

                    if force_refetch:
                        batch_tofetch = entries_batch
                    else:
                        # Filter if they already exist
                        batch_tofetch = [x for x in entries_batch if (x, spec_name) not in self.record_map_]

                    self._internal_fetch_records(batch_tofetch, [spec_name], status, include)

                    # Now lookup the just-fetched records and yield them
                    for entry_name in entries_batch:
                        rec = self._lookup_record(entry_name, spec_name)
                        if rec is None:
                            continue

                        if status is None or rec.status in status:
                            yield entry_name, spec_name, rec

    def remove_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        delete_records: bool = False,
    ) -> DeleteMetadata:
        self.assert_is_not_view()
        self.assert_online()

        body = DatasetRemoveRecordsBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            delete_records=delete_records,
        )

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/bulkDelete",
            None,
            body=body,
        )

        # Delete locally-cached stuff
        self.record_map_ = {
            (e, s): r for (e, s), r in self.record_map_.items() if e not in entry_names and s not in specification_names
        }

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

        body = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
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

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

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

        body = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            status=RecordStatusEnum.waiting,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

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

        body = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            status=RecordStatusEnum.cancelled,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

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

        body = DatasetRecordRevertBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            revert_status=RecordStatusEnum.cancelled,
        )

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/revert",
            None,
            body=body,
        )

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

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

        body = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            status=RecordStatusEnum.invalid,
        )

        ret = self._client.make_request(
            "patch",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
            None,
            body=body,
        )

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

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

        body = DatasetRecordRevertBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            revert_status=RecordStatusEnum.invalid,
        )

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/records/revert",
            None,
            body=body,
        )

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

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
    # Contributed values
    ##############################

    def fetch_contributed_values(self):
        self.assert_is_not_view()
        self.assert_online()

        self.contributed_values_ = self._client.make_request(
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


def dataset_from_dict(data: Dict[str, Any], client: Any, view_data: Optional[DatasetViewWrapper] = None) -> BaseDataset:
    """
    Create a dataset object from a datamodel

    This determines the appropriate dataset class (deriving from BaseDataset)
    and creates an instance of that class.

    This works if the data is a datamodel object already or a dictionary
    """

    dataset_type = data["dataset_type"]
    cls = BaseDataset.get_subclass(dataset_type)
    return cls(client, view_data, **data)


def load_dataset_view(view_path: str):
    view_data = DatasetViewWrapper(view_path=view_path)
    raw_data = view_data.get_datamodel()

    return dataset_from_dict(raw_data, client=None, view_data=view_data)
