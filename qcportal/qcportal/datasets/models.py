from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any, List, Iterable, Type, Tuple, Union, Callable

import pandas as pd
import pydantic
from pydantic import BaseModel, Extra, validator
from qcelemental.models.types import Array

from qcportal.base_models import RestModelBase, validate_list_to_single
from qcportal.metadata_models import UpdateMetadata, DeleteMetadata
from qcportal.records import AllRecordTypes, PriorityEnum, RecordStatusEnum, record_from_datamodel
from qcportal.utils import make_list


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
    class _DataModel(BaseModel):
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

        metadata: Dict[str, Any]
        extras: Dict[str, Any]

        ########################################
        # Info about entries, specs, and records
        ########################################
        entry_names: List[str] = []

        # To be overridden by the derived class with more specific types
        specifications: Dict[str, Any] = {}
        entries: Dict[str, Any] = {}
        record_map: Dict[Tuple[str, str], Any] = {}

        # Values computed outside QCA
        contributed_values: Optional[Dict[str, ContributedValues]] = None

    client: Any
    raw_data: _DataModel  # Meant to be overridden by derived classes

    # Some dataset options
    auto_fetch_missing: bool = True  # Automatically fetch missing records from the server

    # To be overridden by the derived classes
    dataset_type: str
    _entry_type: Optional[Type] = None
    _specification_type: Optional[Type] = None
    _record_item_type: Optional[Type] = None
    _record_type: Optional[Type] = None

    @classmethod
    def from_datamodel(cls, raw_data: _DataModel, client: Any = None):
        """
        Create a dataset from a dataset DataModel
        """

        return cls(client=client, raw_data=raw_data, dataset_type=raw_data.dataset_type)

    def _post_add_entries(self, entry_names) -> None:
        """
        Perform some actions after entries have been added to the remote server

        Parameters
        ----------
        entry_names
            Names of the new entries that have been added to the remote server
        """

        self.raw_data.entry_names.extend(x for x in entry_names if x not in self.raw_data.entry_names)

    def _post_add_specification(self, specification_name) -> None:
        """
        Perform some actions after specifications have been added to the remote server

        Parameters
        ----------
        specification_name
            Name of the new specification that has been added to the remote server
        """

        # Ignoring the function argument for now... Just get all specs
        self.fetch_specifications()

    def _update_metadata(self, **kwargs):
        self.assert_online()

        new_body = {
            "name": self.raw_data.name,
            "description": self.raw_data.description,
            "tagline": self.raw_data.tagline,
            "tags": self.raw_data.tags,
            "group": self.raw_data.group,
            "visibility": self.raw_data.visibility,
            "provenance": self.raw_data.provenance,
            "default_tag": self.raw_data.default_tag,
            "default_priority": self.raw_data.default_priority,
            "metadata": self.raw_data.metadata,
        }

        new_body.update(**kwargs)

        self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}",
            DatasetModifyMetadata,
            None,
            None,
            new_body,
            None,
        )

        self.raw_data = self.raw_data.copy(update=new_body)

    def submit(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        tag: Optional[str] = None,
        priority: PriorityEnum = None,
    ):
        self.assert_online()

        body_data = DatasetSubmitBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            tag=tag,
            priority=priority,
        )

        ret = self.client._auto_request(
            "post", f"v1/datasets/{self.dataset_type}/{self.id}/submit", DatasetSubmitBody, None, Any, body_data, None
        )

        return ret

    #########################################
    # Various properties and getters/setters
    #########################################

    def status(self) -> Dict[str, Any]:
        self.assert_online()

        return self.client._auto_request(
            "get",
            f"v1/datasets/{self.dataset_type}/{self.id}/status",
            None,
            None,
            Dict[str, Dict[RecordStatusEnum, int]],
            None,
            None,
        )

    def detailed_status(self) -> List[Tuple[str, str, RecordStatusEnum]]:
        self.assert_online()

        return self.client._auto_request(
            "get",
            f"v1/datasets/{self.dataset_type}/{self.id}/detailed_status",
            None,
            None,
            List[Tuple[str, str, RecordStatusEnum]],
            None,
            None,
        )

    @property
    def offline(self) -> bool:
        return self.client is None

    def assert_online(self):
        if self.offline:
            raise RuntimeError("Dataset does not connected to a QCFractal server")

    @property
    def id(self) -> int:
        return self.raw_data.id

    @property
    def name(self) -> str:
        return self.raw_data.name

    def set_name(self, new_name: str):
        self._update_metadata(name=new_name)

    @property
    def description(self) -> str:
        return self.raw_data.description

    def set_description(self, new_description: str):
        self._update_metadata(description=new_description)

    @property
    def visibility(self) -> bool:
        return self.raw_data.visibility

    def set_visibility(self, new_visibility: bool):
        self._update_metadata(visibility=new_visibility)

    @property
    def group(self) -> str:
        return self.raw_data.group

    def set_group(self, new_group: str):
        self._update_metadata(group=new_group)

    @property
    def tags(self) -> List[str]:
        return self.raw_data.tags

    def set_tags(self, new_tags: List[str]):
        self._update_metadata(tags=new_tags)

    @property
    def tagline(self) -> str:
        return self.raw_data.tagline

    def set_tagline(self, new_tagline: str):
        self._update_metadata(tagline=new_tagline)

    @property
    def provenance(self) -> Dict[str, Any]:
        return self.raw_data.provenance

    def set_provenance(self, new_provenance: Dict[str, Any]):
        self._update_metadata(provenance=new_provenance)

    @property
    def metadata(self) -> Dict[str, Any]:
        return self.raw_data.metadata

    def set_metadata(self, new_metadata: Dict[str, Any]):
        self._update_metadata(metadata=new_metadata)

    @property
    def default_tag(self) -> Optional[str]:
        return self.raw_data.default_tag

    def set_default_tag(self, new_default_tag: str):
        self._update_metadata(default_tag=new_default_tag)

    @property
    def default_priority(self) -> PriorityEnum:
        return self.raw_data.default_priority

    def set_default_priority(self, new_default_priority: PriorityEnum):
        self._update_metadata(default_priority=new_default_priority)

    @property
    def specifications(self) -> Optional[Dict[str, Any]]:
        if not self.raw_data.specifications:
            self.fetch_specifications()

        return self.raw_data.specifications

    ###################################
    # Specifications
    ###################################
    def fetch_specifications(self) -> None:
        """
        Fetch all specifications from the remote server

        These are fetched and then stored internally, and not returned.
        """
        if self.offline:
            return

        self.raw_data.specifications = self.client._auto_request(
            "get",
            f"v1/datasets/{self.dataset_type}/{self.id}/specifications",
            None,
            None,
            Dict[str, self._specification_type],
            None,
            None,
        )

    def rename_specification(self, old_name: str, new_name: str) -> UpdateMetadata:
        self.assert_online()

        name_map = {old_name: new_name}

        self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}/specifications",
            Dict[str, str],
            None,
            None,
            name_map,
            None,
        )

        self.raw_data.specifications = {name_map.get(k, k): v for k, v in self.raw_data.specifications.items()}

        # Renames the specifications in the record map
        self.raw_data.record_map = {(e, name_map.get(s, s)): r for (e, s), r in self.raw_data.record_map.items()}

    def delete_specification(self, name: str, delete_records: bool = False) -> DeleteMetadata:
        self.assert_online()

        body_data = DatasetDeleteStrBody(names=[name], delete_records=delete_records)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/specifications/bulkDelete",
            DatasetDeleteStrBody,
            None,
            DeleteMetadata,
            body_data,
            None,
        )

        # Delete locally-cached stuff
        self.raw_data.specifications.pop(name, None)
        self.raw_data.record_map = {(e, s): r for (e, s), r in self.raw_data.record_map.items() if s != name}

        return ret

    ###################################
    # Entries
    ###################################
    def fetch_entry_names(self) -> None:
        """
        Fetch all entry names from the remote server

        These are fetched and then stored internally, and not returned.
        """
        if self.offline:
            return

        self.raw_data.entry_names = self.client._auto_request(
            "get",
            f"v1/datasets/{self.dataset_type}/{self.id}/entry_names",
            None,
            None,
            List[str],
            None,
            None,
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
        api_include
            Additional fields/data to include when fetch the entry (the 'raw' fields used by the web API)
        """

        if not entry_names:
            return

        body_data = DatasetFetchEntryBody(names=entry_names)

        fetched_entries = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/entries/bulkFetch",
            DatasetFetchEntryBody,
            None,
            Dict[str, self._entry_type],
            body_data,
            None,
        )

        self.raw_data.entries.update(fetched_entries)

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
        include
            Additional fields/data to include when fetching the entry
        force_refetch
            If true, fetch data from the server even if it already exists locally
        """

        if self.offline:
            return

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
            entry_names = [x for x in entry_names if x not in self.raw_data.entries]

        fetch_limit: int = self.client.api_limits["get_dataset_entries"] // 4
        n_entries = len(entry_names)

        for start_idx in range(0, n_entries, fetch_limit):
            entries_batch = entry_names[start_idx : start_idx + fetch_limit]
            self._internal_fetch_entries(entries_batch)

    def get_entry(
        self,
        entry_name: str,
        force_refetch: bool = False,
    ) -> Optional[Any]:
        """
        Obtain entry information

        The entry will be automatically fetched from the remote server if needed.
        """

        self.fetch_entries(entry_name, force_refetch=force_refetch)
        return self.raw_data.entries.get(entry_name, None)

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
        if force_refetch:
            self.fetch_entry_names()

        # if not specified, do all entries
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        fetch_limit: int = self.client.api_limits["get_dataset_entries"] // 4
        n_entries = len(entry_names)

        if self.raw_data.entries is None:
            self.raw_data.entries = {}

        for start_idx in range(0, n_entries, fetch_limit):
            names_batch = entry_names[start_idx : start_idx + fetch_limit]

            # If forcing refetching, then use the whole batch. Otherwise, strip out
            # any existing entries
            if force_refetch:
                names_tofetch = names_batch
            else:
                names_tofetch = [x for x in names_batch if x not in self.raw_data.entries]

            self._internal_fetch_entries(names_tofetch)

            # Loop over the whole batch (not just what we fetched)
            for entry_name in names_batch:
                entry = self.raw_data.entries.get(entry_name, None)

                if entry is not None:
                    yield entry

    @property
    def entry_names(self) -> List[str]:
        if not self.raw_data.entry_names:
            self.fetch_entry_names()

        return self.raw_data.entry_names

    def rename_entries(self, name_map: Dict[str, str]):
        ret = self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}/entries",
            Dict[str, str],
            None,
            None,
            name_map,
            None,
        )
        self.assert_online()

        # rename locally cached entries and stuff
        self.raw_data.entry_names = [name_map.get(x, x) for x in self.raw_data.entry_names]
        self.raw_data.entries = {name_map.get(k, k): v for k, v in self.raw_data.entries.items()}

        # Renames the entries in the record map
        self.raw_data.record_map = {(name_map.get(e, e), s): r for (e, s), r in self.raw_data.record_map.items()}

    def delete_entries(self, names: Union[str, Iterable[str]], delete_records: bool = False) -> DeleteMetadata:
        self.assert_online()

        names = make_list(names)
        body_data = DatasetDeleteStrBody(names=names, delete_records=delete_records)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/entries/bulkDelete",
            DatasetDeleteStrBody,
            None,
            DeleteMetadata,
            body_data,
            None,
        )

        # Delete locally-cached stuff
        self.raw_data.entry_names = [x for x in self.raw_data.entry_names if x not in names]
        self.raw_data.entries = {x: y for x, y in self.raw_data.entries.items() if x not in names}
        self.raw_data.record_map = {(e, s): r for (e, s), r in self.raw_data.record_map.items() if e not in names}

        return ret

    ###########################
    # Records
    ###########################
    def _lookup_record(self, entry_name: str, specification_name: str):
        return self.raw_data.record_map.get((entry_name, specification_name), None)

    def _internal_fetch_records(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]],
        api_include: Optional[Iterable[str]],
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
        api_include
            Additional fields/data to include when fetch the entry (the 'raw' fields used by the web API)
        """

        if not (entry_names and specification_names):
            return

        body_data = DatasetFetchRecordsBody(
            entry_names=entry_names, specification_names=specification_names, status=status, include=api_include
        )

        record_info = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/records/bulkFetch",
            DatasetFetchRecordsBody,
            None,
            List[self._record_item_type],
            body_data,
            None,
        )

        # Update the locally-stored records
        for rec_item in record_info:
            record = record_from_datamodel(rec_item.record, self.client)
            self.raw_data.record_map[(rec_item.entry_name, rec_item.specification_name)] = record

    def _internal_update_records(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]],
        api_include: Optional[Iterable[str]],
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
            Fetch only records with these statuses (only records with the given status on the server will be fetched)
        api_include
            Additional fields/data to include when fetch the entry (the 'raw' fields used by the web API)
        """

        if not (entry_names and specification_names):
            return

        # Get modified_on field of all the records
        body_data = DatasetFetchRecordsBody(
            entry_names=entry_names,
            specification_names=specification_names,
            status=status,
            include=["modified_on"],
        )

        modified_info = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/records/bulkFetch",
            DatasetFetchRecordsBody,
            None,
            List[Dict[str, Any]],
            body_data,
            None,
        )

        # Which ones need to be updated
        need_updating = []
        for minfo in modified_info:
            entry_name = minfo["entry_name"]
            spec_name = minfo["specification_name"]
            existing_record = self.raw_data.record_map.get((entry_name, spec_name), None)

            # Too lazy to look up how pydantic stores datetime, so use pydantic to parse it
            minfo_mtime = pydantic.parse_obj_as(datetime, minfo["record"]["modified_on"])

            # It's expected that existing_record is not None (ie, that the record had been downloaded already)
            # But handle this edge case anyway
            if existing_record is None or existing_record.modified_on < minfo_mtime:
                need_updating.append((entry_name, spec_name))

        # Go via one spec at a time
        for spec_name in specification_names:
            entries_to_update = [x[0] for x in need_updating if x[1] == spec_name]
            self._internal_fetch_records(entries_to_update, [spec_name], None, api_include)

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

        if self.offline:
            return

        # Reload entry names if we are forcing refetching
        if force_refetch:
            self.fetch_entry_names()
            self.fetch_specifications()

        status = make_list(status)
        api_include = self._record_type.transform_includes(include)

        # if not specified, do all entries and specs
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        if specification_names is None:
            specification_names = list(self.specifications.keys())
        else:
            specification_names = make_list(specification_names)

        # Determine the number of entries in each batch
        # Assume there are many more entries than specifications, and that
        # everything has been submitted
        # Divide by 4 to go easy on the server
        fetch_limit: int = self.client.api_limits["get_records"] // 4

        n_entries = len(entry_names)

        # Do all entries for one spec. This simplifies things, especially with handling
        # existing or update-able records
        for spec_name in specification_names:
            for start_idx in range(0, n_entries, fetch_limit):
                entries_batch = entry_names[start_idx : start_idx + fetch_limit]

                # Handle existing records that need to be updated
                if fetch_updated and not force_refetch:
                    existing_batch = [x for x in entries_batch if (x, spec_name) in self.raw_data.record_map]
                    self._internal_update_records(existing_batch, [spec_name], status, api_include)

                # Prune records that already exist, and then fetch them
                if not force_refetch:
                    entries_batch = [x for x in entries_batch if (x, spec_name) not in self.raw_data.record_map]

                self._internal_fetch_records(entries_batch, [spec_name], status, api_include)

    def get_record(
        self,
        entry_name: str,
        specification_name: str,
        include: Optional[Iterable[str]] = None,
        force_refetch: bool = False,
    ) -> AllRecordTypes:
        """
        Obtain a calculation record related to this dataset

        The record will be automatically fetched from the remote server if needed.
        """

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
        if force_refetch:
            self.fetch_entry_names()
            self.fetch_specifications()

        status = make_list(status)
        api_include = self._record_type.transform_includes(include)

        # if not specified, do all entries and specs
        if entry_names is None:
            entry_names = self.entry_names
        else:
            entry_names = make_list(entry_names)

        if specification_names is None:
            specification_names = list(self.specifications.keys())
        else:
            specification_names = make_list(specification_names)

        # Smaller fetch limit for iteration (than in fetch_records)
        fetch_limit: int = self.client.api_limits["get_records"] // 10

        n_entries = len(entry_names)

        for spec_name in specification_names:
            for start_idx in range(0, n_entries, fetch_limit):
                entries_batch = entry_names[start_idx : start_idx + fetch_limit]

                # Handle existing records that need to be updated
                if fetch_updated and not force_refetch:
                    existing_batch = [x for x in entries_batch if (x, spec_name) in self.raw_data.record_map]
                    self._internal_update_records(existing_batch, [spec_name], status, api_include)

                if force_refetch:
                    batch_tofetch = entries_batch
                else:
                    # Filter if they already exist
                    batch_tofetch = [x for x in entries_batch if (x, spec_name) not in self.raw_data.record_map]

                self._internal_fetch_records(batch_tofetch, [spec_name], status, api_include)

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
        self.assert_online()

        body_data = DatasetRemoveRecordsBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            delete_records=delete_records,
        )

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/records/bulkDelete",
            DatasetRemoveRecordsBody,
            None,
            None,
            body_data,
            None,
        )

        # Delete locally-cached stuff
        self.raw_data.record_map = {
            (e, s): r
            for (e, s), r in self.raw_data.record_map.items()
            if e not in entry_names and s not in specification_names
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
        self.assert_online()

        body_data = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            tag=new_tag,
            priority=new_priority,
            comment=new_comment,
        )

        ret = self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}/records",
            DatasetRecordModifyBody,
            None,
            None,
            body_data,
            None,
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
        self.assert_online()

        body_data = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            status=RecordStatusEnum.waiting,
        )

        ret = self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}/records",
            DatasetRecordModifyBody,
            None,
            None,
            body_data,
            None,
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
        self.assert_online()

        body_data = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            status=RecordStatusEnum.cancelled,
        )

        ret = self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}/records",
            DatasetRecordModifyBody,
            None,
            None,
            body_data,
            None,
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
        self.assert_online()

        body_data = DatasetRecordRevertBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            revert_status=RecordStatusEnum.cancelled,
        )

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/records/revert",
            DatasetRecordRevertBody,
            None,
            None,
            body_data,
            None,
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
        self.assert_online()

        body_data = DatasetRecordModifyBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            status=RecordStatusEnum.invalid,
        )

        ret = self.client._auto_request(
            "patch",
            f"v1/datasets/{self.dataset_type}/{self.id}/records",
            DatasetRecordModifyBody,
            None,
            None,
            body_data,
            None,
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
        self.assert_online()

        body_data = DatasetRecordRevertBody(
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            revert_status=RecordStatusEnum.invalid,
        )

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/{self.dataset_type}/{self.id}/records/revert",
            DatasetRecordRevertBody,
            None,
            None,
            body_data,
            None,
        )

        if refetch_records:
            self.fetch_records(entry_names, specification_names)

        return ret

    def compile_values(
        self,
        value_call: Callable,
        value_name: str,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        include: Optional[Iterable[str]] = None,
        fetch_updated: bool = True,
        force_refetch: bool = False,
    ) -> pd.DataFrame:
        def _data_generator():
            for entry_name, spec_name, record in self.iterate_records(
                entry_names=entry_names,
                specification_names=specification_names,
                status=RecordStatusEnum.complete,
                include=include,
                fetch_updated=fetch_updated,
                force_refetch=force_refetch,
            ):
                yield entry_name, spec_name, value_call(record)

        df = pd.DataFrame(_data_generator(), columns=("entry", "specification", value_name))

        return df.pivot(index="entry", columns="specification", values=value_name)

    ##############################
    # Contributed values
    ##############################

    def fetch_contributed_values(self):
        if self.offline:
            return

        self.raw_data.contributed_values = self.client._auto_request(
            "get",
            f"v1/datasets/{self.id}/contributed_values",
            None,
            None,
            Optional[Dict[str, ContributedValues]],
            None,
            None,
        )

    @property
    def contributed_values(self) -> Dict[str, ContributedValues]:
        if not self.raw_data.contributed_values:
            self.fetch_contributed_values()

        return self.raw_data.contributed_values


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
    include: Optional[List[str]] = None
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
    include: Optional[List[str]] = None


class DatasetSubmitBody(RestModelBase):
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    tag: Optional[str] = None
    priority: Optional[PriorityEnum] = None


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
