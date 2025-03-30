from __future__ import annotations

import logging
import math
import os
from datetime import datetime
from enum import Enum
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

try:
    import pydantic.v1 as pydantic
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr, Field, root_validator
except ImportError:
    import pydantic
    from pydantic import BaseModel, Extra, validator, PrivateAttr, Field, root_validator
from qcelemental.models.types import Array
from tabulate import tabulate
from tqdm import tqdm

from qcportal.base_models import RestModelBase, validate_list_to_single, CommonBulkGetBody
from qcportal.internal_jobs import InternalJob, InternalJobStatusEnum
from qcportal.metadata_models import DeleteMetadata, InsertMetadata, InsertCountsMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, BaseRecord
from qcportal.utils import make_list, chunk_iterable
from qcportal.cache import DatasetCache, read_dataset_metadata, get_records_with_cache
from qcportal.external_files import ExternalFile

if TYPE_CHECKING:
    from qcportal.client import PortalClient
    from pandas import DataFrame


class DatasetAttachmentType(str, Enum):
    """
    The type of attachment a file is for a dataset
    """

    other = "other"
    view = "view"


class DatasetAttachment(ExternalFile):
    attachment_type: DatasetAttachmentType


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
    provenance: Dict[str, Any]
    extras: Dict[str, Any]

    default_compute_tag: str
    default_compute_priority: PriorityEnum

    owner_user: Optional[str]
    owner_group: Optional[str]

    ########################################
    # Caches of information
    ########################################
    _entry_names: List[str] = PrivateAttr([])
    _specification_names: List[str] = PrivateAttr([])

    # All local cache data. May be backed by memory or disk
    _cache_data: DatasetCache = PrivateAttr()

    ######################################################
    # Fields not always included when fetching the dataset
    ######################################################
    contributed_values_: Optional[Dict[str, ContributedValues]] = Field(None, alias="contributed_values")
    attachments_: Optional[List[DatasetAttachment]] = Field(None, alias="attachments")

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

        # TODO - DEPRECATED - remove eventually
        if "group" in kwargs:
            del kwargs["group"]
        if "visibility" in kwargs:
            del kwargs["visibility"]
        if "metadata" in kwargs:
            kwargs["extras"] = kwargs.pop("metadata")
        if "default_tag" in kwargs:
            kwargs["default_compute_tag"] = kwargs.pop("default_tag")
        if "default_priority" in kwargs:
            kwargs["default_compute_priority"] = kwargs.pop("default_priority")

        BaseModel.__init__(self, **kwargs)

        # Calls derived class propagate_client
        # which should filter down to the ones in this (BaseDataset) class
        self.propagate_client(client)

        assert self._client is client, "Client not set in base dataset class?"

        if cache_data is not None:
            # Passed in cache data. That takes priority
            self._cache_data = cache_data
        elif self._client:
            # Ask the client cache for our cache
            self._cache_data = client.cache.get_dataset_cache(self.id, type(self))
        else:
            # Memory_backed cache, not shared
            # TODO - share? Use class id as a key? Would allow for threading
            self._cache_data = DatasetCache("file:/?mode=memory", False, type(self))

        if not self._cache_data.read_only:
            # Add metadata to cache file (in case the user wants to share it)
            self._cache_data.update_metadata("dataset_metadata", self)

            # Only address, not username/password
            self._cache_data.update_metadata("client_address", self._client.address)

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

    def _background_add_entries(self, entries: Union[BaseModel, Sequence[BaseModel]]) -> InternalJob:
        """
        Internal function for adding entries to a dataset as an internal job

        This function handles batching and some type checking

        Parameters
        ----------
        entries
            Entries to add. May be just a single entry or a sequence of entries
        """

        self.assert_is_not_view()
        self.assert_online()

        entries = make_list(entries)
        assert all(isinstance(x, self._new_entry_type) for x in entries), "Incorrect entry type"

        job_id = self._client.make_request(
            "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/background_add_entries", int, body=entries
        )

        return self.get_internal_job(job_id)

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
            "provenance": self.provenance,
            "default_compute_tag": self.default_compute_tag,
            "default_compute_priority": self.default_compute_priority,
            "extras": self.extras,
        }

        new_body.update(**kwargs)
        body = DatasetModifyMetadata(**new_body)
        self._client.make_request("patch", f"api/v1/datasets/{self.dataset_type}/{self.id}", None, body=body)

        self.name = body.name
        self.description = body.description
        self.tagline = body.tagline
        self.tags = body.tags
        self.provenance = body.provenance
        self.default_compute_tag = body.default_compute_tag
        self.default_compute_priority = body.default_compute_priority
        self.extras = body.extras

        self._cache_data.update_metadata("dataset_metadata", self)

    def submit(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        compute_tag: Optional[str] = None,
        compute_priority: Optional[PriorityEnum] = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
    ) -> InsertCountsMetadata:
        """
        Create records for this dataset

        This function actually populates the datasets records given the entry and
        specification information.

        Parameters
        ----------
        entry_names
            Submit only records for these entries
        specification_names
            Submit only records for these specifications
        compute_tag
            Use this compute tag for submissions (overrides the dataset default tag)
        compute_priority
            Use this compute priority for submissions (overrides the dataset default priority)
        find_existing
            If True, the database will be searched for existing records that match the requested calculations, and new
            records created for those that don't match. If False, new records will always be created.
        """

        logger = logging.getLogger(self.__class__.__name__)
        if "tag" in kwargs:
            logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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

        n_inserted = 0
        n_existing = 0

        for spec in specification_names:
            for entry_batch in tqdm(chunk_iterable(entry_names, batch_size), total=n_batches, disable=None):
                body_data = DatasetSubmitBody(
                    entry_names=entry_batch,
                    specification_names=[spec],
                    compute_tag=compute_tag,
                    compute_priority=compute_priority,
                    find_existing=find_existing,
                )

                meta = self._client.make_request(
                    "post",
                    f"api/v1/datasets/{self.dataset_type}/{self.id}/submit",
                    InsertCountsMetadata,
                    body=body_data,
                )

                n_inserted += meta.n_inserted
                n_existing += meta.n_existing

        return InsertCountsMetadata(n_inserted=n_inserted, n_existing=n_existing)

    def background_submit(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        compute_tag: Optional[str] = None,
        compute_priority: PriorityEnum = None,
        find_existing: bool = True,
        **kwargs,  # For deprecated parameters
    ) -> InternalJob:
        """
        Adds a dataset submission internal job to the server

        This internal job is the one to actually do the submission, which can take a while.

        You can check the progress of the internal job using the return object.

        See :meth:`submit` for info on the function parameters.

        Returns
        -------
        :
            An internal job object that can be watch or used to determine the progress of the job.
        """

        logger = logging.getLogger(self.__class__.__name__)
        if "tag" in kwargs:
            logger.warning("'tag' is deprecated; use 'compute_tag' instead")
            compute_tag = kwargs["tag"]
        if "priority" in kwargs:
            logger.warning("'priority' is deprecated; use 'compute_priority' instead")
            compute_priority = kwargs["priority"]

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

        body_data = DatasetSubmitBody(
            entry_names=entry_names,
            specification_names=specification_names,
            compute_tag=compute_tag,
            compute_priority=compute_priority,
            find_existing=find_existing,
        )

        job_id = self._client.make_request(
            "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/background_submit", int, body=body_data
        )

        return self.get_internal_job(job_id)

    #########################################
    # Internal jobs
    #########################################
    def get_internal_job(self, job_id: int) -> InternalJob:
        self.assert_is_not_view()
        self.assert_online()

        ij_dict = self._client.make_request("get", f"/api/v1/datasets/{self.id}/internal_jobs/{job_id}", Dict[str, Any])
        refresh_url = f"/api/v1/datasets/{self.id}/internal_jobs/{ij_dict['id']}"
        return InternalJob(client=self._client, refresh_url=refresh_url, **ij_dict)

    def list_internal_jobs(
        self, status: Optional[Union[InternalJobStatusEnum, Iterable[InternalJobStatusEnum]]] = None
    ) -> List[InternalJob]:
        self.assert_is_not_view()
        self.assert_online()

        url_params = DatasetGetInternalJobParams(status=make_list(status))
        ij_dicts = self._client.make_request(
            "get", f"/api/v1/datasets/{self.id}/internal_jobs", List[Dict[str, Any]], url_params=url_params
        )
        return [
            InternalJob(client=self._client, refresh_url=f"/api/v1/datasets/{self.id}/internal_jobs/{ij['id']}", **ij)
            for ij in ij_dicts
        ]

    #########################################
    # Attachments
    #########################################
    def fetch_attachments(self):
        self.assert_is_not_view()
        self.assert_online()

        self.attachments_ = self._client.make_request(
            "get",
            f"api/v1/datasets/{self.id}/attachments",
            Optional[List[DatasetAttachment]],
        )

    @property
    def attachments(self) -> List[DatasetAttachment]:
        if not self.attachments_:
            self.fetch_attachments()

        return self.attachments_

    def delete_attachment(self, file_id: int):
        self.assert_is_not_view()
        self.assert_online()

        self._client.make_request(
            "delete",
            f"api/v1/datasets/{self.id}/attachments/{file_id}",
            None,
        )

        self.fetch_attachments()

    def download_attachment(
        self,
        attachment_id: int,
        destination_path: Optional[str] = None,
        overwrite: bool = True,
    ):
        """
        Downloads an attachment

        If destination path is not given, the file will be placed in the current directory, and the
        filename determined by what is stored on the server.

        Parameters
        ----------
        attachment_id
            ID of the attachment to download. See the `attachments` property
        destination_path
            Full path to the destination file (including filename)
        overwrite
            If True, any existing file will be overwritten
        """

        attachment_map = {x.id: x for x in self.attachments}
        if attachment_id not in attachment_map:
            raise ValueError(f"File id {attachment_id} is not a valid attachment for this dataset")

        if destination_path is None:
            attachment_data = attachment_map[attachment_id]
            destination_path = os.path.join(os.getcwd(), attachment_data.file_name)

        self._client.download_external_file(attachment_id, destination_path, overwrite=overwrite)

    #########################################
    # View creation and use
    #########################################
    def list_views(self):
        return [x for x in self.attachments if x.attachment_type == DatasetAttachmentType.view]

    def download_view(
        self,
        view_file_id: Optional[int] = None,
        destination_path: Optional[str] = None,
        overwrite: bool = True,
    ):
        """
        Downloads a view for this dataset

        If a `view_file_id` is not given, the most recent view will be downloaded.

        If destination path is not given, the file will be placed in the current directory, and the
        filename determined by what is stored on the server.

        Parameters
        ----------
        view_file_id
            ID of the view to download. See :meth:`list_views`. If `None`, will download the latest view
        destination_path
            Full path to the destination file (including filename)
        overwrite
            If True, any existing file will be overwritten
        """

        my_views = self.list_views()

        if not my_views:
            raise ValueError(f"No views available for this dataset")

        if view_file_id is None:
            latest_view_ids = max(my_views, key=lambda x: x.created_on)
            view_file_id = latest_view_ids.id

        view_map = {x.id: x for x in self.list_views()}
        if view_file_id not in view_map:
            raise ValueError(f"File id {view_file_id} is not a valid view for this dataset")

        self.download_attachment(view_file_id, destination_path, overwrite=overwrite)

    def use_view_cache(
        self,
        view_file_path: str,
    ):
        """
        Loads a vuew for this dataset as a cache file

        Parameters
        ----------
        view_file_path
            Full path to the view file
        """

        cache_uri = f"file:{view_file_path}"
        dcache = DatasetCache(cache_uri=cache_uri, read_only=False, dataset_type=type(self))

        meta = dcache.get_metadata("dataset_metadata")

        if meta["id"] != self.id:
            raise ValueError(
                f"Info in view file does not match this dataset. ID in the file {meta['id']}, ID of this dataset {self.id}"
            )

        if meta["dataset_type"] != self.dataset_type:
            raise ValueError(
                f"Info in view file does not match this dataset. Dataset type in the file {meta['dataset_type']}, dataset type of this dataset {self.dataset_type}"
            )

        if meta["name"] != self.name:
            raise ValueError(
                f"Info in view file does not match this dataset. Dataset name in the file {meta['name']}, name of this dataset {self.name}"
            )

        self._cache_data = dcache

    def preload_cache(self, view_file_id: Optional[int] = None):
        """
        Downloads a view file and uses it as the current cache

        Parameters
        ----------
        view_file_id
            ID of the view to download. See :meth:`list_views`. If `None`, will download the latest view
        """

        self.assert_is_not_view()
        self.assert_online()

        if not self._client.cache.is_disk:
            raise RuntimeError("Caching to disk is not enabled. Set the cache_dir path when constructing the client")

        destination_path = self._client.cache.get_dataset_cache_path(self.id)
        self.download_view(view_file_id=view_file_id, destination_path=destination_path, overwrite=True)
        self.use_view_cache(destination_path)

    def create_view(
        self,
        description: str,
        provenance: Dict[str, Any],
        status: Optional[Iterable[RecordStatusEnum]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        *,
        include_children: bool = True,
    ) -> InternalJob:
        """
        Creates a view of this dataset on the server

        This function will return an :class:`~qcportal.internal_jobs.InternalJob` which can be used to watch
        for completion if desired. The job will run server side without user interaction.

        Note the ID field of the object if you with to retrieve this internal job later
        (via :meth:`get_internal_jobs` or
        :meth:`PortalClient.get_internal_job <qcportal.client.PortalClient.get_internal_job>`)

        Parameters
        ----------
        description
            Optional string describing the view file
        provenance
            Dictionary with any metadata or other information about the view. Information regarding
            the options used to create the view will be added.
        status
            List of statuses to include. Default is to include records with any status
        include
            List of specific record fields to include in the export. Default is to include most fields
        exclude
            List of specific record fields to exclude from the export. Defaults to excluding none.
        include_children
            Specifies whether child records associated with the main records should also be included (recursively)
            in the view file.

        Returns
        -------
        :
            An :class:`~qcportal.internal_job.InternalJob` object which can be used to watch for completion.
        """

        body = DatasetCreateViewBody(
            description=description,
            provenance=provenance,
            status=status,
            include=include,
            exclude=exclude,
            include_children=include_children,
        )

        job_id = self._client.make_request(
            "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/create_view", int, body=body
        )

        return self.get_internal_job(job_id)

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

    def set_tags(self, new_tags: List[str]):
        self._update_metadata(tags=new_tags)

    def set_tagline(self, new_tagline: str):
        self._update_metadata(tagline=new_tagline)

    def set_provenance(self, new_provenance: Dict[str, Any]):
        self._update_metadata(provenance=new_provenance)

    def set_extras(self, new_extras: Dict[str, Any]):
        self._update_metadata(extras=new_extras)

    def set_default_compute_tag(self, new_default_compute_tag: str):
        self._update_metadata(default_compute_tag=new_default_compute_tag)

    def set_default_compute_priority(self, new_default_compute_priority: PriorityEnum):
        self._update_metadata(default_compute_priority=new_default_compute_priority)

    ##########################################
    # DEPRECATED - for backwards compatibility
    ##########################################
    @property
    def metadata(self) -> Dict[str, Any]:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("'metadata' is deprecated and will be removed in a future release. Use 'extras' instead")
        return self.extras

    def set_metadata(self, new_metadata: Dict[str, Any]):
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("set_metadata is deprecated and will be removed in a future release. Use set_extras instead")
        self.set_extras(new_metadata)

    @property
    def default_tag(self) -> str:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning(
            "'default_tag' is deprecated and will be removed in a future release. Use 'default_compute_tag' instead"
        )
        return self.default_compute_tag

    def set_default_tag(self, new_default_tag: str):
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("set_default_tag is deprecated and will be removed in a future release. Use set_extras instead")
        self.set_default_compute_tag(new_default_tag)

    @property
    def default_priority(self) -> PriorityEnum:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning(
            "'default_priority' is deprecated and will be removed in a future release. Use 'default_compute_priority' instead"
        )
        return self.default_compute_priority

    def set_default_priority(self, new_default_priority: PriorityEnum):
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning(
            "set_default_priority is deprecated and will be removed in a future release. Use set_extras instead"
        )
        self.set_default_compute_priority(new_default_priority)

    @property
    def visibility(self) -> bool:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("'visibility' is deprecated and will be removed in a future release")
        return True

    def set_visibility(self, new_visibility: bool):
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("set_visibility is deprecated and will be removed in a future release")

    @property
    def group(self) -> str:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("'group' is deprecated and will be removed in a future release")
        return "default"

    def set_group(self, new_group: str):
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("set_group is deprecated and will be removed in a future release")

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
            if self.is_view:
                self._specification_names = self._cache_data.get_specification_names()
            else:
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
            if self.is_view:
                self._entry_names = self._cache_data.get_entry_names()
            else:
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

    def modify_entries(
        self,
        attribute_map: Optional[Dict[str, Dict[str, Any]]] = None,
        comment_map: Optional[Dict[str, str]] = None,
        overwrite_attributes: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        body = DatasetModifyEntryBody(
            attribute_map=attribute_map, comment_map=comment_map, overwrite_attributes=overwrite_attributes
        )

        self._client.make_request(
            "patch", f"api/v1/datasets/{self.dataset_type}/{self.id}/entries/modify", None, body=body
        )

        # Sync local cache with updated server.
        entries_to_sync = set()
        if attribute_map is not None:
            entries_to_sync = entries_to_sync | attribute_map.keys()
        if comment_map is not None:
            entries_to_sync = entries_to_sync | comment_map.keys()

        self.fetch_entries(entries_to_sync, force_refetch=True)

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
    ) -> List[Tuple[str, str, BaseRecord]]:
        """
        Fetches records from the remote server

        This does not do any checking for existing records, but is used to actually
        request the data from the server.

        Note: This function does not do any batching w.r.t. server API limits. It is expected that is done
        before this function is called. This function also does not look up records in the cache, but does
        attach the cache to the records.

        Note: Records are not returned in any particular order

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

        Returns
        -------
        :
            List of tuples (entry_name, spec_name, record)
        """

        if not (entry_names and specification_names):
            return []

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

        # This function always fetches, so force_fetch = True
        # But records will be attached to thee cache
        records = get_records_with_cache(self._client, self._cache_data, self._record_type, record_ids, include, True)

        # Update the locally-stored metadata for these dataset records
        # zip(record_info, records) = ((entry_name, spec_name, record_id), record)
        update_records = [(ename, sname, r) for (ename, sname, _), r in zip(record_info, records)]
        update_info = [(ename, sname, r.id) for (ename, sname, r) in update_records]
        self._cache_data.update_dataset_records(update_info)

        return update_records

    def _internal_update_records(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]],
        include: Optional[Iterable[str]],
    ) -> List[Tuple[str, str, BaseRecord]]:
        """
        Update local record information if the record has been modified on the server

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
            return []

        # Returns list of tuple (entry name, spec_name, id, status, modified_on) of records
        # we have our local cache
        updateable_record_info = self._cache_data.get_dataset_record_info(entry_names, specification_names, None)

        # print(f"UPDATEABLE RECORDS: {len(updateable_record_info)}")
        if not updateable_record_info:
            return []

        batch_size = math.ceil(self._client.api_limits["get_records"] / 4)
        server_modified_time: Dict[int, datetime] = {}  # record_id -> modified time on server

        # Find out which records have been updated on the server
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
                    server_modified_time[sri["id"]] = pydantic.parse_obj_as(datetime, sri["modified_on"])

        # Which ones need to be fully updated
        need_updating: Dict[str, List[str]] = {}  # key is specification, value is list of entry names
        for entry_name, spec_name, record_id, _, local_mtime in updateable_record_info:
            server_mtime = server_modified_time.get(record_id, None)

            # Perhaps the record doesn't exist on the server anymore or something
            if server_mtime is None:
                continue

            if local_mtime < server_mtime:
                need_updating.setdefault(spec_name, [])
                need_updating[spec_name].append(entry_name)

        # Update from the server one spec at a time
        # print(f"Updated on server: {len(needs_updating)}")
        updated_records = []

        for spec_name, entries_to_update in need_updating.items():
            for entries_batch in chunk_iterable(entries_to_update, batch_size):
                # Updates dataset record metadata if needed
                r = self._internal_fetch_records(entries_batch, [spec_name], None, include)
                updated_records.extend(r)

        return updated_records

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
        batch_size: int = math.ceil(self._client.api_limits["get_records"])
        n_batches = math.ceil(len(entry_names) / batch_size)

        # Do all entries for one spec. This simplifies things, especially with handling
        # existing or update-able records
        for spec_name in specification_names:
            for entry_names_batch in tqdm(chunk_iterable(entry_names, batch_size), total=n_batches, disable=None):
                records_batch = []

                # Handle existing records that need to be updated
                if force_refetch:
                    r = self._internal_fetch_records(entry_names_batch, [spec_name], status, include)
                    records_batch.extend(r)

                else:
                    missing_entries = entry_names_batch.copy()

                    if fetch_updated:
                        updated_records = self._internal_update_records(missing_entries, [spec_name], status, include)
                        records_batch.extend(updated_records)

                        # what wasn't updated
                        updated_entries = [x for x, _, _ in updated_records]
                        missing_entries = [e for e in entry_names_batch if e not in updated_entries]

                    # Check if we have any cached records
                    cached_records = self._cache_data.get_dataset_records(missing_entries, [spec_name])
                    for _, _, cr in cached_records:
                        cr.propagate_client(self._client)

                    records_batch.extend(cached_records)

                    # what we need to fetch from the server
                    cached_entries = [x[0] for x in cached_records]
                    missing_entries = [e for e in missing_entries if e not in cached_entries]

                    fetched_records = self._internal_fetch_records(missing_entries, [spec_name], status, include)
                    records_batch.extend(fetched_records)

                # Write the record batch to the cache at once. Also marks the records as clean (no need to writeback)
                self._cache_data.update_records([r for _, _, r in records_batch])

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

        record = None
        if force_refetch:
            records = self._internal_fetch_records([entry_name], [specification_name], None, include)
            if records:
                record = records[0][2]
        elif fetch_updated:
            records = self._internal_update_records([entry_name], [specification_name], None, include)
            if records:
                record = records[0][2]

        if record is None:
            # Attempt to get from cache
            record = self._cache_data.get_dataset_record(entry_name, specification_name)

        if record is None and not self.is_view:
            # not in cache
            records = self._internal_fetch_records([entry_name], [specification_name], None, include)
            if records:
                record = records[0][2]

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
                    record_data = self._cache_data.get_dataset_records(entry_names_batch, [spec_name], status)

                    for e, s, r in record_data:
                        yield e, s, r
        else:
            batch_size: int = math.ceil(self._client.api_limits["get_records"])

            for spec_name in specification_names:
                for entry_names_batch in chunk_iterable(entry_names, batch_size):
                    records_batch = []

                    # Handle existing records that need to be updated
                    if force_refetch:
                        r = self._internal_fetch_records(entry_names_batch, [spec_name], status, include)
                        records_batch.extend(r)

                    else:
                        missing_entries = entry_names_batch.copy()

                        if fetch_updated:
                            updated_records = self._internal_update_records(
                                missing_entries, [spec_name], status, include
                            )
                            records_batch.extend(updated_records)

                            # what wasn't updated
                            updated_entries = [x for x, _, _ in updated_records]
                            missing_entries = [e for e in entry_names_batch if e not in updated_entries]

                        # Check if we have any cached records
                        cached_records = self._cache_data.get_dataset_records(missing_entries, [spec_name])
                        for _, _, cr in cached_records:
                            cr.propagate_client(self._client)

                        records_batch.extend(cached_records)

                        # what we need to fetch from the server
                        cached_entries = [x[0] for x in cached_records]
                        missing_entries = [e for e in missing_entries if e not in cached_entries]

                        fetched_records = self._internal_fetch_records(missing_entries, [spec_name], status, include)
                        records_batch.extend(fetched_records)

                    # Let the writeback mechanism handle writing to the cache
                    for e, s, r in records_batch:
                        if status is None or r.status in status:
                            yield e, s, r

    def remove_records(
        self,
        entry_names: Union[str, Iterable[str]],
        specification_names: Union[str, Iterable[str]],
        delete_records: bool = False,
    ) -> None:
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        for entry_names_batch in chunk_iterable(entry_names, 200):
            body = DatasetRemoveRecordsBody(
                entry_names=entry_names_batch,
                specification_names=specification_names,
                delete_records=delete_records,
            )

            self._client.make_request(
                "post",
                f"api/v1/datasets/{self.dataset_type}/{self.id}/records/bulkDelete",
                None,
                body=body,
            )

            if delete_records:
                record_info = self._cache_data.get_dataset_records(entry_names_batch, specification_names)
                self._cache_data.delete_records([r.id for _, _, r in record_info])

            self._cache_data.delete_dataset_records(entry_names_batch, specification_names)

    def _modify_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        new_compute_tag: Optional[str] = None,
        new_compute_priority: Optional[PriorityEnum] = None,
        new_comment: Optional[str] = None,
        new_status: Optional[RecordStatusEnum] = None,
        *,
        refetch_records: bool = False,
        **kwargs,  # For deprecated parameters
    ):

        # TODO - DEPRECATED - remove eventually
        logger = logging.getLogger(self.__class__.__name__)
        if "new_tag" in kwargs:
            logger.warning("'new_tag' is deprecated; use 'new_compute_tag' instead")
            new_compute_tag = kwargs["new_tag"]
        if "new_priority" in kwargs:
            logger.warning("'new_priority' is deprecated; use 'new_compute_priority' instead")
            new_compute_priority = kwargs["new_priority"]

        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        if entry_names is None:
            body = DatasetRecordModifyBody(
                entry_names=None,
                specification_names=specification_names,
                compute_tag=new_compute_tag,
                compute_priority=new_compute_priority,
                comment=new_comment,
                status=new_status,
            )

            self._client.make_request(
                "patch",
                f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
                None,
                body=body,
            )
        else:
            for entry_names_batch in chunk_iterable(entry_names, 200):
                body = DatasetRecordModifyBody(
                    entry_names=entry_names_batch,
                    specification_names=specification_names,
                    compute_tag=new_compute_tag,
                    compute_priority=new_compute_priority,
                    comment=new_comment,
                    status=new_status,
                )

                self._client.make_request(
                    "patch",
                    f"api/v1/datasets/{self.dataset_type}/{self.id}/records",
                    None,
                    body=body,
                )

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

    def _revert_records(
        self,
        revert_status: RecordStatusEnum,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self.assert_is_not_view()
        self.assert_online()

        entry_names = make_list(entry_names)
        specification_names = make_list(specification_names)

        if entry_names is None:
            body = DatasetRecordRevertBody(
                entry_names=None,
                specification_names=specification_names,
                revert_status=revert_status,
            )

            self._client.make_request(
                "post",
                f"api/v1/datasets/{self.dataset_type}/{self.id}/records/revert",
                None,
                body=body,
            )
        else:
            for entry_names_batch in chunk_iterable(entry_names, 200):
                body = DatasetRecordRevertBody(
                    entry_names=entry_names_batch,
                    specification_names=specification_names,
                    revert_status=revert_status,
                )

                self._client.make_request(
                    "post",
                    f"api/v1/datasets/{self.dataset_type}/{self.id}/records/revert",
                    None,
                    body=body,
                )

        if refetch_records:
            self.fetch_records(entry_names, specification_names, force_refetch=True)

    def modify_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        new_compute_tag: Optional[str] = None,
        new_compute_priority: Optional[PriorityEnum] = None,
        new_comment: Optional[str] = None,
        *,
        refetch_records: bool = False,
    ):

        self._modify_records(
            entry_names=entry_names,
            specification_names=specification_names,
            new_compute_tag=new_compute_tag,
            new_compute_priority=new_compute_priority,
            new_comment=new_comment,
            refetch_records=refetch_records,
        )

    def reset_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):

        self._modify_records(
            entry_names=entry_names,
            specification_names=specification_names,
            new_status=RecordStatusEnum.waiting,
            refetch_records=refetch_records,
        )

    def cancel_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):

        self._modify_records(
            entry_names=entry_names,
            specification_names=specification_names,
            new_status=RecordStatusEnum.cancelled,
            refetch_records=refetch_records,
        )

    def uncancel_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self._revert_records(
            revert_status=RecordStatusEnum.cancelled,
            entry_names=entry_names,
            specification_names=specification_names,
            refetch_records=refetch_records,
        )

    def invalidate_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):

        self._modify_records(
            entry_names=entry_names,
            specification_names=specification_names,
            new_status=RecordStatusEnum.invalid,
            refetch_records=refetch_records,
        )

    def uninvalidate_records(
        self,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        *,
        refetch_records: bool = False,
    ):
        self._revert_records(
            revert_status=RecordStatusEnum.invalid,
            entry_names=entry_names,
            specification_names=specification_names,
            refetch_records=refetch_records,
        )

    def copy_entries_from(
        self,
        source_dataset_id: int,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
    ):
        """
        Copies entries from another dataset into this one

        If entries already exist with the same name, an exception is raised.

        Parameters
        ----------
        source_dataset_id
            The ID of the dataset to copy entries from
        entry_names
            Names of the entries to copy. If not provided, all entries will be copied.
        """

        self.assert_is_not_view()
        self.assert_online()

        body_data = DatasetCopyFromBody(
            source_dataset_id=source_dataset_id,
            entry_names=make_list(entry_names),
            copy_entries=True,
        )

        self._client.make_request(
            "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/copy_from", None, body=body_data
        )

        self.fetch_entry_names()

    def copy_specifications_from(
        self,
        source_dataset_id: int,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
    ):
        """
        Copies specifications from another dataset into this one

        If specifications already exist with the same name, an exception is raised.

        Parameters
        ----------
        source_dataset_id
            The ID of the dataset to copy entries from
        specification_names
            Names of the specifications to copy. If not provided, all specifications will be copied.
        """
        self.assert_is_not_view()
        self.assert_online()

        body_data = DatasetCopyFromBody(
            source_dataset_id=source_dataset_id,
            specification_names=make_list(specification_names),
            copy_specifications=True,
        )

        self._client.make_request(
            "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/copy_from", None, body=body_data
        )

        self.fetch_specifications()

    def copy_records_from(
        self,
        source_dataset_id: int,
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
    ):
        """
        Copies records from another dataset into this one

        Entries and specifications will also be copied.
        If entries or specifications already exist with the same name, an exception is raised.

        This does not actually fully copy records - the records will be linked to both datasets

        Parameters
        ----------
        source_dataset_id
            The ID of the dataset to copy entries from
        entry_names
            Names of the entries to copy. If not provided, all entries will be copied.
        specification_names
            Names of the specifications to copy. If not provided, all specifications will be copied.
        """

        self.assert_is_not_view()
        self.assert_online()

        body_data = DatasetCopyFromBody(
            source_dataset_id=source_dataset_id,
            entry_names=make_list(entry_names),
            specification_names=make_list(specification_names),
            copy_records=True,
        )

        self._client.make_request(
            "post", f"api/v1/datasets/{self.dataset_type}/{self.id}/copy_from", None, body=body_data
        )

        self.fetch_entry_names()
        self.fetch_specifications()

    def compile_values(
        self,
        value_call: Callable,
        value_names: Union[Sequence[str], str] = "value",
        entry_names: Optional[Union[str, Iterable[str]]] = None,
        specification_names: Optional[Union[str, Iterable[str]]] = None,
        unpack: bool = False,
    ) -> "DataFrame":
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
        pandas.DataFrame
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
        import pandas as pd

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

    def get_properties_df(
        self,
        properties_list: Sequence[str],
        entry_names: Sequence[str] | None = None,
        specification_names: Sequence[str] | None = None,
    ) -> "DataFrame":
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

        entry_names
            Entry names to filter records. If not provided, considers all entries.

        specification_names
            Specification names to filter records. If not provided, considers all specifications.

        Returns:
        --------
        pandas.DataFrame
            A DataFrame populated with the specified properties for each record.
        """

        # create lambda function to get all properties at once
        extract_properties = lambda x: [x.properties.get(property_name) for property_name in properties_list]

        # retrieve values.
        result = self.compile_values(
            extract_properties,
            value_names=properties_list,
            unpack=True,
            specification_names=specification_names,
            entry_names=entry_names,
        )

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

        self.contributed_values_ = self._client.make_request(
            "get",
            f"api/v1/datasets/{self.id}/contributed_values",
            Optional[Dict[str, ContributedValues]],
        )

    @property
    def contributed_values(self) -> Dict[str, ContributedValues]:
        if not self.contributed_values_:
            self.fetch_contributed_values()

        return self.contributed_values_


class DatasetAddBody(RestModelBase):
    name: str
    description: str
    tagline: str
    tags: List[str]
    provenance: Dict[str, Any]
    default_compute_tag: str
    default_compute_priority: PriorityEnum
    extras: Dict[str, Any]
    owner_group: Optional[str]
    existing_ok: bool = False

    # TODO - DEPRECATED - Remove eventually
    @root_validator(pre=True)
    def _rm_deprecated(cls, values):
        if "group" in values:
            del values["group"]
        if "visibility" in values:
            del values["visibility"]
        if "metadata" in values:
            values["extras"] = values.pop("metadata")

        if "default_tag" in values:
            values["default_compute_tag"] = values.pop("default_tag")
        if "default_priority" in values:
            values["default_compute_priority"] = values.pop("default_priority")

        return values


class DatasetModifyMetadata(RestModelBase):
    name: str
    description: str
    tags: List[str]
    tagline: str
    provenance: Optional[Dict[str, Any]]
    extras: Optional[Dict[str, Any]]

    default_compute_tag: str
    default_compute_priority: PriorityEnum

    # TODO - DEPRECATED - Remove eventually
    @root_validator(pre=True)
    def _rm_deprecated(cls, values):
        if "group" in values:
            del values["group"]
        if "visibility" in values:
            del values["visibility"]
        if "metadata" in values:
            values["extras"] = values.pop("metadata")

        if "default_tag" in values:
            values["default_compute_tag"] = values.pop("default_tag")
        if "default_priority" in values:
            values["default_compute_priority"] = values.pop("default_priority")

        return values


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


class DatasetCloneBody(RestModelBase):
    source_dataset_id: int
    new_dataset_name: str


class DatasetCopyFromBody(RestModelBase):
    source_dataset_id: int
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    copy_entries: bool = False
    copy_specifications: bool = False
    copy_records: bool = False


class DatasetFetchRecordsBody(RestModelBase):
    entry_names: List[str]
    specification_names: List[str]
    status: Optional[List[RecordStatusEnum]] = None


class DatasetCreateViewBody(RestModelBase):
    description: Optional[str]
    provenance: Dict[str, Any]
    status: Optional[List[RecordStatusEnum]] = (None,)
    include: Optional[List[str]] = (None,)
    exclude: Optional[List[str]] = (None,)
    include_children: bool = (True,)


class DatasetSubmitBody(RestModelBase):
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    compute_tag: Optional[str] = None
    compute_priority: Optional[PriorityEnum] = None
    owner_group: Optional[str] = None
    find_existing: bool = True

    @root_validator(pre=True)
    def _rm_deprecated(cls, values):
        # TODO - DEPRECATED - Remove eventually
        if "tag" in values:
            values["compute_tag"] = values.pop("tag")
        if "priority" in values:
            values["compute_priority"] = values.pop("priority")

        return values


class DatasetRecordModifyBody(RestModelBase):
    entry_names: Optional[List[str]] = None
    specification_names: Optional[List[str]] = None
    status: Optional[RecordStatusEnum] = None
    compute_priority: Optional[PriorityEnum] = None
    compute_tag: Optional[str] = None
    comment: Optional[str] = None

    @root_validator(pre=True)
    def _rm_deprecated(cls, values):
        # TODO - DEPRECATED - Remove eventually
        if "tag" in values:
            values["compute_tag"] = values.pop("tag")
        if "priority" in values:
            values["compute_priority"] = values.pop("priority")

        return values


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


class DatasetModifyEntryBody(RestModelBase):
    attribute_map: Optional[Dict[str, Dict[str, Any]]] = None
    comment_map: Optional[Dict[str, str]] = None
    overwrite_attributes: bool = False


class DatasetGetInternalJobParams(RestModelBase):
    status: Optional[List[InternalJobStatusEnum]] = None


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


def load_dataset_view(file_path: str) -> BaseDataset:
    # Reads this as a read-only "view"
    ds_meta = read_dataset_metadata(file_path)
    ds_type = BaseDataset.get_subclass(ds_meta["dataset_type"])

    file_path = os.path.abspath(file_path)
    cache_uri = f"file:{file_path}?mode=ro"
    ds_cache = DatasetCache(cache_uri, True, ds_type)

    # Views never have a client attached
    return dataset_from_dict(ds_meta, None, cache_data=ds_cache)


def dataset_from_cache(file_path: str) -> BaseDataset:
    # Keep old name around
    return load_dataset_view(file_path)


def create_dataset_view(
    client: PortalClient,
    dataset_id: int,
    file_path: str,
    include: Optional[Iterable[str]] = None,
    overwrite: bool = False,
):
    file_path = os.path.abspath(file_path)

    if os.path.exists(file_path) and not os.path.isfile(file_path):
        raise ValueError(f"Path {file_path} exists and is not a file")

    if os.path.exists(file_path) and not overwrite:
        raise ValueError(f"File {file_path} exists and overwrite is False")

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Manually get it, because we want to use a different cache file
    ds_dict = client.make_request("get", f"api/v1/datasets/{dataset_id}", Dict[str, Any])
    ds_cache = DatasetCache(f"file:{file_path}", False, BaseDataset.get_subclass(ds_dict["dataset_type"]))

    ds = dataset_from_dict(ds_dict, client, ds_cache)

    ds.fetch_records(include=include, force_refetch=True)
