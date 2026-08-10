from __future__ import annotations

import os
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, field_validator, PrivateAttr, Field, ConfigDict

from qcportal.all_inputs import AllInputTypes
from qcportal.all_results import AllResultTypes
from qcportal.base_models import ProjURLParameters
from qcportal.base_models import RestModelBase, validate_list_to_single
from qcportal.dataset_models import BaseDataset, dataset_from_dict
from qcportal.external_files.models import ExternalFile, ExternalFileUploadBase
from qcportal.metadata_models import InsertCountsMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, RecordAddBodyBase, record_from_dict, BaseRecord
from qcportal.utils import make_list

if TYPE_CHECKING:
    from qcportal.client import PortalClient


class ProjectAttachmentType(str, Enum):
    """
    The type of attachment a file is for a project
    """

    other = "other"


class ProjectAttachment(ExternalFile):
    """
    A file that has been uploaded to a project
    """

    attachment_type: ProjectAttachmentType
    tags: list[str]


class ProjectQueryModel(RestModelBase):
    project_name: str | None = None
    include: list[str] | None = None
    exclude: list[str] | None = None


class ProjectDeleteParams(RestModelBase):
    delete_records: bool = False
    delete_datasets: bool = False
    delete_dataset_records: bool = False

    @field_validator("delete_records", "delete_datasets", "delete_dataset_records", mode="before")
    @classmethod
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class ProjectAddBody(RestModelBase):
    name: str
    description: str
    tagline: str
    tags: list[str]
    default_compute_tag: str
    default_compute_priority: PriorityEnum
    extras: dict[str, Any]
    existing_ok: bool = False


# This is basically a duplicate of DatasetAddBody, but with
# dataset_type added. Ok to duplicate because we will eventually move
# to projects-only
class ProjectDatasetAddBody(RestModelBase):
    dataset_type: str
    name: str
    description: str
    tagline: str
    tags: list[str]
    provenance: dict[str, Any]
    default_compute_tag: str
    default_compute_priority: PriorityEnum
    extras: dict[str, Any]
    existing_ok: bool = False


class ProjectLinkDatasetBody(RestModelBase):
    dataset_id: int
    name: str | None
    description: str | None
    tagline: str | None
    tags: list[str] | None


class ProjectUnlinkDatasetsBody(RestModelBase):
    dataset_ids: list[int]
    delete_datasets: bool
    delete_dataset_records: bool


class ProjectRecordAddBody(RecordAddBodyBase):
    record_input: AllInputTypes
    name: str
    description: str
    tags: list[str]


class ProjectRecordImportBody(RestModelBase):
    record_data: AllResultTypes
    name: str
    description: str
    tags: list[str]


class ProjectLinkRecordBody(RestModelBase):
    record_id: int
    name: str
    description: str
    tags: list[str]


class ProjectUnlinkRecordsBody(RestModelBase):
    record_ids: list[int]
    delete_records: bool


class ProjectAttachmentUploadBody(ExternalFileUploadBase):
    attachment_type: ProjectAttachmentType
    tags: list[str]


class ProjectRecordMetadata(BaseModel):
    """
    Information about a record contained in a project

    This is the lightweight information about a record that a project stores, and does
    not include the record itself. Use :meth:`Project.get_record` to obtain the full record.
    """

    record_id: int
    name: str
    description: str
    tags: list[str]

    record_type: str
    status: RecordStatusEnum


class ProjectDatasetMetadata(BaseModel):
    """
    Information about a dataset contained in a project

    This is the lightweight information about a dataset that a project stores, and does
    not include the dataset itself. Use :meth:`Project.get_dataset` to obtain the full dataset.
    """

    dataset_id: int
    dataset_type: str
    name: str
    description: str
    tagline: str
    tags: list[str]


class ProjectQueryRecords(RestModelBase):
    record_id: list[int]

class ProjectQueryDatasets(RestModelBase):
    dataset_id: list[int]

class Project(BaseModel):
    """
    A named collection of records, datasets, and files

    A project groups together the records and datasets belonging to a single piece of work.
    Within a project, records and datasets are given names, and most methods here accept
    either that name or the underlying ID.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=False)

    id: int
    name: str
    description: str
    tagline: str
    tags: list[str]

    default_compute_tag: str
    default_compute_priority: PriorityEnum

    owner_user: str | None

    extras: dict[str, Any]

    ########################################
    # Caches of information
    ########################################
    _record_metadata: list[ProjectRecordMetadata] = PrivateAttr([])
    _dataset_metadata: list[ProjectDatasetMetadata] = PrivateAttr([])

    ######################################################
    # Fields not always included when fetching the project
    ######################################################
    attachments_: list[ProjectAttachment] | None = Field(None, alias="attachments")

    #############################
    # Private non-pydantic fields
    #############################
    _client: Any = PrivateAttr(None)

    @property
    def offline(self) -> bool:
        """
        True if this project is not connected to a server
        """
        return self._client is None

    def assert_online(self):
        """
        Raises a RuntimeError if this project is not connected to a server
        """
        if self.offline:
            raise RuntimeError("Project is not connected to a QCFractal server")

    def __init__(self, client: PortalClient | None = None, **kwargs):
        BaseModel.__init__(self, **kwargs)

        # Calls derived class propagate_client
        # which should filter down to the ones in this (BaseDataset) class
        self.propagate_client(client)

    def propagate_client(self, client):
        """
        Propagates a client to this record to any fields within this record that need it

        This may also be called from derived class propagate_client functions as well
        """
        self._client = client

    #############################
    # General info
    #############################
    @property
    def dataset_metadata(self) -> list[ProjectDatasetMetadata]:
        """
        Information about the datasets in this project

        This is fetched from the server the first time it is accessed, and then cached. Use
        :meth:`fetch_dataset_metadata` to fetch it again.
        """
        self.assert_online()
        if len(self._dataset_metadata) == 0:
            self.fetch_dataset_metadata()
        return self._dataset_metadata

    @property
    def record_metadata(self) -> list[ProjectRecordMetadata]:
        """
        Information about the records in this project

        This is fetched from the server the first time it is accessed, and then cached. Use
        :meth:`fetch_record_metadata` to fetch it again.
        """
        self.assert_online()
        if len(self._record_metadata) == 0:
            self.fetch_record_metadata()
        return self._record_metadata

    def status(self) -> dict[str, dict[RecordStatusEnum, int]]:
        """
        Obtain a summary of the status of everything in this project

        The returned dictionary has two keys - ``records`` and ``datasets``. Each maps to a
        dictionary of record status to the number of records with that status. The ``records``
        entry counts only the records added directly to the project; the ``datasets`` entry
        counts the records of all the datasets in the project, summed together.

        Returns
        -------
        :
            Counts of record statuses, split into records and datasets
        """
        self.assert_online()

        return self._client.make_request(
            "get", f"api/v1/projects/{self.id}/status", dict[str, dict[RecordStatusEnum, int]]
        )

    #############################
    # Records
    #############################
    def _lookup_record_id(self, name: str) -> int:
        for d in self._record_metadata:
            if d.name == name:
                return d.record_id

        raise KeyError(f"Record '{name}' not found")

    def fetch_record_metadata(self):
        """
        Fetches information about the records in this project from the server

        This overwrites what is stored locally, and is available through the
        :attr:`record_metadata` property.
        """
        self.assert_online()

        self._record_metadata = self._client.make_request(
            "get", f"api/v1/projects/{self.id}/record_metadata", list[ProjectRecordMetadata]
        )

    def add_record(
        self,
        name: str,
        record_input: AllInputTypes,
        *,
        description: str | None = None,
        tags: list[str] = None,
        compute_tag: str | None = None,
        compute_priority: PriorityEnum | None = None,
        find_existing: bool = True,
    ) -> BaseRecord:
        """
        Creates a new record in this project and submits it for computation

        Unlike the ``add_*`` methods of the client, this takes a single record input object
        (for example, a :class:`~qcportal.singlepoint.record_models.SinglepointInput`) and
        creates a single record.

        Parameters
        ----------
        name
            Name to give this record within the project. Must be unique within the project
        record_input
            The specification and input molecule(s), as a record input object
        description
            Optional longer description of this record
        tags
            Optional list of tags to attach to this record within the project
        compute_tag
            The :ref:`compute tag <glossary_tag>` to use. Defaults to the project's
            ``default_compute_tag``
        compute_priority
            The priority to run this computation at. Defaults to the project's
            ``default_compute_priority``
        find_existing
            If True, and a matching record already exists on the server, use that record rather
            than creating a new one. See :ref:`record_submit_dedup`

        Returns
        -------
        :
            The new record, attached to the project and server
        """

        self.assert_online()

        if tags is None:
            tags = []
        if description is None:
            description = ""
        if compute_tag is None:
            compute_tag = self.default_compute_tag
        if compute_priority is None:
            compute_priority = self.default_compute_priority

        body_data = ProjectRecordAddBody(
            name=name,
            description=description,
            tags=tags,
            record_input=record_input,
            compute_tag=compute_tag,
            compute_priority=compute_priority,
            find_existing=find_existing,
        )

        meta, record_id = self._client.make_request(
            "post", f"api/v1/projects/{self.id}/records", tuple[InsertCountsMetadata, int], body=body_data
        )

        if not meta.success:
            raise RuntimeError(f"Adding record failed: {meta.error_message}")

        full_record = self.get_record(record_id)

        record_meta = ProjectRecordMetadata(
            record_id=record_id,
            name=name,
            description=description,
            tags=tags,
            record_type=full_record.record_type,
            status=full_record.status,
        )

        self._record_metadata.append(record_meta)

        return full_record

    def import_record(
        self,
        name: str,
        record: AllResultTypes,
        *,
        description: str | None = None,
        tags: list[str] = None,
    ) -> BaseRecord:
        """
        Imports an already-computed record into this project

        The record is not run on this server - the existing results are stored as-is. This is
        used for ingesting records computed elsewhere (on another server, or by hand).

        Parameters
        ----------
        name
            Name to give this record within the project. Must be unique within the project
        record
            The completed record or result to import
        description
            Optional longer description of this record
        tags
            Optional list of tags to attach to this record within the project

        Returns
        -------
        :
            The record, attached to the project and server
        """

        self.assert_online()

        if tags is None:
            tags = []
        if description is None:
            description = ""

        body_data = ProjectRecordImportBody(
            name=name,
            description=description,
            tags=tags,
            record_data=record,
        )

        record_id = self._client.make_request("post", f"api/v1/projects/{self.id}/records/import", int, body=body_data)

        full_record = self.get_record(record_id)

        record_meta = ProjectRecordMetadata(
            record_id=record_id,
            name=name,
            description=description,
            tags=tags,
            record_type=full_record.record_type,
            status=full_record.status,
        )

        self._record_metadata.append(record_meta)

        return full_record

    def link_record(
        self,
        record_id: int,
        name: str,
        description: str,
        tags: list[str],
    ) -> BaseRecord:
        """
        Adds an existing record on the server to this project

        The record itself is not copied or modified - the project gains a reference to it,
        along with a project-local name, description, and tags.

        Parameters
        ----------
        record_id
            ID of an existing record on the server
        name
            Name to give this record within the project. Must be unique within the project
        description
            Longer description of this record
        tags
            List of tags to attach to this record within the project

        Returns
        -------
        :
            The linked record
        """

        body = ProjectLinkRecordBody(record_id=record_id, name=name, description=description, tags=tags)
        self._client.make_request("post", f"api/v1/projects/{self.id}/records/link", None, body=body)

        rec = self.get_record(record_id)

        record_meta = ProjectRecordMetadata(
            record_id=record_id,
            name=name,
            description=description,
            tags=tags,
            record_type=rec.record_type,
            status=rec.status,
        )

        self._record_metadata.append(record_meta)

        return rec

    def unlink_records(self, record_ids: int | str | list[int | str], delete_records: bool = False):
        """
        Removes records from this project

        By default the records remain on the server and only the association with this project
        is removed.

        Parameters
        ----------
        record_ids
            Record IDs or project-local record names to remove from the project
        delete_records
            If True, also delete the records themselves from the server
        """
        record_ids = make_list(record_ids)
        record_ids = [self._lookup_record_id(rid) if isinstance(rid, str) else rid for rid in record_ids]
        body = ProjectUnlinkRecordsBody(record_ids=record_ids, delete_records=delete_records)

        self._client.make_request("post", f"api/v1/projects/{self.id}/records/unlink", None, body=body)
        self._record_metadata = [r for r in self._record_metadata if r.record_id not in record_ids]

    def get_record(
        self,
        record_id: int | str,
        include: Sequence[str] | None = None,
    ) -> BaseRecord:
        """
        Obtain a record contained in this project

        Parameters
        ----------
        record_id
            The record ID, or the name the record was given within this project
        include
            Additional fields to include in the returned record

        Returns
        -------
        :
            The record, of the appropriate type for the computation
        """

        if isinstance(record_id, str):
            record_id = self._lookup_record_id(record_id)

        url_params = ProjURLParameters(include=include)

        r_dict = self._client.make_request(
            "get", f"api/v1/projects/{self.id}/records/{record_id}", dict[str, Any], url_params=url_params
        )

        # set prefix to be the main prefix of the server. We will fetch all the records/datasets
        # directly via the regular (non-project) endpoints
        return record_from_dict(r_dict, client=self._client, base_url_prefix="api/v1")

    #############################
    # Datasets
    #############################
    def _lookup_dataset_id(self, name: str) -> int:
        for d in self._dataset_metadata:
            if d.name == name:
                return d.dataset_id

        raise KeyError(f"Dataset '{name}' not found")

    def fetch_dataset_metadata(self):
        """
        Fetches information about the datasets in this project from the server

        This overwrites what is stored locally, and is available through the
        :attr:`dataset_metadata` property.
        """
        self.assert_online()

        self._dataset_metadata = self._client.make_request(
            "get",
            f"api/v1/projects/{self.id}/dataset_metadata",
            list[ProjectDatasetMetadata],
        )

    def add_dataset(
        self,
        dataset_type: str,
        name: str,
        description: str | None = None,
        tagline: str | None = None,
        tags: list[str] | None = None,
        provenance: dict[str, Any] | None = None,
        default_compute_tag: str | None = None,
        default_compute_priority: PriorityEnum | None = None,
        extras: dict[str, Any] | None = None,
        existing_ok: bool = False,
    ) -> BaseDataset:
        """
        Creates a new dataset within this project

        Parameters
        ----------
        dataset_type
            The type of dataset to create (``singlepoint``, ``optimization``, and so on)
        name
            Name to give this dataset. Must be unique within the project
        description
            Optional longer description of this dataset
        tagline
            Optional short description of this dataset
        tags
            Optional list of tags to attach to this dataset
        provenance
            Optional dictionary describing the source of this dataset
        default_compute_tag
            The default :ref:`compute tag <glossary_tag>` for computations submitted from this
            dataset. Defaults to the project's ``default_compute_tag``
        default_compute_priority
            The default priority for computations submitted from this dataset. Defaults to the
            project's ``default_compute_priority``
        extras
            Optional dictionary of arbitrary additional information
        existing_ok
            If True, return the existing dataset if one with this name is already in the project,
            rather than raising an exception

        Returns
        -------
        :
            The new dataset, of the appropriate type for ``dataset_type``
        """

        self.assert_online()

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

        if default_compute_tag is None:
            default_compute_tag = self.default_compute_tag
        if default_compute_priority is None:
            default_compute_priority = self.default_compute_priority

        body = ProjectDatasetAddBody(
            dataset_type=dataset_type,
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
            provenance=provenance,
            default_compute_tag=default_compute_tag,
            default_compute_priority=default_compute_priority,
            extras=extras,
            existing_ok=existing_ok,
        )

        ds_id = self._client.make_request("post", f"api/v1/projects/{self.id}/datasets", int, body=body)
        full_dataset = self.get_dataset(ds_id)

        dataset_meta = ProjectDatasetMetadata(
            dataset_id=ds_id,
            dataset_type=dataset_type,
            name=name,
            description=description,
            tagline=tagline,
            tags=tags,
        )

        self._dataset_metadata.append(dataset_meta)

        return full_dataset

    def link_dataset(
        self,
        dataset_id: int,
        name: str | None = None,
        description: str | None = None,
        tagline: str | None = None,
        tags: list[str] | None = None,
    ) -> BaseDataset:
        """
        Adds an existing dataset on the server to this project

        The dataset itself is not copied. The project gains a reference to it, optionally
        under a different name and description.

        Parameters
        ----------
        dataset_id
            ID of an existing dataset on the server
        name
            Name to give this dataset within the project. If None, the dataset's existing
            name is kept
        description
            Longer description of this dataset. If None, the existing description is kept
        tagline
            Short description of this dataset. If None, the existing tagline is kept
        tags
            List of tags for this dataset. If None, the existing tags are kept

        Returns
        -------
        :
            The linked dataset
        """

        body = ProjectLinkDatasetBody(
            dataset_id=dataset_id, name=name, description=description, tagline=tagline, tags=tags
        )
        self._client.make_request("post", f"api/v1/projects/{self.id}/datasets/link", None, body=body)
        ds = self.get_dataset(dataset_id)

        dataset_meta = ProjectDatasetMetadata(
            dataset_id=ds.id,
            dataset_type=ds.dataset_type,
            name=ds.name,
            description=ds.description,
            tagline=ds.tagline,
            tags=ds.tags,
        )

        self._dataset_metadata.append(dataset_meta)

        return ds

    def unlink_datasets(
        self,
        dataset_ids: int | str | list[int | str],
        delete_datasets: bool = False,
        delete_dataset_records: bool = False,
    ):
        """
        Removes datasets from this project

        By default the datasets remain on the server and only the association with this project
        is removed.

        Parameters
        ----------
        dataset_ids
            Dataset IDs or project-local dataset names to remove from the project
        delete_datasets
            If True, also delete the datasets themselves from the server
        delete_dataset_records
            If True, also delete the records contained in those datasets
        """
        dataset_ids = make_list(dataset_ids)
        dataset_ids = [self._lookup_dataset_id(ds_id) if isinstance(ds_id, str) else ds_id for ds_id in dataset_ids]
        body = ProjectUnlinkDatasetsBody(
            dataset_ids=dataset_ids, delete_datasets=delete_datasets, delete_dataset_records=delete_dataset_records
        )

        self._client.make_request("post", f"api/v1/projects/{self.id}/datasets/unlink", None, body=body)
        self._dataset_metadata = [d for d in self._dataset_metadata if d.dataset_id not in dataset_ids]

    def get_dataset(self, dataset_id: int | str) -> BaseDataset:
        """
        Obtain a dataset contained in this project

        Parameters
        ----------
        dataset_id
            The dataset ID, or the name the dataset was given within this project

        Returns
        -------
        :
            The dataset, of the appropriate type for the dataset
        """
        if isinstance(dataset_id, str):
            dataset_id = self._lookup_dataset_id(dataset_id)

        ds_dict = self._client.make_request("get", f"api/v1/projects/{self.id}/datasets/{dataset_id}", dict[str, Any])

        # set prefix to be the main prefix of the server. We will fetch all the records/datasets
        # directly via the regular (non-project) endpoints
        return dataset_from_dict(ds_dict, client=self._client, base_url_prefix="api/v1")

    #############################
    # Attachments
    #############################
    def fetch_attachments(self):
        """
        Fetches the metadata for this project's attachments from the server

        This overwrites what is stored locally, and is available through the
        :attr:`attachments` property.
        """
        self.assert_online()

        self.attachments_ = self._client.make_request(
            "get", f"api/v1/projects/{self.id}/attachments", list[ProjectAttachment]
        )

        for att in self.attachments_:
            att.propagate_client(self._client)

    @property
    def attachments(self) -> list[ProjectAttachment]:
        """
        The files that have been uploaded to this project

        This is fetched from the server the first time it is accessed, and then cached.
        The returned objects contain only the file metadata - use
        :meth:`~qcportal.external_files.models.ExternalFile.download` to obtain the contents.
        """
        self.assert_online()
        if self.attachments_ is None:
            self.fetch_attachments()
        return self.attachments_

    def upload_attachment(
        self,
        file_path: str,
        attachment_type: ProjectAttachmentType,
        tags: list[str],
        description: str | None = None,
        provenance: dict[str, Any] | None = None,
        new_file_name: str | None = None,
    ) -> int:
        """
        Uploads a file to this project

        Parameters
        ----------
        file_path
            Path to the local file to upload
        attachment_type
            The kind of attachment this is. Currently only ``other`` is available
        tags
            List of tags to attach to this file
        description
            Optional longer description of this file
        provenance
            Optional dictionary describing the source of this file
        new_file_name
            Store the file under this name instead of the base name of ``file_path``

        Returns
        -------
        :
            ID of the newly-created attachment
        """
        self.assert_online()

        if provenance is None:
            provenance = {}

        if description is None:
            description = ""

        file_name = os.path.basename(file_path) if new_file_name is None else new_file_name
        file_info = [(file_name, file_path)]
        body = ProjectAttachmentUploadBody(
            file_name=file_name,
            description=description,
            provenance=provenance,
            attachment_type=attachment_type,
            tags=tags,
        )

        file_id = self._client.make_request(
            "post", f"api/v1/projects/{self.id}/attachments", int, body=body, upload_files=file_info
        )

        # Force refetch next time
        self.attachments_ = None
        return file_id

    def delete_attachment(self, file_id: int):
        """
        Deletes an attachment from this project

        Parameters
        ----------
        file_id
            ID of the attachment to delete
        """
        self.assert_online()
        self._client.make_request("delete", f"api/v1/projects/{self.id}/attachments/{file_id}", None)

        # Force refetch next time
        self.attachments_ = None
