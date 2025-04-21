from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Optional, List, Dict, Any, Tuple, Union

from qcportal.all_inputs import AllInputTypes
from qcportal.base_models import RestModelBase, validate_list_to_single
from qcportal.dataset_models import BaseDataset, dataset_from_dict
from qcportal.external_files import ExternalFile
from qcportal.metadata_models import InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum, RecordStatusEnum, RecordAddBodyBase, record_from_dict, BaseRecord
from qcportal.utils import make_list

try:
    import pydantic.v1 as pydantic
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr, Field
except ImportError:
    import pydantic
    from pydantic import BaseModel, Extra, validator, PrivateAttr, Field, root_validator

if TYPE_CHECKING:
    from qcportal.client import PortalClient


class ProjectAttachmentType(str, Enum):
    """
    The type of attachment a file is for a dataset
    """

    other = "other"


class ProjectAttachment(ExternalFile):
    attachment_type: ProjectAttachmentType
    tags: List[str]


class ProjectQueryModel(RestModelBase):
    project_name: Optional[str] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class ProjectDeleteParams(RestModelBase):
    delete_records: bool = False
    delete_datasets: bool = False
    delete_dataset_records: bool = False

    @validator("delete_records", "delete_datasets", "delete_dataset_records", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class ProjectAddBody(RestModelBase):
    name: str
    description: str
    tagline: str
    tags: List[str]
    default_compute_tag: str
    default_compute_priority: PriorityEnum
    extras: Dict[str, Any]
    existing_ok: bool = False


# This is basically a duplicate of DatasetAddBody, but with
# dataset_type added. Ok to duplicate because we will eventually move
# to projects-only
class ProjectDatasetAddBody(RestModelBase):
    dataset_type: str
    name: str
    description: str
    tagline: str
    tags: List[str]
    provenance: Dict[str, Any]
    default_compute_tag: str
    default_compute_priority: PriorityEnum
    extras: Dict[str, Any]
    existing_ok: bool = False


class ProjectLinkDatasetBody(RestModelBase):
    dataset_id: int
    name: Optional[str]
    description: Optional[str]
    tagline: Optional[str]
    tags: Optional[List]


class ProjectUnlinkDatasetsBody(RestModelBase):
    dataset_ids: List[int]
    delete_datasets: bool
    delete_dataset_records: bool


class ProjectRecordAddBody(RecordAddBodyBase):
    record_input: AllInputTypes = Field(..., discriminator="record_type")
    name: str
    description: str
    tags: List[str]


class ProjectLinkRecordBody(RestModelBase):
    record_id: int
    name: str
    description: str
    tags: List[str]


class ProjectUnlinkRecordsBody(RestModelBase):
    record_ids: List[int]
    delete_records: bool


class ProjectRecordMetadata(BaseModel):
    record_id: int
    name: str
    description: str
    tags: List[str]

    record_type: str
    status: RecordStatusEnum


class ProjectDatasetMetadata(BaseModel):
    dataset_id: int
    dataset_type: str
    name: str
    description: str
    tagline: str
    tags: List[str]


class Project(BaseModel):
    class Config:
        extra = Extra.forbid
        allow_mutation = True
        validate_assignment = True

    id: int
    name: str
    description: str
    tagline: str
    tags: List[str]

    default_compute_tag: str
    default_compute_priority: PriorityEnum

    owner_user: Optional[str]

    extras: Dict[str, Any]

    ########################################
    # Caches of information
    ########################################
    _record_metadata: List[ProjectRecordMetadata] = PrivateAttr([])
    _dataset_metadata: List[ProjectDatasetMetadata] = PrivateAttr([])
    _molecules: List[Molecule] = PrivateAttr([])

    ######################################################
    # Fields not always included when fetching the dataset
    ######################################################
    attachments_: Optional[List[ProjectAttachment]] = Field(None, alias="attachments")

    #############################
    # Private non-pydantic fields
    #############################
    _client: Any = PrivateAttr(None)

    @property
    def offline(self) -> bool:
        return self._client is None

    def assert_online(self):
        if self.offline:
            raise RuntimeError("Project is not connected to a QCFractal server")

    def __init__(self, client: Optional[PortalClient] = None, **kwargs):
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
    def dataset_metadata(self) -> List[ProjectDatasetMetadata]:
        self.assert_online()
        if len(self._dataset_metadata) == 0:
            self.fetch_dataset_metadata()
        return self._dataset_metadata

    @property
    def record_metadata(self) -> List[ProjectRecordMetadata]:
        self.assert_online()
        if len(self._record_metadata) == 0:
            self.fetch_record_metadata()
        return self._record_metadata

    def status(self) -> Dict[str, Dict[RecordStatusEnum, int]]:
        self.assert_online()

        return self._client.make_request(
            "get", f"api/v1/projects/{self.id}/status", Dict[str, Dict[RecordStatusEnum, int]]
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
        self.assert_online()

        self._record_metadata = self._client.make_request(
            "get", f"api/v1/projects/{self.id}/record_metadata", List[ProjectRecordMetadata]
        )

    def add_record(
        self,
        name: str,
        record_input: AllInputTypes,
        *,
        description: Optional[str] = None,
        tags: List[str] = None,
        compute_tag: Optional[str] = None,
        compute_priority: Optional[PriorityEnum] = None,
        find_existing: bool = True,
    ) -> BaseRecord:

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
            "post", f"api/v1/projects/{self.id}/records", Tuple[InsertCountsMetadata, int], body=body_data
        )

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
        tags: List[str],
    ) -> BaseRecord:

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

    def unlink_records(self, record_ids: Union[int, str, List[Union[int, str]]], delete_records: bool = False):
        record_ids = make_list(record_ids)
        record_ids = [self._lookup_record_id(rid) if isinstance(rid, str) else rid for rid in record_ids]
        body = ProjectUnlinkRecordsBody(record_ids=record_ids, delete_records=delete_records)

        self._client.make_request("post", f"api/v1/projects/{self.id}/records/unlink", None, body=body)
        self._record_metadata = [r for r in self._record_metadata if r.record_id not in record_ids]

    def get_record(self, record_id: Union[int, str]) -> BaseRecord:

        if isinstance(record_id, str):
            record_id = self._lookup_record_id(record_id)

        r_dict = self._client.make_request("get", f"api/v1/projects/{self.id}/records/{record_id}", Dict[str, Any])

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
        self.assert_online()

        self._dataset_metadata = self._client.make_request(
            "get",
            f"api/v1/projects/{self.id}/dataset_metadata",
            List[ProjectDatasetMetadata],
        )

    def add_dataset(
        self,
        dataset_type: str,
        name: str,
        description: Optional[str] = None,
        tagline: Optional[str] = None,
        tags: Optional[List[str]] = None,
        provenance: Optional[Dict[str, Any]] = None,
        default_compute_tag: Optional[str] = None,
        default_compute_priority: Optional[PriorityEnum] = None,
        extras: Optional[Dict[str, Any]] = None,
        existing_ok: bool = False,
    ) -> BaseDataset:

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
        name: Optional[str] = None,
        description: Optional[str] = None,
        tagline: Optional[str] = None,
        tags: Optional[List] = None,
    ) -> BaseDataset:

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
        dataset_ids: Union[int, str, List[Union[int, str]]],
        delete_datasets: bool = False,
        delete_dataset_records: bool = False,
    ):
        dataset_ids = make_list(dataset_ids)
        dataset_ids = [self._lookup_dataset_id(ds_id) if isinstance(ds_id, str) else ds_id for ds_id in dataset_ids]
        body = ProjectUnlinkDatasetsBody(
            dataset_ids=dataset_ids, delete_datasets=delete_datasets, delete_dataset_records=delete_dataset_records
        )

        self._client.make_request("post", f"api/v1/projects/{self.id}/datasets/unlink", None, body=body)
        self._dataset_metadata = [d for d in self._dataset_metadata if d.dataset_id not in dataset_ids]

    def get_dataset(self, dataset_id: Union[int, str]) -> BaseDataset:
        if isinstance(dataset_id, str):
            dataset_id = self._lookup_dataset_id(dataset_id)

        ds_dict = self._client.make_request("get", f"api/v1/projects/{self.id}/datasets/{dataset_id}", Dict[str, Any])

        # set prefix to be the main prefix of the server. We will fetch all the records/datasets
        # directly via the regular (non-project) endpoints
        return dataset_from_dict(ds_dict, client=self._client, base_url_prefix="api/v1")
