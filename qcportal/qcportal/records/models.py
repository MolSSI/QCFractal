from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Union, Iterable, Set

from pydantic import BaseModel, Extra, constr
from qcelemental.models.results import Provenance

from ..base_models import (
    RestModelBase,
    QueryProjModelBase,
    ProjURLParameters,
)
from ..nativefiles import NativeFile
from ..outputstore import OutputStore, OutputTypeEnum


class PriorityEnum(int, Enum):
    """
    The priority of a Task. Higher priority will be pulled first.
    """

    high = 2
    normal = 1
    low = 0

    @classmethod
    def _missing_(cls, priority):
        """Attempts to find the correct priority in a case-insensitive way

        If a string being converted to a PriorityEnum is missing, then this function
        will convert the case and try to find the appropriate priority.
        """

        if isinstance(priority, int):
            # An integer that is outside the range of valid priorities
            return

        priority = priority.lower()

        # Search this way rather than doing 'in' since we are comparing
        # a string to an enum
        for p in cls:
            if priority == p.name:
                return p


class RecordStatusEnum(str, Enum):
    """
    The state of a record object. The states which are available are a finite set.
    """

    complete = "complete"
    waiting = "waiting"
    running = "running"
    error = "error"
    cancelled = "cancelled"
    invalid = "invalid"
    deleted = "deleted"

    @classmethod
    def _missing_(cls, name):
        """Attempts to find the correct status in a case-insensitive way

        If a string being converted to a RecordStatusEnum is missing, then this function
        will convert the case and try to find the appropriate status.
        """
        name = name.lower()

        # Search this way rather than doing 'in' since we are comparing
        # a string to an enum
        for status in cls:
            if name == status:
                return status


class ComputeHistory(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int
    status: RecordStatusEnum
    manager_name: Optional[str]
    modified_on: datetime
    provenance: Optional[Provenance]
    outputs: Optional[Dict[str, OutputStore]]

    def get_output(self, output_type: OutputTypeEnum) -> Optional[Union[str, Dict[str, Any]]]:
        if not self.outputs:
            return None

        o = self.outputs.get(output_type, None)
        if o is None:
            return None
        elif o.output_type == OutputTypeEnum.error:
            return o.as_json
        else:
            return o.as_string


class RecordInfoBackup(BaseModel):
    class Config:
        extra = Extra.forbid

    old_status: RecordStatusEnum
    old_tag: Optional[str]
    old_priority: Optional[PriorityEnum]
    modified_on: datetime


class RecordComment(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int
    username: Optional[str]
    timestamp: datetime
    comment: str


class TaskRecord(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int

    spec: Optional[Dict[str, Any]]
    tag: str
    priority: PriorityEnum
    required_programs: List[str]
    created_on: datetime


class ServiceDependency(BaseModel):
    id: int
    service_id: int
    record_id: int
    extras: Dict[str, Any]


class ServiceRecord(BaseModel):
    id: int
    record_id: int

    tag: str
    priority: PriorityEnum
    created_on: datetime

    service_state: Optional[Dict[str, Any]] = None
    dependencies: Optional[List[ServiceDependency]] = None


class BaseRecord(BaseModel):
    class _DataModel(BaseModel):
        class Config:
            extra = Extra.forbid
            allow_mutation = True
            validate_assignment = True

        id: int
        record_type: str
        is_service: bool

        extras: Optional[Dict[str, Any]] = None

        status: RecordStatusEnum
        manager_name: Optional[str]

        created_on: datetime
        modified_on: datetime

        compute_history: List[ComputeHistory]

        task: Optional[TaskRecord] = None
        service: Optional[ServiceRecord] = None

        comments: Optional[List[RecordComment]] = None

        native_files: Optional[Dict[str, NativeFile]] = None

        info_backup: Optional[List[RecordInfoBackup]]

    class Config:
        extra = Extra.forbid

    record_type: str
    client: Any
    raw_data: _DataModel  # Meant to be overridden by derived classes

    @classmethod
    def from_datamodel(cls, raw_data: _DataModel, client: Any = None):
        return cls(client=client, raw_data=raw_data, record_type=raw_data.record_type)

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:
        """
        Transforms user-friendly includes into includes used by the web API
        """

        if includes is None:
            return None

        ret: Set[str] = {"*"}

        if "task" in includes:
            ret.add("task")
        if "service" in includes:
            ret |= {"service.*", "service.dependencies"}
        if "outputs" in includes:
            ret |= {"compute_history.*", "compute_history.outputs"}
        if "comments" in includes:
            ret.add("comments")

        return ret

    def _assert_online(self):
        if self.offline:
            raise RuntimeError("Record is not connected to a client")

    def _fetch_compute_history(self, include_outputs: bool = False):
        url_params = {}

        if include_outputs:
            url_params = {"include": ["*", "outputs"]}

        self.raw_data.compute_history = self.client._auto_request(
            "get",
            f"v1/records/{self.raw_data.id}/compute_history",
            None,
            ProjURLParameters,
            List[ComputeHistory],
            None,
            url_params,
        )

    def _fetch_task(self):
        self._assert_online()

        if self.raw_data.is_service:
            return

        self.raw_data.task = self.client._auto_request(
            "get",
            f"v1/records/{self.raw_data.id}/task",
            None,
            None,
            Optional[TaskRecord],
            None,
            None,
        )

    def _fetch_service(self):
        self._assert_online()

        if not self.raw_data.is_service:
            return

        self.raw_data.service = self.client._auto_request(
            "get",
            f"v1/records/{self.raw_data.id}/service",
            None,
            None,
            Optional[ServiceRecord],
            None,
            None,
        )

    def _fetch_comments(self):
        self._assert_online()

        self.raw_data.comments = self.client._auto_request(
            "get",
            f"v1/records/{self.raw_data.id}/comments",
            None,
            None,
            Optional[List[RecordComment]],
            None,
            None,
        )

    def _fetch_native_files(self):
        self._assert_online()

        self.raw_data.native_files = self.client._auto_request(
            "get",
            f"v1/records/{self.raw_data.id}/native_files",
            None,
            None,
            Optional[Dict[str, NativeFile]],
            None,
            None,
        )

    def _get_output(self, output_type: OutputTypeEnum) -> Optional[Union[str, Dict[str, Any]]]:
        if not self.raw_data.compute_history:
            self._fetch_compute_history(include_outputs=True)

        if not self.raw_data.compute_history:
            return None

        last_computation = self.raw_data.compute_history[-1]
        if last_computation.outputs is None:
            self._fetch_compute_history(include_outputs=True)
            last_computation = self.raw_data.compute_history[-1]

        return last_computation.get_output(output_type)

    @property
    def offline(self) -> bool:
        return self.client is None

    @property
    def id(self) -> int:
        return self.raw_data.id

    @property
    def is_service(self) -> bool:
        return self.raw_data.is_service

    @property
    def extras(self) -> Optional[Dict[str, Any]]:
        return self.raw_data.extras

    @property
    def status(self):
        return self.raw_data.status

    @property
    def manager_name(self) -> Optional[str]:
        return self.raw_data.manager_name

    @property
    def created_on(self) -> datetime:
        return self.raw_data.created_on

    @property
    def modified_on(self) -> datetime:
        return self.raw_data.modified_on

    @property
    def compute_history(self) -> List[ComputeHistory]:
        return self.raw_data.compute_history

    @property
    def task(self) -> Optional[TaskRecord]:
        if self.raw_data.task is None:
            self._fetch_task()
        return self.raw_data.task

    @property
    def service(self) -> Optional[ServiceRecord]:
        if self.raw_data.service is None:
            self._fetch_service()
        return self.raw_data.service

    @property
    def comments(self) -> Optional[List[RecordComment]]:
        if self.raw_data.comments is None:
            self._fetch_comments()
        return self.raw_data.comments

    @property
    def native_files(self) -> Optional[Dict[str, NativeFile]]:
        if self.raw_data.native_files is None:
            self._fetch_native_files()
        return self.raw_data.native_files

    @property
    def stdout(self) -> Optional[str]:
        return self._get_output(OutputTypeEnum.stdout)

    @property
    def stderr(self) -> Optional[str]:
        return self._get_output(OutputTypeEnum.stderr)

    @property
    def error(self) -> Optional[Dict[str, Any]]:
        return self._get_output(OutputTypeEnum.error)


ServiceDependency.update_forward_refs()


class RecordAddBodyBase(RestModelBase):
    tag: constr(to_lower=True)
    priority: PriorityEnum


class RecordModifyBody(RestModelBase):
    record_ids: List[int]
    status: Optional[RecordStatusEnum] = None
    priority: Optional[PriorityEnum] = None
    tag: Optional[str] = None
    comment: Optional[str] = None


class RecordDeleteBody(RestModelBase):
    record_ids: List[int]
    soft_delete: bool
    delete_children: bool


class RecordRevertBody(RestModelBase):
    revert_status: RecordStatusEnum
    record_ids: List[int]


class RecordQueryFilters(QueryProjModelBase):
    record_id: Optional[List[int]] = None
    record_type: Optional[List[str]] = None
    manager_name: Optional[List[str]] = None
    status: Optional[List[RecordStatusEnum]] = None
    dataset_id: Optional[List[int]] = None
    parent_id: Optional[List[int]] = None
    child_id: Optional[List[int]] = None
    created_before: Optional[datetime] = None
    created_after: Optional[datetime] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None
