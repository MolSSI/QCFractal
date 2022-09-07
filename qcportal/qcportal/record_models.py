from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Union, Iterable, Set, Tuple, Sequence

from pydantic import BaseModel, Extra, constr
from qcelemental.models.results import Provenance

from qcportal.base_models import (
    RestModelBase,
    QueryProjModelBase,
    ProjURLParameters,
    QueryIteratorBase,
)
from qcportal.metadata_models import QueryMetadata
from qcportal.nativefiles import NativeFile
from qcportal.outputstore import OutputStore, OutputTypeEnum


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
    class Config:
        extra = Extra.forbid

    service_id: int
    record_id: int
    extras: Dict[str, Any]


class ServiceRecord(BaseModel):
    class Config:
        extra = Extra.forbid

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

    client: Any
    raw_data: _DataModel  # Meant to be overridden by derived classes

    # all subclasses
    _all_subclasses = {}

    def __init_subclass__(cls):
        """
        Register derived classes for later use
        """

        # Get the record type. This is kind of ugly, but works.
        # We could use ClassVar, but in my tests it doesn't work for
        # disambiguating (ie, via parse_obj_as)
        record_type = cls._DataModel.__fields__["record_type"].default

        cls._all_subclasses[record_type] = cls

    @classmethod
    def get_subclass(cls, record_type: str):
        subcls = cls._all_subclasses.get(record_type)
        if subcls is None:
            raise RuntimeError(f"Cannot find subclass for record type {record_type}")
        return subcls

    @classmethod
    def from_datamodel(cls, raw_data: Union[BaseRecord._DataModel, Dict[str, Any]], client: Any = None) -> BaseRecord:
        """
        Create a record from a record DataModel (as an object or a dictionary)
        """
        return cls(client=client, raw_data=raw_data)

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

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} id={self.id} status={self.status}>"

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

        url_params = {"include": ["*", "dependencies"]}

        self.raw_data.service = self.client._auto_request(
            "get",
            f"v1/records/{self.raw_data.id}/service",
            None,
            ProjURLParameters,
            Optional[ServiceRecord],
            None,
            url_params,
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
    def record_type(self) -> str:
        return self.raw_data.record_type

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


class RecordQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: RecordQueryFilters, record_type: Optional[str]):
        api_limit = client.api_limits["get_records"] // 4
        self.record_type = record_type

        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[BaseRecord]]:
        if self.record_type is None:
            meta, raw_data = self.client._auto_request(
                "post",
                f"v1/records/query",
                type(self.query_filters),  # Pass through as is
                None,
                Tuple[Optional[QueryMetadata], List[Dict[str, Any]]],
                self.query_filters,
                None,
            )
        else:
            meta, raw_data = self.client._auto_request(
                "post",
                f"v1/records/{self.record_type}/query",
                type(self.query_filters),  # Pass through as is
                None,
                Tuple[Optional[QueryMetadata], List[Dict[str, Any]]],
                self.query_filters,
                None,
            )

        return meta, records_from_datamodels(raw_data, self.client)


def record_from_datamodel(data: Union[BaseRecord._DataModel, Dict[str, Any]], client: Any) -> BaseRecord:
    """
    Create a record object from a datamodel

    This determines the appropriate record class (deriving from BaseRecord)
    and creates an instance of that class.

    This works if the data is a datamodel object already or a dictionary
    """

    if isinstance(data, BaseRecord._DataModel):
        record_type = data.record_type
    else:
        record_type = data["record_type"]

    cls = BaseRecord.get_subclass(record_type)
    return cls.from_datamodel(data, client)


def records_from_datamodels(
    data: Sequence[Optional[Union[BaseRecord._DataModel, Dict[str, Any]]]],
    client: Any,
) -> List[Optional[BaseRecord]]:
    """
    Create a list of record objects from a sequence of datamodels

    This determines the appropriate record class (deriving from BaseRecord)
    and creates an instance of that class.
    """

    ret: List[Optional[BaseRecord]] = []
    for rd in data:
        if rd is None:
            ret.append(None)
        else:
            ret.append(record_from_datamodel(rd, client))
    return ret
