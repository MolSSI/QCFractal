from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Union, Iterable, Set, Tuple, Type, Sequence, ClassVar

from dateutil.parser import parse as date_parser
from pydantic import BaseModel, Extra, constr, validator, PrivateAttr, Field
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

    # This ordering shouldn't change in the near future, as it conflicts
    # a bit with some migration testing
    complete = "complete"
    invalid = "invalid"
    running = "running"
    error = "error"
    waiting = "waiting"
    cancelled = "cancelled"
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

    function: Optional[str]
    function_kwargs_lb_id: Optional[int]

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

    owner_user: Optional[str]
    owner_group: Optional[str]

    ######################################################
    # Fields not always included when fetching the record
    ######################################################
    compute_history_: Optional[List[ComputeHistory]] = Field(None, alias="compute_history")
    task_: Optional[TaskRecord] = Field(None, alias="task")
    service_: Optional[ServiceRecord] = Field(None, alias="service")
    comments_: Optional[List[RecordComment]] = Field(None, alias="comments")
    native_files_: Optional[Dict[str, NativeFile]] = Field(None, alias="native_files")

    # Private non-pydantic fields
    _client: Any = PrivateAttr(None)
    """ Client connected to the server that this record belongs to """

    # A dictionary of all subclasses (calculation types) to actual class type
    _all_subclasses: ClassVar[Dict[str, Type[BaseRecord]]] = {}

    def __init__(self, client=None, **kwargs):
        BaseModel.__init__(self, **kwargs)

        # Calls derived class propagate_client & make_caches,
        # which should filter down to the ones in this (BaseRecord) class
        self.propagate_client(client)
        self.make_caches()

        assert self._client is client, "Client not set in base record class?"

    def __init_subclass__(cls):
        """
        Register derived classes for later use
        """

        # Get the record type. This is kind of ugly, but works.
        # We could use ClassVar, but in my tests it doesn't work for
        # disambiguating (ie, via parse_obj_as)
        record_type = cls.__fields__["record_type"].default
        cls._all_subclasses[record_type] = cls

    @classmethod
    def get_subclass(cls, record_type: str) -> Type[BaseRecord]:
        """
        Obtain a subclass of this class given its record_type
        """

        subcls = cls._all_subclasses.get(record_type)
        if subcls is None:
            raise RuntimeError(f"Cannot find subclass for record type {record_type}")
        return subcls

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} id={self.id} status={self.status}>"

    def propagate_client(self, client):
        """
        Propagates a client to this record to any fields within this record that need it

        This is expected to be called from derived class propagate_client functions as well
        """
        self._client = client

    def make_caches(self):
        """
        Prepare any internal caches

        This is expected to be called from derived class make_caches as well
        """
        pass

    def _assert_online(self):
        """Raises an exception if this record does not have an associated client"""
        if self.offline:
            raise RuntimeError("Record is not connected to a client")

    def _fetch_compute_history(self, include_outputs: bool = False):
        self._assert_online()

        url_params = {}

        if include_outputs:
            url_params = {"include": ["*", "outputs"]}

        self.compute_history_ = self._client._auto_request(
            "get",
            f"v1/records/{self.id}/compute_history",
            None,
            ProjURLParameters,
            List[ComputeHistory],
            None,
            url_params,
        )

    def _fetch_task(self):
        self._assert_online()

        if self.is_service:
            return

        self.task_ = self._client._auto_request(
            "get",
            f"v1/records/{self.id}/task",
            None,
            None,
            Optional[TaskRecord],
            None,
            None,
        )

    def _fetch_service(self):
        self._assert_online()

        if not self.is_service:
            return

        url_params = {"include": ["*", "dependencies"]}

        self.service_ = self._client._auto_request(
            "get",
            f"v1/records/{self.id}/service",
            None,
            ProjURLParameters,
            Optional[ServiceRecord],
            None,
            url_params,
        )

    def _fetch_comments(self):
        self._assert_online()

        self.comments_ = self._client._auto_request(
            "get",
            f"v1/records/{self.id}/comments",
            None,
            None,
            Optional[List[RecordComment]],
            None,
            None,
        )

    def _fetch_native_files(self):
        self._assert_online()

        self.native_files_ = self._client._auto_request(
            "get",
            f"v1/records/{self.id}/native_files",
            None,
            None,
            Optional[Dict[str, NativeFile]],
            None,
            None,
        )

    def _get_last_compute_history(self, include_outputs: bool = False) -> Optional[ComputeHistory]:
        if self.compute_history_ is None:
            self._fetch_compute_history(include_outputs=include_outputs)

        if not self.compute_history_:
            return None

        # If we want outputs but we don't have them
        if include_outputs and not self.compute_history_[-1].outputs:
            self._fetch_compute_history(include_outputs=include_outputs)

        return self.compute_history_[-1]

    def _get_output(self, output_type: OutputTypeEnum) -> Optional[Union[str, Dict[str, Any]]]:
        last_history = self._get_last_compute_history(include_outputs=True)
        if last_history is None:
            return None

        return last_history.get_output(output_type)

    def _handle_includes(self, includes: Optional[Iterable[str]]):
        """
        Fetches information specified by some iterable of strings
        """
        if includes is None:
            return

        if "task" in includes:
            self._fetch_task()
        if "service" in includes:
            self._fetch_task()
        if "compute_history" in includes and "outputs" not in includes:
            self._fetch_compute_history(False)
        if "outputs" in includes:
            self._fetch_compute_history(True)
        if "comments" in includes:
            self._fetch_comments()
        if "native_files" in includes:
            self._fetch_native_files()

    @property
    def offline(self) -> bool:
        return self._client is None

    @property
    def compute_history(self) -> List[ComputeHistory]:
        if self.compute_history_ is None:
            self._fetch_compute_history()
        return self.compute_history_

    @property
    def task(self) -> Optional[TaskRecord]:
        if self.task_ is None:
            self._fetch_task()
        return self.task_

    @property
    def service(self) -> Optional[ServiceRecord]:
        if self.service_ is None:
            self._fetch_service()
        return self.service_

    @property
    def comments(self) -> Optional[List[RecordComment]]:
        if self.comments_ is None:
            self._fetch_comments()
        return self.comments_

    @property
    def native_files(self) -> Optional[Dict[str, NativeFile]]:
        if self.native_files_ is None:
            self._fetch_native_files()
        return self.native_files_

    @property
    def stdout(self) -> Optional[str]:
        return self._get_output(OutputTypeEnum.stdout)

    @property
    def stderr(self) -> Optional[str]:
        return self._get_output(OutputTypeEnum.stderr)

    @property
    def error(self) -> Optional[Dict[str, Any]]:
        return self._get_output(OutputTypeEnum.error)

    @property
    def provenance(self) -> Optional[Provenance]:
        last_history = self._get_last_compute_history(include_outputs=False)
        if last_history is None:
            return None
        return last_history.provenance


ServiceDependency.update_forward_refs()


class RecordAddBodyBase(RestModelBase):
    tag: constr(to_lower=True)
    priority: PriorityEnum
    owner_group: Optional[str]


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
    owner_user: Optional[List[Union[int, str]]] = None
    owner_group: Optional[List[Union[int, str]]] = None

    @validator("created_before", "created_after", "modified_before", "modified_after", pre=True)
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class RecordQueryIterator(QueryIteratorBase):
    """
    Iterator for all types of record queries

    This iterator transparently handles batching and pagination over the results
    of a record query, and works with all kinds of records.
    """

    def __init__(
        self,
        client,
        query_filters: RecordQueryFilters,
        record_type: Optional[str],
        include: Optional[Iterable[str]] = None,
    ):
        """
        Construct an iterator

        Parameters
        ----------
        client
            QCPortal client object used to contact/retrieve data from the server
        query_filters
            The actual query information to send to the server
        record_type
            What type of record we are querying for
        """

        batch_limit = client.api_limits["get_records"] // 4
        self.record_type = record_type
        self.include = include

        QueryIteratorBase.__init__(self, client, query_filters, batch_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[BaseRecord]]:
        if self.record_type is None:
            meta, raw_data = self._client._auto_request(
                "post",
                f"v1/records/query",
                type(self._query_filters),  # Pass through as is
                None,
                Tuple[Optional[QueryMetadata], List[Dict[str, Any]]],
                self._query_filters,
                None,
            )
        else:
            meta, raw_data = self._client._auto_request(
                "post",
                f"v1/records/{self.record_type}/query",
                type(self._query_filters),  # Pass through as is
                None,
                Tuple[Optional[QueryMetadata], List[Dict[str, Any]]],
                self._query_filters,
                None,
            )

        records = records_from_dicts(raw_data, self._client)

        if self.include:
            for r in records:
                r._handle_includes(self.include)

        return meta, records


def record_from_dict(data: Dict[str, Any], client: Any = None) -> BaseRecord:
    """
    Create a record object from a dictionary containing the record information

    This determines the appropriate record class (deriving from BaseRecord)
    and creates an instance of that class.
    """

    record_type = data["record_type"]
    cls = BaseRecord.get_subclass(record_type)
    return cls(**data, client=client)


def records_from_dicts(
    data: Sequence[Optional[Dict[str, Any]]],
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
            ret.append(record_from_dict(rd, client))
    return ret
