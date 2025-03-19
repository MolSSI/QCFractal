from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Union, Iterable, Tuple, Type, Sequence, ClassVar, TypeVar

from dateutil.parser import parse as date_parser

try:
    from pydantic.v1 import BaseModel, Extra, constr, validator, PrivateAttr, Field, parse_obj_as, root_validator
except ImportError:
    from pydantic import BaseModel, Extra, constr, validator, PrivateAttr, Field, parse_obj_as, root_validator
from qcelemental.models.results import Provenance

from qcportal.base_models import (
    RestModelBase,
    QueryModelBase,
    QueryIteratorBase,
)

from qcportal.cache import RecordCache, get_records_with_cache
from qcportal.compression import CompressionEnum, decompress, get_compressed_ext

_T = TypeVar("_T")


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

    @classmethod
    def make_ordered_status(cls, statuses: Iterable[RecordStatusEnum]) -> List[RecordStatusEnum]:
        """Returns a list of the given statuses but in a defined order"""
        order = [cls.complete, cls.error, cls.running, cls.waiting, cls.cancelled, cls.invalid, cls.deleted]
        return sorted(statuses, key=lambda x: order.index(x))


class OutputTypeEnum(str, Enum):
    """
    What type of data is stored
    """

    stdout = "stdout"
    stderr = "stderr"
    error = "error"


class OutputStore(BaseModel):
    """
    Storage of outputs and error messages, with optional compression
    """

    class Config:
        extra = Extra.forbid

    output_type: OutputTypeEnum = Field(..., description="The type of output this is (stdout, error, etc)")
    compression_type: CompressionEnum = Field(CompressionEnum.none, description="Compression method (such as lzma)")
    data_: Optional[bytes] = Field(None, alias="data")

    _data_url: Optional[str] = PrivateAttr(None)
    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, history_base_url):
        self._client = client
        self._data_url = f"{history_base_url}/outputs/{self.output_type.value}/data"

    def _fetch_raw_data(self):
        if self.data_ is not None:
            return

        if self._client is None:
            raise RuntimeError("No client to fetch output data from")

        cdata, ctype = self._client.make_request(
            "get",
            self._data_url,
            Tuple[bytes, CompressionEnum],
        )

        assert self.compression_type == ctype
        self.data_ = cdata

    @property
    def data(self) -> Any:
        self._fetch_raw_data()
        return decompress(self.data_, self.compression_type)


class ComputeHistory(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int
    status: RecordStatusEnum
    manager_name: Optional[str]
    modified_on: datetime
    provenance: Optional[Provenance]
    outputs_: Optional[Dict[str, OutputStore]] = Field(None, alias="outputs")

    _client: Any = PrivateAttr(None)
    _base_url: Optional[str] = PrivateAttr(None)

    def propagate_client(self, client, record_base_url):
        self._client = client
        self._base_url = f"{record_base_url}/compute_history/{self.id}"

        if self.outputs_ is not None:
            for o in self.outputs_.values():
                o.propagate_client(self._client, self._base_url)

    def fetch_all(self):
        self._fetch_outputs()

    def _fetch_outputs(self):
        if self._client is None:
            raise RuntimeError("This compute history is not connected to a client")

        self.outputs_ = self._client.make_request(
            "get",
            f"{self._base_url}/outputs",
            Dict[str, OutputStore],
        )

        for o in self.outputs_.values():
            o.propagate_client(self._client, self._base_url)
            o._fetch_raw_data()

    @property
    def outputs(self) -> Dict[str, OutputStore]:
        if self.outputs_ is None:
            self._fetch_outputs()
        return self.outputs_

    def get_output(self, output_type: OutputTypeEnum) -> Any:
        if not self.outputs:
            return None

        o = self.outputs.get(output_type, None)
        if o is None:
            return None
        else:
            return o.data

    @property
    def stdout(self) -> Any:
        return self.get_output("stdout")

    @property
    def stderr(self) -> Any:
        return self.get_output("stderr")

    @property
    def error(self) -> Any:
        return self.get_output("error")


class NativeFile(BaseModel):
    """
    Storage of native files, with compression
    """

    class Config:
        extra = Extra.forbid

    name: str = Field(..., description="Name of the file")
    compression_type: CompressionEnum = Field(..., description="Compression method (such as lzma)")
    data_: Optional[bytes] = Field(None, alias="data")

    _data_url: Optional[str] = PrivateAttr(None)
    _client: Any = PrivateAttr(None)

    def propagate_client(self, client, record_base_url):
        self._client = client
        self._data_url = f"{record_base_url}/native_files/{self.name}/data"

    def fetch_all(self):
        self._fetch_raw_data()

    def _fetch_raw_data(self):
        if self.data_ is not None:
            return

        if self._client is None:
            raise RuntimeError("No client to fetch native file data from")

        cdata, ctype = self._client.make_request(
            "get",
            self._data_url,
            Tuple[bytes, CompressionEnum],
        )

        assert self.compression_type == ctype
        self.data_ = cdata

    @property
    def data(self) -> Any:
        self._fetch_raw_data()
        return decompress(self.data_, self.compression_type)

    def save_file(
        self, directory: str, new_name: Optional[str] = None, keep_compressed: bool = False, overwrite: bool = False
    ):
        """
        Saves the file to the given directory
        """

        if new_name is None:
            name = self.name
        else:
            name = new_name

        if keep_compressed:
            name += get_compressed_ext(self.compression_type)

        full_path = os.path.join(directory, name)
        if os.path.exists(full_path) and not overwrite:
            raise RuntimeError(f"File {full_path} already exists. Not overwriting")

        if keep_compressed:
            with open(full_path, "wb") as f:
                f.write(self.data)
        else:
            d = self.data

            # TODO - streaming decompression?
            if isinstance(d, str):
                with open(full_path, "wt") as f:
                    f.write(self.data)
            elif isinstance(d, bytes):
                with open(full_path, "wb") as f:
                    f.write(self.data)
            else:
                raise RuntimeError(f"Cannot write data of type {type(d)} to a file")


class RecordComment(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int
    username: Optional[str]
    timestamp: datetime
    comment: str


class RecordTask(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int

    function: Optional[str]
    function_kwargs_compressed: Optional[bytes]

    compute_tag: str
    compute_priority: PriorityEnum
    required_programs: List[str]

    # TODO - DEPRECATED - remove at some point
    @property
    def tag(self) -> str:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning("'tag' is deprecated and will be removed in a future release. Use 'compute_tag' instead")
        return self.compute_tag

    @property
    def priority(self) -> PriorityEnum:
        logger = logging.getLogger(self.__class__.__name__)
        logger.warning(
            "'priority' is deprecated and will be removed in a future release. Use 'compute_priority' instead"
        )
        return self.compute_priority

    @property
    def function_kwargs(self) -> Optional[Dict[str, Any]]:
        if self.function_kwargs_compressed is None:
            return None
        else:
            return decompress(self.function_kwargs_compressed, CompressionEnum.zstd)

    # TODO - DEPRECATED - remove at some point
    @root_validator(pre=True)
    def _old_tag_priority(cls, values):
        if "tag" in values:
            values["compute_tag"] = values.pop("tag")
        if "priority" in values:
            values["compute_priority"] = values.pop("priority")

        return values


class ServiceDependency(BaseModel):
    class Config:
        extra = Extra.forbid

    record_id: int
    extras: Dict[str, Any]


class RecordService(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int

    compute_tag: str
    compute_priority: PriorityEnum
    find_existing: bool

    service_state: Optional[Dict[str, Any]] = None
    dependencies: List[ServiceDependency]

    @root_validator(pre=True)
    def _old_tag_priority(cls, values):
        if "tag" in values:
            values["compute_tag"] = values.pop("tag")
        if "priority" in values:
            values["compute_priority"] = values.pop("priority")

        return values


class BaseRecord(BaseModel):
    class Config:
        extra = Extra.forbid
        allow_mutation = True
        validate_assignment = True

    id: int
    record_type: str
    is_service: bool

    properties: Optional[Dict[str, Any]]
    extras: Dict[str, Any] = Field({})

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
    task_: Optional[RecordTask] = Field(None, alias="task")
    service_: Optional[RecordService] = Field(None, alias="service")
    comments_: Optional[List[RecordComment]] = Field(None, alias="comments")
    native_files_: Optional[Dict[str, NativeFile]] = Field(None, alias="native_files")

    # Private non-pydantic fields
    _client: Any = PrivateAttr(None)
    _base_url: str = PrivateAttr(None)

    # A dictionary of all subclasses (calculation types) to actual class type
    _all_subclasses: ClassVar[Dict[str, Type[BaseRecord]]] = {}

    # Local record cache we can use for child records
    # This record may also be part of the cache
    _record_cache: Optional[RecordCache] = PrivateAttr(None)
    _cache_dirty: bool = PrivateAttr(False)

    def __init__(self, client=None, **kwargs):
        BaseModel.__init__(self, **kwargs)

        # Calls derived class propagate_client
        # which should filter down to the ones in this (BaseRecord) class
        self.propagate_client(client)

        assert self._client is client, "Client not set in base record class?"

    @validator("extras", pre=True)
    def _validate_extras(cls, v):
        # For backwards compatibility. Older servers may have 'None' as the extras
        if v is None:
            return {}
        return v

    def __init_subclass__(cls):
        """
        Register derived classes for later use
        """

        # Get the record type. This is kind of ugly, but works.
        # We could use ClassVar, but in my tests it doesn't work for
        # disambiguating (ie, via parse_obj_as)
        record_type = cls.__fields__["record_type"].default
        cls._all_subclasses[record_type] = cls

    def __del__(self):
        # Sometimes this won't exist if there is an exception during construction

        # TODO - we check sys.meta_path. Pydantic attempts an import of something, which is
        #        not good if the interpreter is shutting down. This is a hack to avoid that.
        #        Pydantic v2 may fix this
        if (
            hasattr(self, "_record_cache")
            and self._record_cache is not None
            and not self._record_cache.read_only
            and self._cache_dirty
            and sys.meta_path is not None
        ):
            self.sync_to_cache(True)  # Don't really *have* to detach, but why not

        s = super()
        if hasattr(s, "__del__"):
            s.__del__(self)

    @classmethod
    def get_subclass(cls, record_type: str) -> Type[BaseRecord]:
        """
        Obtain a subclass of this class given its record_type
        """

        subcls = cls._all_subclasses.get(record_type)
        if subcls is None:
            raise RuntimeError(f"Cannot find subclass for record type {record_type}")
        return subcls

    @classmethod
    def _fetch_children_multi(
        cls, client, record_cache, records: Iterable[BaseRecord], include: Iterable[str], force_fetch: bool = False
    ):
        """
        Fetches all children of the given records recursively

        This tries to work efficiently, fetching larger batches of children
        that can span multiple records

        Meant to be overridden by derived classes
        """
        pass

    @classmethod
    def fetch_children_multi(
        cls, records: Iterable[Optional[BaseRecord]], include: Optional[Iterable[str]] = None, force_fetch: bool = False
    ):
        """
        Fetches all children of the given records

        This tries to work efficiently, fetching larger batches of children
        that can span multiple records
        """

        # Remove any None records
        # can happen if missing_ok=True in some function calls
        records = [r for r in records if r is not None]

        if not records:
            return

        # Get the first record (for the client and other info)
        template_record = next(iter(records))

        if not all(isinstance(r, type(template_record)) for r in records):
            raise RuntimeError("Fetching children of records with different types is not supported.")

        if not all(r._client is template_record._client for r in records):
            raise RuntimeError("Fetching children of records with different clients is not supported.")

        if not all(r._record_cache is template_record._record_cache for r in records):
            raise RuntimeError("Fetching children of records with different record caches is not supported.")

        # Call the derived class function
        if include is None:
            include = []
        cls._fetch_children_multi(
            template_record._client, template_record._record_cache, records, include=include, force_fetch=force_fetch
        )

    def fetch_children(self, include: Optional[Iterable[str]] = None, force_fetch: bool = False):
        """
        Fetches all children of this record recursively
        """
        self.fetch_children_multi([self], include, force_fetch)

    def sync_to_cache(self, detach: bool = False):
        """
        Syncs this record to the cache

        If `detach` is True, then the record will be removed from the cache
        """

        if self._record_cache is None:
            return
        if self._record_cache.read_only:
            return

        self._record_cache.writeback_record(self)
        self._cache_dirty = False

        if detach:
            self._record_cache = None

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} id={self.id} status={self.status}>"

    def propagate_client(self, client):
        """
        Propagates a client and related information to this record to any fields within this record that need it

        This is expected to be called from derived class propagate_client functions as well
        """
        self._client = client
        self._base_url = f"api/v1/records/{self.record_type}/{self.id}"

        if self.compute_history_ is not None:
            for ch in self.compute_history_:
                ch.propagate_client(self._client, self._base_url)

        if self.native_files_ is not None:
            for nf in self.native_files_.values():
                nf.propagate_client(self._client, self._base_url)

    def _get_child_records(
        self,
        child_record_ids: Sequence[int],
        child_record_type: Type[_Record_T],
        include: Optional[Iterable[str]] = None,
    ) -> List[_Record_T]:
        """
        Helper function for obtaining child records either from the cache or from the server

        The records are returned in the same order as the `record_ids` parameter.

        If `include` is specified, additional fields will be fetched from the server. However, if the records are in the
        cache already, they may be missing those fields.
        """

        return get_records_with_cache(
            self._client, self._record_cache, child_record_type, child_record_ids, include, force_fetch=False
        )

    def _assert_online(self):
        """Raises an exception if this record does not have an associated client"""
        if self.offline:
            raise RuntimeError("Record is not connected to a client")

    def _fetch_compute_history(self):
        self._assert_online()

        self.compute_history_ = self._client.make_request(
            "get", f"{self._base_url}/compute_history", List[ComputeHistory]
        )

        self.propagate_client(self._client)

    def _fetch_task(self):
        if self.is_service:
            self.task_ = None
        else:
            self.task_ = self._client.make_request("get", f"{self._base_url}/task", Optional[RecordTask])

    def _fetch_service(self):
        if not self.is_service:
            self.service_ = None
        else:
            self.service_ = self._client.make_request("get", f"{self._base_url}/service", Optional[RecordService])

    def _fetch_comments(self):
        self._assert_online()

        self.comments_ = self._client.make_request("get", f"{self._base_url}/comments", List[RecordComment])

    def _fetch_native_files(self):
        self.native_files_ = self._client.make_request("get", f"{self._base_url}/native_files", Dict[str, NativeFile])
        self.propagate_client(self._client)

    def _get_output(self, output_type: OutputTypeEnum) -> Optional[Union[str, Dict[str, Any]]]:
        history = self.compute_history
        if not history:
            return None
        return history[-1].get_output(output_type)

    @property
    def offline(self) -> bool:
        return self._client is None

    @property
    def children_status(self) -> Dict[RecordStatusEnum, int]:
        """Returns a dictionary of the status of all children of this record"""
        self._assert_online()

        return self._client.make_request(
            "get",
            f"{self._base_url}/children_status",
            Dict[RecordStatusEnum, int],
        )

    @property
    def children_errors(self) -> List[BaseRecord]:
        """Returns errored child records"""
        self._assert_online()

        error_ids = self._client.make_request(
            "get",
            f"{self._base_url}/children_errors",
            List[int],
        )

        return self._client._get_records_by_type(None, error_ids)

    @property
    def compute_history(self) -> List[ComputeHistory]:
        if self.compute_history_ is None:
            self._fetch_compute_history()
        return self.compute_history_

    @property
    def task(self) -> Optional[RecordTask]:
        # task_ may be None because it either hasn't been fetched or it doesn't exist
        # fetch only if it has been set at some point
        if self.task_ is None and "task_" not in self.__fields_set__:
            self._fetch_task()
        return self.task_

    @property
    def service(self) -> Optional[RecordService]:
        # service_ may be None because it either hasn't been fetched or it doesn't exist
        # fetch only if it has been set at some point
        if self.service_ is None and "service_" not in self.__fields_set__:
            self._fetch_service()
        return self.service_

    def get_waiting_reason(self) -> Dict[str, Any]:
        return self._client.make_request("get", f"api/v1/records/{self.id}/waiting_reason", Dict[str, Any])

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
        history = self.compute_history
        if not history:
            return None
        return history[-1].provenance


ServiceDependency.update_forward_refs()

_Record_T = TypeVar("_Record_T", bound=BaseRecord)


class RecordAddBodyBase(RestModelBase):
    compute_tag: constr(to_lower=True)
    compute_priority: PriorityEnum
    owner_group: Optional[str]
    find_existing: bool = True

    @root_validator(pre=True)
    def _rm_deprecated(cls, values):
        # TODO - DEPRECATED - Remove eventually
        if "tag" in values:
            values["compute_tag"] = values.pop("tag")
        if "priority" in values:
            values["compute_priority"] = values.pop("priority")

        return values


class RecordModifyBody(RestModelBase):
    record_ids: List[int]
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


class RecordDeleteBody(RestModelBase):
    record_ids: List[int]
    soft_delete: bool
    delete_children: bool


class RecordRevertBody(RestModelBase):
    revert_status: RecordStatusEnum
    record_ids: List[int]


class RecordQueryFilters(QueryModelBase):
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


class RecordQueryIterator(QueryIteratorBase[_Record_T]):
    """
    Iterator for all types of record queries

    This iterator transparently handles batching and pagination over the results
    of a record query, and works with all kinds of records.
    """

    def __init__(
        self,
        client,
        query_filters: RecordQueryFilters,
        record_type: Type[_Record_T],
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

    def _request(self) -> List[_Record_T]:
        if self.record_type is None:
            record_ids = self._client.make_request(
                "post",
                f"api/v1/records/query",
                List[int],
                body=self._query_filters,
            )
        else:
            # Get the record type string. This is kind of ugly, but works.
            record_type_str = self.record_type.__fields__["record_type"].default
            record_ids = self._client.make_request(
                "post",
                f"api/v1/records/{record_type_str}/query",
                List[int],
                body=self._query_filters,
            )

        return self._client._get_records_by_type(self.record_type, record_ids, include=self.include)


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
    client: Any = None,
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
