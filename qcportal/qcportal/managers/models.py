from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List

from dateutil.parser import parse as date_parser
from pydantic import BaseModel, Field, constr, validator, Extra, PrivateAttr

from qcportal.base_models import RestModelBase, QueryProjModelBase
from ..base_models import QueryIteratorBase


class ManagerStatusEnum(str, Enum):
    """
    The state of a Queue Manager. The states which are available are a finite set.
    """

    active = "active"
    inactive = "inactive"

    @classmethod
    def _missing_(cls, name):
        """Attempts to find the correct status in a case-insensitive way

        If a string being converted to a ManagerStatusEnum is missing, then this function
        will convert the case and try to find the appropriate status.
        """
        name = name.lower()

        # Search this way rather than doing 'in' since we are comparing
        # a string to an enum
        for status in cls:
            if name == status:
                return status


class ManagerName(BaseModel):
    class Config:
        extra = Extra.forbid

    cluster: str
    hostname: str
    uuid: str

    @property
    def fullname(self):
        return self.cluster + "-" + self.hostname + "-" + self.uuid

    def __str__(self):
        return self.fullname


class ComputeManagerLogEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    manager_id: int

    claimed: int
    successes: int
    failures: int
    rejected: int

    total_cpu_hours: float
    active_tasks: int
    active_cores: int
    active_memory: float

    timestamp: datetime


class ComputeManager(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int = Field(...)
    name: str = Field(...)
    cluster: str = Field(...)
    hostname: str
    username: Optional[str]
    tags: List[str]

    claimed: int
    successes: int
    failures: int
    rejected: int

    total_cpu_hours: float
    active_tasks: int
    active_cores: int
    active_memory: float

    status: ManagerStatusEnum
    created_on: datetime
    modified_on: datetime

    manager_version: str
    programs: Dict[str, List[str]]

    log_: Optional[List[ComputeManagerLogEntry]] = None

    _client: Any = PrivateAttr(None)
    _base_url: Optional[str] = PrivateAttr(None)

    def propagate_client(self, client):
        self._client = client
        self._base_url = f"api/v1/managers/{self.name}"

    def _fetch_log(self):
        if self._client is None:
            raise RuntimeError("This manager object is not connected to a client")

        self.log_ = self._client.make_request(
            "get",
            f"{self._base_url}/log",
            List[ComputeManagerLogEntry],
        )

    @property
    def log(self):
        if self.log_ is None:
            self._fetch_log()
        return self.log_


class ManagerActivationBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    manager_version: str = Field(..., description="Version of the manager itself")
    username: Optional[str] = Field(..., description="Username this manager is connected with")
    programs: Dict[constr(to_lower=True), List[str]] = Field(..., description="Programs available on this manager")
    tags: List[constr(to_lower=True)] = Field(..., description="Tags this manager will compute for")

    @validator("tags")
    def validate_tags(cls, v):
        v = [x for x in v if len(x) > 0]

        if len(v) == 0:
            raise ValueError("'tags' field contains no non-zero-length tags")

        return list(dict.fromkeys(v))  # remove duplicates, maintaining order (in python 3.6+)

    @validator("programs")
    def validate_programs(cls, v):
        # Remove programs of zero length
        v = {x: y for x, y in v.items() if len(x) > 0}
        if len(v) == 0:
            raise ValueError("'programs' field contains no non-zero-length programs")
        return v


class ManagerUpdateBody(RestModelBase):
    status: ManagerStatusEnum
    active_tasks: int
    active_cores: int
    active_memory: float
    total_cpu_hours: float


class ManagerQueryFilters(QueryProjModelBase):
    manager_id: Optional[List[int]] = None
    name: Optional[List[str]] = None
    cluster: Optional[List[str]] = None
    hostname: Optional[List[str]] = None
    status: Optional[List[ManagerStatusEnum]] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None

    @validator("modified_before", "modified_after", pre=True)
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class ManagerQueryIterator(QueryIteratorBase[ComputeManager]):
    """
    Iterator for manager queries

    This iterator transparently handles batching and pagination over the results
    of a manager query
    """

    def __init__(self, client, query_filters: ManagerQueryFilters):
        """
        Construct an iterator

        Parameters
        ----------
        client
            QCPortal client object used to contact/retrieve data from the server
        query_filters
            The actual query information to send to the server
        """

        batch_limit = client.api_limits["get_managers"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, batch_limit)

    def _request(self) -> List[ComputeManager]:
        managers = self._client.make_request(
            "post",
            "api/v1/managers/query",
            List[ComputeManager],
            body=self._query_filters,
        )

        for m in managers:
            m.propagate_client(self._client)

        return managers
