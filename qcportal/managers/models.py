from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Iterable, Set, Tuple

from pydantic import BaseModel, Field, constr, validator

from qcportal.base_models import RestModelBase, QueryProjModelBase
from ..base_models import QueryIteratorBase
from ..metadata_models import QueryMetadata


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
    cluster: str
    hostname: str
    uuid: str

    @property
    def fullname(self):
        return self.cluster + "-" + self.hostname + "-" + self.uuid

    def __str__(self):
        return self.fullname


class ComputeManagerLogEntry(BaseModel):
    id: int
    manager_id: int

    claimed: int
    successes: int
    failures: int
    rejected: int

    total_worker_walltime: float
    total_task_walltime: float
    active_tasks: int
    active_cores: int
    active_memory: float

    timestamp: datetime


class ComputeManager(BaseModel):
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

    total_worker_walltime: float
    total_task_walltime: float
    active_tasks: int
    active_cores: int
    active_memory: float

    status: ManagerStatusEnum
    created_on: datetime
    modified_on: datetime

    manager_version: str
    programs: Dict[str, Any]

    log: Optional[List[ComputeManagerLogEntry]] = None

    @staticmethod
    def transform_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:
        if includes is None:
            return None

        ret: Set[str] = {"*"}

        if "log" in includes:
            ret.add("log")

        return ret


class ManagerActivationBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    manager_version: str = Field(..., description="Version of the manager itself")
    username: Optional[str] = Field(..., description="Username this manager is connected with")
    programs: Dict[constr(to_lower=True), Optional[str]] = Field(..., description="Programs available on this manager")
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
    total_worker_walltime: float
    total_task_walltime: float
    active_tasks: int
    active_cores: int
    active_memory: float


class ManagerQueryFilters(QueryProjModelBase):
    manager_id: Optional[List[int]] = None
    name: Optional[List[str]] = None
    cluster: Optional[List[str]] = None
    hostname: Optional[List[str]] = None
    status: Optional[List[ManagerStatusEnum]] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None


class ManagerQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: ManagerQueryFilters):
        api_limit = client.api_limits["get_managers"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[ManagerQueryFilters]]:
        return self.client._auto_request(
            "post",
            "v1/managers/query",
            ManagerQueryFilters,
            None,
            Tuple[Optional[QueryMetadata], List[ComputeManager]],
            self.query_filters,
            None,
        )
