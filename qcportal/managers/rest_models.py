from datetime import datetime
from typing import Optional, List, Dict

from pydantic import Field, validator, constr

from .models import ManagerName, ManagerStatusEnum
from ..base_models import RestModelBase, QueryProjModelBase


class ManagerActivationBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    manager_version: str = Field(..., description="Version of the manager itself")
    qcengine_version: str = Field(..., description="Version of QCEngine running on the manager")
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


class ManagerQueryBody(QueryProjModelBase):
    id: Optional[List[int]] = None
    name: Optional[List[str]] = None
    cluster: Optional[List[str]] = None
    hostname: Optional[List[str]] = None
    status: Optional[List[ManagerStatusEnum]] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None
