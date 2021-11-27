from __future__ import annotations

from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


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

    qcengine_version: str
    manager_version: str
    programs: Dict[str, Any]

    log: Optional[List[ComputeManagerLogEntry]]
