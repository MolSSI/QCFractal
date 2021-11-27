from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, Extra
import abc
from datetime import datetime
from qcfractal.portal.outputstore import OutputStore
from qcelemental.models.results import Provenance
from typing import Optional, Dict, Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.portal import PortalClient


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
    outputs: Optional[List[OutputStore]]


class TaskRecord(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    record_id: int

    spec: Dict[str, Any]
    tag: Optional[str] = None
    required_programs: List[str]
    priority: PriorityEnum
    created_on: datetime


class BaseRecord(abc.ABC):
    class _DataModel(BaseModel):
        class Config:
            extra = Extra.forbid
            allow_mutation = True
            validate_assignment = True

        id: int

        record_type: str = Field(..., description="The type of record this is (singlepoint, optimization, etc)")

        protocols: Optional[Dict[str, Any]] = None  # TODO- remove

        extras: Optional[Dict[str, Any]] = None

        status: RecordStatusEnum
        manager_name: Optional[str]

        created_on: datetime
        modified_on: datetime

        compute_history: List[ComputeHistory]

        task: Optional[TaskRecord] = None

    _data: _DataModel

    def __init__(self, client: PortalClient, data: _DataModel):
        self._client = client
        self._data = data

    @property
    def compute_history(self):
        return self._data.compute_history

    def _retrieve_outputs(self):
        # Retrieve the entire compute history, including outputs
        self._data.compute_history = self._client.get_compute_history(self._data.id, include_outputs=True)
