from __future__ import annotations

from pydantic import BaseModel, Field, constr, validator, Extra
import abc
from datetime import datetime
from qcfractal.interface.models import PriorityEnum, RecordStatusEnum
from qcfractal.portal.components.outputstore import OutputStore
from qcelemental.models.results import Provenance
from typing import Optional, Dict, Any, List, TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.portal import PortalClient


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
