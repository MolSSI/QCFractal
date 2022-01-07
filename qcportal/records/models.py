from __future__ import annotations

import abc
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, TYPE_CHECKING, Union

from pydantic import BaseModel, Extra, validator
from qcelemental.models.results import Provenance

from ..base_models import (
    RestModelBase,
    QueryProjModelBase,
    CommonGetProjURLParameters,
    validate_list_to_single,
)
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
    outputs: Optional[List[OutputStore]]

    def get_output(self, output_type: OutputTypeEnum) -> Optional[Union[str, Dict[str, Any]]]:
        if not self.outputs:
            return None

        for o in self.outputs:
            if o.output_type == output_type:
                if o.output_type == OutputTypeEnum.error:
                    return o.as_json
                else:
                    return o.as_string

            return None


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
    tag: Optional[str] = None
    required_programs: List[str]
    priority: PriorityEnum
    created_on: datetime


class ServiceRecord(BaseModel):
    id: int
    record_id: int

    tag: Optional[str] = None
    priority: PriorityEnum
    created_on: datetime

    service_state: Dict[str, Any]


class BaseRecord(abc.ABC, BaseModel):
    class _DataModel(BaseModel):
        class Config:
            extra = Extra.forbid
            allow_mutation = True
            validate_assignment = True

        id: int
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

    class Config:
        extra = Extra.forbid

    client: Any
    raw_data: _DataModel  # Meant to be overridden by derived classes

    def _retrieve_compute_history(self, include_outputs: bool = False):
        url_params = {}

        if include_outputs:
            url_params = {"include": ["*", "outputs"]}

        self.raw_data.compute_history = self.client._auto_request(
            "get",
            f"v1/record/{self.raw_data.id}/compute_history",
            None,
            CommonGetProjURLParameters,
            List[ComputeHistory],
            None,
            url_params,
        )

    def _retrieve_task(self):
        self.raw_data.task = self.client._auto_request(
            "get",
            f"v1/record/{self.raw_data.id}/task",
            None,
            None,
            Optional[TaskRecord],
            None,
            None,
        )

    def _get_output(self, output_type: OutputTypeEnum) -> Optional[Union[str, Dict[str, Any]]]:
        if not self.raw_data.compute_history:
            return None

        last_computation = self.raw_data.compute_history[-1]
        if last_computation.outputs is None:
            self._retrieve_compute_history(include_outputs=True)
            last_computation = self.raw_data.compute_history[-1]

        return last_computation.get_output(output_type)

    @property
    def compute_history(self):
        return self.raw_data.compute_history

    @property
    def status(self):
        return self.raw_data.status

    @property
    def task(self):
        if self.raw_data.task is None:
            self._retrieve_task()
        return self.raw_data.task

    @property
    def stdout(self) -> Optional[str]:
        return self._get_output(OutputTypeEnum.stdout)

    @property
    def stderr(self) -> Optional[str]:
        return self._get_output(OutputTypeEnum.stderr)

    @property
    def error(self) -> Optional[Dict[str, Any]]:
        return self._get_output(OutputTypeEnum.error)


class RecordAddBodyBase(RestModelBase):
    tag: Optional[str]
    priority: PriorityEnum = PriorityEnum.normal


class RecordModifyBody(RestModelBase):
    record_id: List[int]
    status: Optional[RecordStatusEnum] = None
    priority: Optional[PriorityEnum] = None
    tag: Optional[str] = None
    delete_tag: bool = False
    comment: Optional[str] = None


class RecordDeleteURLParameters(RestModelBase):
    record_id: List[int]
    soft_delete: bool
    delete_children: bool

    @validator("soft_delete", "delete_children", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class RecordUndeleteURLParameters(RestModelBase):
    record_id: List[int]


class RecordQueryBody(QueryProjModelBase):
    record_id: Optional[List[int]] = None
    record_type: Optional[List[str]] = None
    manager_name: Optional[List[str]] = None
    status: Optional[List[RecordStatusEnum]] = None
    created_before: Optional[datetime] = None
    created_after: Optional[datetime] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None
