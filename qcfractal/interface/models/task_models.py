import datetime
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, validator
from qcelemental.models import ComputeError

from .common_models import ObjectId


class DBRef(BaseModel):
    ref: str
    id: ObjectId


class TaskStatusEnum(str, Enum):
    running = "RUNNING"
    waiting = "WAITING"
    error = "ERROR"
    complete = "COMPLETE"


class ManagerStatusEnum(str, Enum):
    active = 'ACTIVE'
    inactive = 'INACTIVE'

class PriorityEnum(int, Enum):
    HIGH = 2
    NORMAL = 1
    LOW = 0


class BaseResultEnum(str, Enum):
    result = "result"
    procedure = "procedure"


class PythonComputeSpec(BaseModel):
    function: str
    args: List[Any]
    kwargs: Dict[str, Any]


class TaskRecord(BaseModel):

    id: ObjectId = None

    spec: PythonComputeSpec
    parser: str
    status: TaskStatusEnum = "WAITING"

    # Compute blockers and prevention
    program: str
    procedure: Optional[str] = None
    manager: Optional[str] = None

    # Sortables
    priority: PriorityEnum = PriorityEnum.NORMAL
    tag: Optional[str] = None

    # Link back to the base Result
    base_result: Union[DBRef, int]
    error: Optional[ComputeError] = None

    # Modified data
    modified_on: datetime.datetime = None
    created_on: datetime.datetime = None


    def __init__(self, **data):

        # Set datetime defaults if not present
        dt = datetime.datetime.utcnow()
        data.setdefault("modified_on", dt)
        data.setdefault("created_on", dt)

        super().__init__(**data)

    class Config:
        extra = "forbid"

    @validator('priority', pre=True)
    def munge_priority(cls, v):
        if isinstance(v, str):
            v = PriorityEnum[v.upper()]
        elif v is None:
            v = PriorityEnum.NORMAL
        return v

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('procedure')
    def check_procedure(cls, v):
        return v.lower()

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))