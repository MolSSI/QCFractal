import datetime
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, validator

from .common_models import ObjectId


class DBRef(BaseModel):
    ref: str
    id: ObjectId


class TaskStatusEnum(str, Enum):
    running = "RUNNING"
    waiting = "WAITING"
    error = "ERROR"
    complete = "COMPLETE"


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
    priority: PriorityEnum = 1
    tag: Optional[str] = None

    # Link back to the base Result
    base_result: DBRef

    # Modified data
    modified_on: datetime.datetime = datetime.datetime.utcnow()
    created_on: datetime.datetime = datetime.datetime.utcnow()

    @validator('priority')
    def munge_priority(cls, v):
        if isinstance(v, str):
            v = TaskStatusEnum[v.upper()]
        return v

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('procedure')
    def check_procedure(cls, v):
        return v.lower()

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))