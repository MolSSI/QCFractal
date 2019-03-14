import datetime
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from .common_models import ObjectId


class TaskStatusEnum(str, Enum):
    running = "RUNNING"
    waiting = "WAITING"
    error = "ERROR"
    complete = "COMPLETE"


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
    priority: int = 0
    tag: Optional[str] = None

    # Link back to the base Result
    base_result: Tuple[BaseResultEnum, ObjectId]

    # Modified data
    modified_on: datetime.datetime = datetime.datetime.utcnow()
    created_on: datetime.datetime = datetime.datetime.utcnow()

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))