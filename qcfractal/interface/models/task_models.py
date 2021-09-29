import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import Field, validator
from qcelemental.models import ComputeError

from .common_models import ObjectId, ProtoModel


class TaskStatusEnum(str, Enum):
    """
    The state of a Task object. The states which are available are a finite set.
    """

    running = "RUNNING"
    waiting = "WAITING"
    error = "ERROR"
    complete = "COMPLETE"


class ManagerStatusEnum(str, Enum):
    """
    The state of a Queue Manager. The states which are available are a finite set.
    """

    active = "ACTIVE"
    inactive = "INACTIVE"


class PriorityEnum(int, Enum):
    """
    The priority of a Task. Higher priority will be pulled first. The priorities which are available are a finite set.
    """

    HIGH = 2
    NORMAL = 1
    LOW = 0


class BaseResultEnum(str, Enum):
    result = "result"
    procedure = "procedure"


class PythonComputeSpec(ProtoModel):
    function: str = Field(
        ..., description="The module and function name of a Python-callable to call. Of the form 'module.function'."
    )
    args: List[Any] = Field(
        ..., description="A List of positional arguments to pass into ``function`` in order they appear."
    )
    kwargs: Dict[str, Any] = Field(..., description="Dictionary of keyword arguments to pass into ``function``.")


class TaskRecord(ProtoModel):

    id: ObjectId = Field(None, description="The Database assigned Id of the Task, if it has been assigned yet.")

    spec: PythonComputeSpec = Field(..., description="The Python function specification for this Task.")
    parser: str = Field(..., description="The type of operation this is Task is. Can be 'single' or 'optimization'.")
    status: TaskStatusEnum = Field(TaskStatusEnum.waiting, description="What stage of processing this task is at.")

    # Compute blockers and prevention
    program: str = Field(
        ..., description="Name of the quantum chemistry program which must be present to execute this task."
    )
    procedure: Optional[str] = Field(
        None, description="Name of the procedure the compute platform must be able to perform to execute this task."
    )
    manager: Optional[str] = Field(None, description="The Queue Manager that evaluated this task.")

    # Sortables
    priority: PriorityEnum = Field(PriorityEnum.NORMAL, description=str(PriorityEnum.__doc__))
    tag: Optional[str] = Field(
        None,
        description="The optional tag assigned to this Task. Tagged tasks can only be pulled by Queue Managers which "
        "explicitly reference this tag. If no Tag is specified, any Queue Manager can pull this Task.",
    )
    # Link back to the base Result
    base_result: ObjectId = Field(
        ..., description="Reference to the output Result from this Task as it exists within the database."
    )

    # Modified data
    modified_on: datetime.datetime = Field(None, description="The last time this task was updated in the Database.")
    created_on: datetime.datetime = Field(None, description="The time when this task was created in the Database.")

    def __init__(self, **data):

        # Set datetime defaults if not present
        dt = datetime.datetime.utcnow()
        data.setdefault("modified_on", dt)
        data.setdefault("created_on", dt)

        super().__init__(**data)

    @validator("priority", pre=True)
    def munge_priority(cls, v):
        if isinstance(v, str):
            v = PriorityEnum[v.upper()]
        elif v is None:
            v = PriorityEnum.NORMAL
        return v

    @validator("program")
    def check_program(cls, v):
        return v.lower()

    @validator("procedure")
    def check_procedure(cls, v):
        if v:
            v = v.lower()
        return v
