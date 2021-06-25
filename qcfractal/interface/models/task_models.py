import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, validator, constr

from .common_models import ObjectId, ProtoModel, AtomicResultProtocols, OptimizationProtocols


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


class PriorityEnum(int, Enum):
    """
    The priority of a Task. Higher priority will be pulled first.
    """

    high = 2
    normal = 1
    low = 0

    @classmethod
    def _missing_(cls, name):
        """Attempts to find the correct priority in a case-insensitive way

        If a string being converted to a PriorityEnum is missing, then this function
        will convert the case and try to find the appropriate priority.
        """

        if isinstance(name, int):
            # An integer that is outside the range of valid priorities
            return

        name = name.lower()

        # Search this way rather than doing 'in' since we are comparing
        # a string to an enum
        for status in cls:
            if name == status.name:
                return status


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

    # Compute blockers and prevention
    required_programs: Dict[str, Optional[str]] = Field(
        ..., description="Name of the quantum chemistry program which must be present to execute this task."
    )
    manager: Optional[str] = Field(None, description="The Queue Manager that evaluated this task.")

    # Sortables
    priority: PriorityEnum = Field(PriorityEnum.normal, description=str(PriorityEnum.__doc__))
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
    created_on: datetime.datetime = Field(None, description="The time when this task was created in the Database.")

    def __init__(self, **data):

        # Set datetime defaults if not present
        dt = datetime.datetime.utcnow()
        data.setdefault("created_on", dt)

        super().__init__(**data)


class SingleProcedureSpecification(ProtoModel):
    procedure: constr(to_lower=True, regex="single") = Field("single")
    driver: constr(to_lower=True)
    program: constr(to_lower=True)
    method: constr(to_lower=True)
    basis: Optional[constr(to_lower=True)] = Field(None)
    keywords: Optional[Union[ObjectId, Dict[str, Any]]] = Field(None)
    protocols: AtomicResultProtocols = Field(AtomicResultProtocols())
    tag: Optional[str] = Field(None)
    priority: PriorityEnum = Field(PriorityEnum.normal)


class OptimizationProcedureSpecification(ProtoModel):
    procedure: constr(to_lower=True, regex="optimization") = Field("optimization")
    program: constr(to_lower=True)
    keywords: Dict[str, Any] = Field(default_factory=dict)
    qc_spec: Dict[str, Any]
    protocols: OptimizationProtocols = Field(OptimizationProtocols())
    tag: Optional[str] = Field(None)
    priority: PriorityEnum = Field(PriorityEnum.normal)


# TODO - find a better place for this
from .torsiondrive import TorsionDriveInput
from .gridoptimization import GridOptimizationInput

AllProcedureSpecifications = Union[SingleProcedureSpecification, OptimizationProcedureSpecification]
AllServiceSpecifications = Union[TorsionDriveInput, GridOptimizationInput]
