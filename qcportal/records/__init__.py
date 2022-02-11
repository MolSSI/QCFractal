from typing import Union

# These are all the possible result objects that might be returned by a manager
from qcelemental.models import AtomicResult, OptimizationResult, FailedOperation

from .models import (
    PriorityEnum,
    RecordStatusEnum,
    ComputeHistory,
    BaseRecord,
    RecordModifyBody,
    RecordDeleteBody,
    RecordRevertBody,
    RecordQueryBody,
    RecordAddBodyBase,
)

AllResultTypes = Union[FailedOperation, AtomicResult, OptimizationResult]

# All possible records we can get from the server
from .singlepoint.models import SinglepointRecord
from .optimization.models import OptimizationRecord
from .torsiondrive.models import TorsiondriveRecord
from .gridoptimization.models import GridoptimizationRecord

AllRecordTypes = Union[SinglepointRecord, OptimizationRecord, TorsiondriveRecord, GridoptimizationRecord]
AllRecordDataModelTypes = Union[
    SinglepointRecord._DataModel,
    OptimizationRecord._DataModel,
    TorsiondriveRecord._DataModel,
    GridoptimizationRecord._DataModel,
]
