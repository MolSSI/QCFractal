from typing import Union

from .models import PriorityEnum, RecordStatusEnum, ComputeHistory, BaseRecord
from .rest_models import RecordModifyBody, RecordQueryBody, ComputeHistoryURLParameters

# These are all the possible result objects that might be returned by a manager
from qcelemental.models import AtomicResult, OptimizationResult, FailedOperation

AllResultTypes = Union[FailedOperation, AtomicResult, OptimizationResult]
