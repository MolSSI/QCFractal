from typing import Union

from .models import (
    PriorityEnum,
    RecordStatusEnum,
    ComputeHistory,
    BaseRecord,
    RecordModifyBody,
    RecordDeleteURLParameters,
    RecordQueryBody,
    ComputeHistoryURLParameters,
    RecordAddBodyBase,
)

# These are all the possible result objects that might be returned by a manager
from qcelemental.models import AtomicResult, OptimizationResult, FailedOperation

AllResultTypes = Union[FailedOperation, AtomicResult, OptimizationResult]

# All possible records we can get from the server
from .singlepoint.models import SinglePointRecord

AllRecordTypes = Union[SinglePointRecord]
AllDataModelTypes = Union[SinglePointRecord._DataModel]
