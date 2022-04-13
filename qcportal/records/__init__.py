from typing import Union, Sequence, Optional, Any, List

import pydantic

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
from .reaction.models import ReactionRecord

AllRecordTypes = Union[
    SinglepointRecord, OptimizationRecord, TorsiondriveRecord, GridoptimizationRecord, ReactionRecord
]
AllRecordDataModelTypes = Union[
    SinglepointRecord._DataModel,
    OptimizationRecord._DataModel,
    TorsiondriveRecord._DataModel,
    GridoptimizationRecord._DataModel,
    ReactionRecord._DataModel,
]


def record_from_datamodel(data: AllRecordDataModelTypes, client: Any) -> AllRecordTypes:
    record_init = {"client": client, "record_type": data.record_type, "raw_data": data}

    return pydantic.parse_obj_as(AllRecordTypes, record_init)


def records_from_datamodels(
    data: Sequence[Optional[AllRecordDataModelTypes]],
    client: Any,
) -> List[Optional[AllRecordTypes]]:
    record_init = [
        {"client": client, "record_type": d.record_type, "raw_data": d} if d is not None else None for d in data
    ]

    return pydantic.parse_obj_as(List[Optional[AllRecordTypes]], record_init)
