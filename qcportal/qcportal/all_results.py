from typing import Union

# All possible records we can get from the server
# These are all the possible result objects that might be returned by a manager
from qcelemental.models import AtomicResult, OptimizationResult, FailedOperation

from .generic_result import GenericTaskResult
from .gridoptimization.record_models import GridoptimizationRecord
from .manybody.record_models import ManybodyRecord
from .neb.record_models import NEBRecord
from .optimization.record_models import OptimizationRecord
from .reaction.record_models import ReactionRecord
from .singlepoint.record_models import SinglepointRecord
from .torsiondrive.record_models import TorsiondriveRecord

AllSchemaV1ResultTypes = Union[FailedOperation, AtomicResult, OptimizationResult, GenericTaskResult]
AllQCPortalRecordTypes = Union[
    SinglepointRecord,
    OptimizationRecord,
    TorsiondriveRecord,
    GridoptimizationRecord,
    ReactionRecord,
    ManybodyRecord,
    NEBRecord,
]

AllResultTypes = Union[
    FailedOperation,
    AtomicResult,
    OptimizationResult,
    GenericTaskResult,
    SinglepointRecord,
    OptimizationRecord,
    TorsiondriveRecord,
    GridoptimizationRecord,
    ReactionRecord,
    ManybodyRecord,
    NEBRecord,
]
