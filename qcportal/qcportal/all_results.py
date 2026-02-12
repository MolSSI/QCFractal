from typing import Annotated, Union

from pydantic import Field

from .generic_result import GenericTaskResult
from .gridoptimization.record_models import GridoptimizationRecord
from .manybody.record_models import ManybodyRecord
from .neb.record_models import NEBRecord
from .optimization.record_models import OptimizationRecord
from .qcschema_v1 import AtomicResult, OptimizationResult, FailedOperation
from .reaction.record_models import ReactionRecord
from .singlepoint.record_models import SinglepointRecord
from .torsiondrive.record_models import TorsiondriveRecord

# All possible records we can get from the server
# These are all the possible result objects that might be returned by a manager

# TODO - discriminator?
AllSchemaV1ResultTypes = FailedOperation | AtomicResult | OptimizationResult | GenericTaskResult

AllQCPortalRecordTypes = Annotated[
    SinglepointRecord
    | OptimizationRecord
    | TorsiondriveRecord
    | GridoptimizationRecord
    | ReactionRecord
    | ManybodyRecord
    | NEBRecord,
    Field(discriminator="record_type"),
]

# TODO - discriminator?
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
