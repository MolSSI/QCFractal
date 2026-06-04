from typing import Annotated

from pydantic import Field

from .gridoptimization.record_models import GridoptimizationInput
from .manybody.record_models import ManybodyInput
from .neb.record_models import NEBInput
from .optimization.record_models import OptimizationInput
from .reaction.record_models import ReactionInput
from .singlepoint.record_models import SinglepointInput
from .torsiondrive.record_models import TorsiondriveInput

AllInputTypes = Annotated[
    SinglepointInput
    | OptimizationInput
    | TorsiondriveInput
    | GridoptimizationInput
    | ReactionInput
    | ManybodyInput
    | NEBInput,
    Field(discriminator="record_type"),
]

AllQCPortalInputTypes = Annotated[
    SinglepointInput
    | OptimizationInput
    | TorsiondriveInput
    | GridoptimizationInput
    | ReactionInput
    | ManybodyInput
    | NEBInput,
    Field(discriminator="record_type"),
]
