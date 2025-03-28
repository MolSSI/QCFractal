from .singlepoint.record_models import SinglepointInput
from .optimization.record_models import OptimizationInput
from .torsiondrive.record_models import TorsiondriveInput
from .gridoptimization.record_models import GridoptimizationInput
from .reaction.record_models import ReactionInput
from .manybody.record_models import ManybodyInput
from .neb.record_models import NEBInput

from typing import Union

AllInputTypes = Union[
    SinglepointInput,
    OptimizationInput,
    TorsiondriveInput,
    GridoptimizationInput,
    ReactionInput,
    ManybodyInput,
    NEBInput,
]
