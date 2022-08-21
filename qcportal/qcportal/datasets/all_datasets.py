from typing import Any, Union

import pydantic

# All possible datasets we can get from the server
from .singlepoint.models import SinglepointDataset
from .optimization.models import OptimizationDataset
from .torsiondrive.models import TorsiondriveDataset
from .gridoptimization import GridoptimizationDataset
from .manybody import ManybodyDataset
from .reaction import ReactionDataset
from .neb import NEBDataset

AllDatasetTypes = Union[
    SinglepointDataset,
    OptimizationDataset,
    TorsiondriveDataset,
    GridoptimizationDataset,
    ManybodyDataset,
    ReactionDataset,
    NEBDataset,
]
AllDatasetDataModelTypes = Union[
    SinglepointDataset._DataModel,
    OptimizationDataset._DataModel,
    TorsiondriveDataset._DataModel,
    GridoptimizationDataset._DataModel,
    ManybodyDataset._DataModel,
    ReactionDataset._DataModel,
    NEBDataset._DataModel,
]


def dataset_from_datamodel(data: AllDatasetDataModelTypes, client: Any) -> AllDatasetTypes:
    dataset_init = {"client": client, "dataset_type": data.dataset_type, "raw_data": data}
    return pydantic.parse_obj_as(AllDatasetTypes, dataset_init)
