from typing import Union

from .models import (
    BaseDataset,
    DatasetQueryModel,
    DatasetGetEntryBody,
    DatasetGetRecordItemsBody,
    DatasetSubmitBody,
    DatasetDeleteStrBody,
    DatasetDeleteRecordItemsBody,
    DatasetRecordModifyBody,
    DatasetRecordRevertBody,
)

# All possible datasets we can get from the server
from .singlepoint.models import SinglepointDataset
from .optimization.models import OptimizationDataset

AllDatasetTypes = Union[SinglepointDataset, OptimizationDataset]
AllDatasetDataModelTypes = Union[SinglepointDataset._DataModel, OptimizationDataset._DataModel]
