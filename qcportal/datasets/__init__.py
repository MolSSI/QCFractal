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
    DatasetModifyMetadataBody,
    DatasetQueryRecords,
    DatasetDeleteParams,
)

# All possible datasets we can get from the server
from .singlepoint.models import SinglepointDataset
from .optimization.models import OptimizationDataset
from .torsiondrive.models import TorsiondriveDataset
from .gridoptimization import GridoptimizationDataset

AllDatasetTypes = Union[SinglepointDataset, OptimizationDataset, TorsiondriveDataset, GridoptimizationDataset]
AllDatasetDataModelTypes = Union[
    SinglepointDataset._DataModel,
    OptimizationDataset._DataModel,
    TorsiondriveDataset._DataModel,
    GridoptimizationDataset._DataModel,
]
