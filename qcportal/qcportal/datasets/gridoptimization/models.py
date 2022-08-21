from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.records.gridoptimization import (
    GridoptimizationRecord,
    GridoptimizationSpecification,
)
from qcportal.utils import make_list
from ..models import BaseDataset


class GridoptimizationDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    additional_optimization_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class GridoptimizationDatasetEntry(GridoptimizationDatasetNewEntry):
    initial_molecule: Molecule


# Gridoptimization dataset specifications are just optimization specifications
# The gridoptimization keywords are stored in the entries ^^
class GridoptimizationDatasetSpecification(BaseModel):
    name: str
    specification: GridoptimizationSpecification
    description: Optional[str] = None


class GridoptimizationDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[GridoptimizationRecord._DataModel]


class GridoptimizationDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        dataset_type: Literal["gridoptimization"] = "gridoptimization"

        specifications: Dict[str, GridoptimizationDatasetSpecification] = {}
        entries: Dict[str, GridoptimizationDatasetEntry] = {}
        record_map: Dict[Tuple[str, str], GridoptimizationRecord] = {}

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["gridoptimization"] = "gridoptimization"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = GridoptimizationDatasetEntry
    _specification_type = GridoptimizationDatasetSpecification
    _record_item_type = GridoptimizationDatasetRecordItem
    _record_type = GridoptimizationRecord

    def add_specification(
        self, name: str, specification: GridoptimizationSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        payload = GridoptimizationDatasetSpecification(name=name, specification=specification, description=description)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/specifications",
            List[GridoptimizationDatasetSpecification],
            None,
            InsertMetadata,
            [payload],
            None,
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[GridoptimizationDatasetNewEntry, Iterable[GridoptimizationDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)
        ret = self.client._auto_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/entries/bulkCreate",
            List[GridoptimizationDatasetNewEntry],
            None,
            InsertMetadata,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret
