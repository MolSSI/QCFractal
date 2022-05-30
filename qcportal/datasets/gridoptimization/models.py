from typing import Dict, Any, Union, Optional, List, Iterable, Tuple, Set

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.records.gridoptimization import (
    GridoptimizationRecord,
    GridoptimizationKeywords,
)
from qcportal.records.optimization import OptimizationSpecification
from qcportal.utils import make_list
from ..models import BaseDataset


class GridoptimizationDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecule: Union[Molecule, int]
    gridoptimization_keywords: GridoptimizationKeywords
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class GridoptimizationDatasetEntry(GridoptimizationDatasetNewEntry):
    initial_molecule_id: int
    initial_molecule: Optional[Molecule] = None


# Gridoptimization dataset specifications are just optimization specifications
# The gridoptimization keywords are stored in the entries ^^
class GridoptimizationDatasetSpecification(BaseModel):
    name: str
    specification: OptimizationSpecification
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

    @staticmethod
    def transform_entry_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:
        """
        Transforms user-friendly includes into includes used by the web API
        """

        if includes is None:
            return None

        ret = BaseDataset.transform_entry_includes(includes)

        if "initial_molecule" in includes:
            ret.add("initial_molecule")

        return ret

    def add_specification(self, name: str, specification: OptimizationSpecification, description: Optional[str] = None):

        payload = GridoptimizationDatasetSpecification(name=name, specification=specification, description=description)

        self.client._auto_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/specifications",
            List[GridoptimizationDatasetSpecification],
            None,
            None,
            [payload],
            None,
        )

        self._post_add_specification(name)

    def add_entries(self, entries: Union[GridoptimizationDatasetNewEntry, Iterable[GridoptimizationDatasetNewEntry]]):

        entries = make_list(entries)
        self.client._auto_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/entries/bulkCreate",
            List[GridoptimizationDatasetNewEntry],
            None,
            None,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
