from typing import Dict, Any, Union, Optional, List, Iterable, Tuple, Set

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.records.optimization import OptimizationRecord, OptimizationSpecification
from qcportal.utils import make_list
from .. import BaseDataset


class OptimizationDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class OptimizationDatasetEntry(OptimizationDatasetNewEntry):
    initial_molecule_id: int
    molecule: Optional[Molecule] = None


class OptimizationDatasetSpecification(BaseModel):
    name: str
    specification: OptimizationSpecification
    description: Optional[str] = None


class OptimizationDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[OptimizationRecord._DataModel]


class OptimizationDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        dataset_type: Literal["optimization"] = "optimization"

        specifications: Dict[str, OptimizationDatasetSpecification] = {}
        entries: Dict[str, OptimizationDatasetEntry] = {}
        record_map: Dict[Tuple[str, str], OptimizationRecord] = {}

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["optimization"] = "optimization"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = OptimizationDatasetEntry
    _specification_type = OptimizationDatasetSpecification
    _record_item_type = OptimizationDatasetRecordItem
    _record_type = OptimizationRecord

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
        initial_molecules: Optional[List[Molecule]]

        payload = OptimizationDatasetSpecification(name=name, specification=specification, description=description)

        self.client._auto_request(
            "post",
            f"v1/datasets/optimization/{self.id}/specifications",
            List[OptimizationDatasetSpecification],
            None,
            None,
            [payload],
            None,
        )

        self._post_add_specification(name)

    def add_entries(self, entries: Union[OptimizationDatasetNewEntry, Iterable[OptimizationDatasetNewEntry]]):

        entries = make_list(entries)
        self.client._auto_request(
            "post",
            f"v1/datasets/optimization/{self.id}/entries/bulkCreate",
            List[OptimizationDatasetNewEntry],
            None,
            None,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
