from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.records.optimization import OptimizationRecord, OptimizationSpecification
from qcportal.utils import make_list
from .. import BaseDataset
from ...records import PriorityEnum


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


class OptimizationDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        collection_type: Literal["optimization"]

        # Specifications are always loaded
        specifications: Dict[str, OptimizationDatasetSpecification]
        entries: Optional[List[OptimizationDatasetEntry]]
        record_items: Optional[List[OptimizationDatasetRecordItem]]

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["optimization"]
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = OptimizationDatasetEntry
    _specification_type = OptimizationDatasetSpecification
    _record_item_type = OptimizationDatasetRecordItem
    _record_type = OptimizationRecord

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


#######################
# Web API models
#######################


class OptimizationDatasetAddBody(RestModelBase):
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    group: Optional[str] = None
    provenance: Optional[Dict[str, Any]]
    visibility: bool = True
    default_tag: Optional[str] = None
    default_priority: PriorityEnum = PriorityEnum.normal
