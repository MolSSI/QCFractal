from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.records.optimization import OptimizationInputSpecification, OptimizationSpecification
from qcportal.records.gridoptimization import (
    GridoptimizationInputSpecification,
    GridoptimizationRecord,
    GridoptimizationKeywords,
)
from qcportal.utils import make_list
from .. import BaseDataset
from ...records import PriorityEnum


class GridoptimizationDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecule: Union[Molecule, int]
    gridoptimization_keywords: GridoptimizationKeywords
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class GridoptimizationDatasetEntry(BaseModel):
    dataset_id: int
    name: str
    comment: Optional[str] = None
    initial_molecule_id: int
    gridoptimization_keywords: GridoptimizationKeywords
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


# Gridoptimization dataset specifications are just optimization specifications
# The gridoptimization keywords are stored in the entries ^^
class GridoptimizationDatasetInputSpecification(BaseModel):
    name: str
    specification: OptimizationInputSpecification
    description: Optional[str] = None


class GridoptimizationDatasetSpecification(BaseModel):
    dataset_id: int
    name: str
    specification: OptimizationSpecification
    description: Optional[str] = None


class GridoptimizationDatasetRecordItem(BaseModel):
    dataset_id: int
    entry_name: str
    specification_name: str
    record_id: int


class GridoptimizationDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        collection_type: Literal["gridoptimization"]

        # Specifications are always loaded
        specifications: Dict[str, GridoptimizationDatasetSpecification]
        entries: Optional[List[GridoptimizationDatasetEntry]]
        record_items: Optional[List[GridoptimizationDatasetRecordItem]]

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["gridoptimization"]
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = GridoptimizationDatasetEntry
    _specification_type = GridoptimizationDatasetSpecification
    _record_item_type = GridoptimizationDatasetRecordItem
    _record_type = GridoptimizationRecord

    def add_specification(
        self, name: str, specification: GridoptimizationInputSpecification, description: Optional[str] = None
    ):

        payload = GridoptimizationDatasetInputSpecification(
            name=name, specification=specification, description=description
        )

        self.client._auto_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/specifications",
            List[GridoptimizationDatasetInputSpecification],
            None,
            None,
            [payload],
            None,
        )

        self._post_add_specification(name)

    def add_entries(self, entries: Union[GridoptimizationDatasetEntry, Iterable[GridoptimizationDatasetNewEntry]]):

        entries = make_list(entries)
        self.client._auto_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/entries/bulkCreate",
            List[GridoptimizationDatasetNewEntry],
            None,
            None,
            make_list(entries),
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)


#######################
# Web API models
#######################


class GridoptimizationDatasetAddBody(RestModelBase):
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    group: Optional[str] = None
    provenance: Optional[Dict[str, Any]]
    visibility: bool = True
    default_tag: Optional[str] = None
    default_priority: PriorityEnum = PriorityEnum.normal


class GridoptimizationDatasetDeleteEntryBody(RestModelBase):
    names: List[str]
    delete_records: bool = False


class GridoptimizationDatasetDeleteSpecificationBody(RestModelBase):
    names: List[str]
    delete_records: bool = False
