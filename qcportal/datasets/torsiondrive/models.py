from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.records.optimization import OptimizationSpecification
from qcportal.records.torsiondrive import (
    TorsiondriveRecord,
    TorsiondriveKeywords,
)
from qcportal.utils import make_list
from .. import BaseDataset
from ...records import PriorityEnum


class TorsiondriveDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecules: List[Union[Molecule, int]]
    torsiondrive_keywords: TorsiondriveKeywords
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class TorsiondriveDatasetEntry(TorsiondriveDatasetNewEntry):
    initial_molecule_ids: List[int]
    initial_molecules: Optional[List[Molecule]] = None


# Torsiondrive dataset specifications are just optimization specifications
# The torsiondrive keywords are stored in the entries ^^
class TorsiondriveDatasetSpecification(BaseModel):
    name: str
    specification: OptimizationSpecification
    description: Optional[str] = None


class TorsiondriveDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int


class TorsiondriveDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        collection_type: Literal["torsiondrive"]

        # Specifications are always loaded
        specifications: Dict[str, TorsiondriveDatasetSpecification]
        entries: Optional[List[TorsiondriveDatasetEntry]]
        record_items: Optional[List[TorsiondriveDatasetRecordItem]]

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["torsiondrive"]
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = TorsiondriveDatasetEntry
    _specification_type = TorsiondriveDatasetSpecification
    _record_item_type = TorsiondriveDatasetRecordItem
    _record_type = TorsiondriveRecord

    def add_specification(self, name: str, specification: OptimizationSpecification, description: Optional[str] = None):

        payload = TorsiondriveDatasetSpecification(name=name, specification=specification, description=description)

        self.client._auto_request(
            "post",
            f"v1/datasets/torsiondrive/{self.id}/specifications",
            List[TorsiondriveDatasetSpecification],
            None,
            None,
            [payload],
            None,
        )

        self._post_add_specification(name)

    def add_entries(self, entries: Union[TorsiondriveDatasetNewEntry, Iterable[TorsiondriveDatasetNewEntry]]):

        entries = make_list(entries)
        self.client._auto_request(
            "post",
            f"v1/datasets/torsiondrive/{self.id}/entries/bulkCreate",
            List[TorsiondriveDatasetNewEntry],
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


class TorsiondriveDatasetAddBody(RestModelBase):
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    group: Optional[str] = None
    provenance: Optional[Dict[str, Any]]
    visibility: bool = True
    default_tag: Optional[str] = None
    default_priority: PriorityEnum = PriorityEnum.normal
