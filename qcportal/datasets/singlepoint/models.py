from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.records.singlepoint import SinglepointRecord, QCSpecification
from qcportal.utils import make_list
from .. import BaseDataset
from ...records import PriorityEnum


class SinglepointDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class SinglepointDatasetEntry(SinglepointDatasetNewEntry):
    molecule_id: int
    molecule: Optional[Molecule] = None


class SinglepointDatasetSpecification(BaseModel):
    name: str
    specification: QCSpecification
    description: Optional[str] = None


class SinglepointDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[SinglepointRecord._DataModel]


class SinglepointDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        collection_type: Literal["singlepoint"]

        # Specifications are always loaded
        specifications: Dict[str, SinglepointDatasetSpecification]
        entries: Optional[List[SinglepointDatasetEntry]]
        record_items: Optional[List[SinglepointDatasetRecordItem]]

        contributed_values: Any

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["singlepoint"]
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = SinglepointDatasetEntry
    _specification_type = SinglepointDatasetSpecification
    _record_item_type = SinglepointDatasetRecordItem
    _record_type = SinglepointRecord

    def add_specification(self, name: str, specification: QCSpecification, description: Optional[str] = None):

        payload = SinglepointDatasetSpecification(name=name, specification=specification, description=description)

        self.client._auto_request(
            "post",
            f"v1/datasets/singlepoint/{self.id}/specifications",
            List[SinglepointDatasetSpecification],
            None,
            None,
            [payload],
            None,
        )

        self._post_add_specification(name)

    def add_entries(self, entries: Union[SinglepointDatasetNewEntry, Iterable[SinglepointDatasetNewEntry]]):

        entries = make_list(entries)
        self.client._auto_request(
            "post",
            f"v1/datasets/singlepoint/{self.id}/entries/bulkCreate",
            List[SinglepointDatasetNewEntry],
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


class SinglepointDatasetAddBody(RestModelBase):
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    group: Optional[str] = None
    provenance: Optional[Dict[str, Any]]
    visibility: bool = True
    default_tag: Optional[str] = None
    default_priority: PriorityEnum = PriorityEnum.normal
