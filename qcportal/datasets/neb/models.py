from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.neb import (
    NEBRecord,
    NEBKeywords,
)
from qcportal.utils import make_list
from .. import BaseDataset
from ...records import PriorityEnum


class NEBDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecules: List[Union[Molecule, int]]
    neb_keywords: NEBKeywords
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class NEBDatasetEntry(NEBDatasetNewEntry):
    initial_molecule_ids: List[int]
    initial_molecules: Optional[List[Molecule]] = None


# NEB dataset specifications are just qc specifications
# The neb keywords are stored in the entries ^^
class NEBDatasetSpecification(BaseModel):
    name: str
    specification: NEBSpecification
    description: Optional[str] = None


class NEBDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[NEBRecord._DataModel]


class NEBDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        collection_type: Literal["neb"] = "neb"

        # Specifications are always loaded
        specifications: Optional[Dict[str, NEBDatasetSpecification]] = None
        entries: Optional[Dict[str, NEBDatasetEntry]] = None
        record_items: Optional[List[NEBDatasetRecordItem]] = None

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["neb"] = "neb"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = NEBDatasetEntry
    _specification_type = NEBDatasetSpecification
    _record_item_type = NEBDatasetRecordItem
    _record_type = NEBRecord

    def add_specification(self, name: str, specification: NEBSpecification, description: Optional[str] = None):

        payload = NEBDatasetSpecification(name=name, specification=specification, description=description)

        self.client._auto_request(
            "post",
            f"v1/datasets/neb/{self.id}/specifications",
            List[NEBDatasetSpecification],
            None,
            None,
            [payload],
            None,
        )

        self._post_add_specification(name)

    def add_entries(self, entries: Union[NEBDatasetNewEntry, Iterable[NEBDatasetNewEntry]]):

        entries = make_list(entries)
        self.client._auto_request(
            "post",
            f"v1/datasets/neb/{self.id}/entries/bulkCreate",
            List[NEBDatasetNewEntry],
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


class NEBDatasetAddBody(RestModelBase):
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    group: Optional[str] = None
    provenance: Optional[Dict[str, Any]]
    visibility: bool = True
    default_tag: Optional[str] = None
    default_priority: PriorityEnum = PriorityEnum.normal
