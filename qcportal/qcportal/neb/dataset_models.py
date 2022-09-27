from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.neb.record_models import (
    NEBRecord,
    NEBSpecification,
)
from qcportal.utils import make_list


class NEBDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    comment: Optional[str] = None
    initial_chain: List[Union[int, Molecule]]
    additional_keywords: Dict[str, Any] = {}
    additional_singlepoint_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class NEBDatasetEntry(NEBDatasetNewEntry):
    initial_chain: List[Molecule]


# NEB dataset specification
class NEBDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: NEBSpecification
    description: Optional[str] = None


class NEBDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[NEBRecord._DataModel]


class NEBDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        dataset_type: Literal["neb"] = "neb"

        specifications: Optional[Dict[str, NEBDatasetSpecification]] = {}
        entries: Optional[Dict[str, NEBDatasetEntry]] = {}
        record_map: Optional[List[NEBRecord]] = {}

    raw_data: _DataModel

    # Needed by the base class
    _entry_type = NEBDatasetEntry
    _specification_type = NEBDatasetSpecification
    _record_item_type = NEBDatasetRecordItem
    _record_type = NEBRecord

    def add_specification(
        self, name: str, specification: NEBSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        payload = NEBDatasetSpecification(name=name, specification=specification, description=description)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/neb/{self.id}/specifications",
            List[NEBDatasetSpecification],
            None,
            InsertMetadata,
            [payload],
            None,
        )

        self._post_add_specification(name)
        return ret

    def add_entries(self, entries: Union[NEBDatasetNewEntry, Iterable[NEBDatasetNewEntry]]) -> InsertMetadata:

        entries = make_list(entries)
        ret = self.client._auto_request(
            "post",
            f"v1/datasets/neb/{self.id}/entries/bulkCreate",
            List[NEBDatasetNewEntry],
            None,
            InsertMetadata,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret
