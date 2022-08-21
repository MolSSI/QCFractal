from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.records.manybody import ManybodyRecord, ManybodySpecification
from qcportal.utils import make_list
from ..models import BaseDataset


class ManybodyDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class ManybodyDatasetEntry(ManybodyDatasetNewEntry):
    initial_molecule: Molecule


class ManybodyDatasetSpecification(BaseModel):
    name: str
    specification: ManybodySpecification
    description: Optional[str] = None


class ManybodyDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[ManybodyRecord._DataModel]


class ManybodyDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        dataset_type: Literal["manybody"] = "manybody"

        specifications: Dict[str, ManybodyDatasetSpecification] = {}
        entries: Dict[str, ManybodyDatasetEntry] = {}
        record_map: Dict[Tuple[str, str], ManybodyRecord] = {}

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["manybody"] = "manybody"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = ManybodyDatasetEntry
    _specification_type = ManybodyDatasetSpecification
    _record_item_type = ManybodyDatasetRecordItem
    _record_type = ManybodyRecord

    def add_specification(
        self, name: str, specification: ManybodySpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        initial_molecules: Optional[List[Molecule]]

        payload = ManybodyDatasetSpecification(name=name, specification=specification, description=description)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/manybody/{self.id}/specifications",
            List[ManybodyDatasetSpecification],
            None,
            InsertMetadata,
            [payload],
            None,
        )

        self._post_add_specification(name)
        return ret

    def add_entries(self, entries: Union[ManybodyDatasetNewEntry, Iterable[ManybodyDatasetNewEntry]]) -> InsertMetadata:

        entries = make_list(entries)
        ret = self.client._auto_request(
            "post",
            f"v1/datasets/manybody/{self.id}/entries/bulkCreate",
            List[ManybodyDatasetNewEntry],
            None,
            InsertMetadata,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret
