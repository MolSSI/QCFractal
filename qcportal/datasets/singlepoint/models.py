from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.records.singlepoint import SinglepointRecord, QCSpecification
from qcportal.utils import make_list
from ..models import BaseDataset


class SinglepointDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class SinglepointDatasetEntry(SinglepointDatasetNewEntry):
    molecule: Molecule
    local_results: Optional[Dict[str, Any]] = None


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
        dataset_type: Literal["singlepoint"] = "singlepoint"

        specifications: Dict[str, SinglepointDatasetSpecification] = {}
        entries: Dict[str, SinglepointDatasetEntry] = {}
        record_map: Dict[Tuple[str, str], SinglepointRecord] = {}

        contributed_values: Any

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["singlepoint"] = "singlepoint"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = SinglepointDatasetEntry
    _specification_type = SinglepointDatasetSpecification
    _record_item_type = SinglepointDatasetRecordItem
    _record_type = SinglepointRecord

    def add_specification(
        self, name: str, specification: QCSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        payload = SinglepointDatasetSpecification(name=name, specification=specification, description=description)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/singlepoint/{self.id}/specifications",
            List[SinglepointDatasetSpecification],
            None,
            InsertMetadata,
            [payload],
            None,
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[SinglepointDatasetNewEntry, Iterable[SinglepointDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)
        ret = self.client._auto_request(
            "post",
            f"v1/datasets/singlepoint/{self.id}/entries/bulkCreate",
            List[SinglepointDatasetNewEntry],
            None,
            InsertMetadata,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)

        return ret
