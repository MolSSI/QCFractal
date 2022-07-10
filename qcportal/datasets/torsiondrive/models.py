from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.records.torsiondrive import TorsiondriveRecord, TorsiondriveSpecification
from qcportal.utils import make_list
from ..models import BaseDataset


class TorsiondriveDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_molecules: List[Union[Molecule, int]]
    additional_keywords: Dict[str, Any] = {}
    additional_optimization_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class TorsiondriveDatasetEntry(TorsiondriveDatasetNewEntry):
    initial_molecules: List[Molecule]


# Torsiondrive dataset specifications are just optimization specifications
# The torsiondrive keywords are stored in the entries ^^
class TorsiondriveDatasetSpecification(BaseModel):
    name: str
    specification: TorsiondriveSpecification
    description: Optional[str] = None


class TorsiondriveDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[TorsiondriveRecord._DataModel]


class TorsiondriveDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        dataset_type: Literal["torsiondrive"] = "torsiondrive"

        specifications: Dict[str, TorsiondriveDatasetSpecification] = {}
        entries: Dict[str, TorsiondriveDatasetEntry] = {}
        record_map: Dict[Tuple[str, str], TorsiondriveRecord] = {}

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["torsiondrive"] = "torsiondrive"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = TorsiondriveDatasetEntry
    _specification_type = TorsiondriveDatasetSpecification
    _record_item_type = TorsiondriveDatasetRecordItem
    _record_type = TorsiondriveRecord

    def add_specification(
        self, name: str, specification: TorsiondriveSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        payload = TorsiondriveDatasetSpecification(name=name, specification=specification, description=description)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/torsiondrive/{self.id}/specifications",
            List[TorsiondriveDatasetSpecification],
            None,
            InsertMetadata,
            [payload],
            None,
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[TorsiondriveDatasetNewEntry, Iterable[TorsiondriveDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)
        ret = self.client._auto_request(
            "post",
            f"v1/datasets/torsiondrive/{self.id}/entries/bulkCreate",
            List[TorsiondriveDatasetNewEntry],
            None,
            InsertMetadata,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret
