from typing import Dict, Any, Union, Optional, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.manybody.record_models import ManybodyRecord, ManybodySpecification
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule


class ManybodyDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class ManybodyDatasetEntry(ManybodyDatasetNewEntry):
    initial_molecule: Molecule


class ManybodyDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: ManybodySpecification
    description: Optional[str] = None


class ManybodyDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[ManybodyRecord]


class ManybodyDataset(BaseDataset):
    dataset_type: Literal["manybody"] = "manybody"

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, ManybodyDatasetSpecification] = {}
    entries_: Dict[str, ManybodyDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], ManybodyRecord] = {}

    # Needed by the base class
    _entry_type = ManybodyDatasetEntry
    _new_entry_type = ManybodyDatasetNewEntry
    _specification_type = ManybodyDatasetSpecification
    _record_item_type = ManybodyDatasetRecordItem
    _record_type = ManybodyRecord

    def add_specification(
        self, name: str, specification: ManybodySpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        spec = ManybodyDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(self, entries: Union[ManybodyDatasetNewEntry, Iterable[ManybodyDatasetNewEntry]]) -> InsertMetadata:
        return self._add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecule: Union[int, Molecule],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if attributes is None:
            attributes = {}

        ent = ManybodyDatasetNewEntry(
            name=name,
            initial_molecule=initial_molecule,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
