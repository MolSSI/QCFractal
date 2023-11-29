from typing import Dict, Any, Union, Optional, Iterable, Tuple

try:
    from pydantic.v1 import BaseModel, Extra
except ImportError:
    from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.singlepoint.record_models import (
    SinglepointRecord,
    QCSpecification,
)


class SinglepointDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None
    local_results: Optional[Dict[str, Any]] = None


class SinglepointDatasetEntry(SinglepointDatasetNewEntry):
    molecule: Molecule


class SinglepointDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: QCSpecification
    description: Optional[str] = None


class SinglepointDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[SinglepointRecord]


class SinglepointDataset(BaseDataset):
    dataset_type: Literal["singlepoint"] = "singlepoint"

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, SinglepointDatasetSpecification] = {}
    entries_: Dict[str, SinglepointDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], SinglepointRecord] = {}

    # Needed by the base class
    _entry_type = SinglepointDatasetEntry
    _new_entry_type = SinglepointDatasetNewEntry
    _specification_type = SinglepointDatasetSpecification
    _record_item_type = SinglepointDatasetRecordItem
    _record_type = SinglepointRecord

    def add_specification(
        self, name: str, specification: QCSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        spec = SinglepointDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: Union[SinglepointDatasetNewEntry, Iterable[SinglepointDatasetNewEntry]]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def add_entry(
        self,
        name: str,
        molecule: Union[Molecule, int],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if attributes is None:
            attributes = {}

        ent = SinglepointDatasetNewEntry(
            name=name,
            molecule=molecule,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )
        return self.add_entries(ent)
