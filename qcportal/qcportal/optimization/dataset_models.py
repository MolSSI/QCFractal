from typing import Dict, Any, Union, Optional, Iterable

try:
    from pydantic.v1 import BaseModel, Extra
except ImportError:
    from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.internal_jobs import InternalJob
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationRecord, OptimizationSpecification


class OptimizationDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class OptimizationDatasetEntry(OptimizationDatasetNewEntry):
    initial_molecule: Molecule


class OptimizationDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: OptimizationSpecification
    description: Optional[str] = None


class OptimizationDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[OptimizationRecord]


class OptimizationDataset(BaseDataset):
    dataset_type: Literal["optimization"] = "optimization"

    # Needed by the base class
    _entry_type = OptimizationDatasetEntry
    _new_entry_type = OptimizationDatasetNewEntry
    _specification_type = OptimizationDatasetSpecification
    _record_item_type = OptimizationDatasetRecordItem
    _record_type = OptimizationRecord

    def add_specification(
        self, name: str, specification: OptimizationSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        spec = OptimizationDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: Union[OptimizationDatasetNewEntry, Iterable[OptimizationDatasetNewEntry]]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: Union[OptimizationDatasetNewEntry, Iterable[OptimizationDatasetNewEntry]]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecule: Union[Molecule, int],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if attributes is None:
            attributes = {}

        ent = OptimizationDatasetNewEntry(
            name=name,
            initial_molecule=initial_molecule,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )
        return self.add_entries(ent)
