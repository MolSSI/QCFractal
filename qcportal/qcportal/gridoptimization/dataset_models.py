from typing import Dict, Any, Union, Optional, Iterable

try:
    from pydantic.v1 import BaseModel, Extra
except ImportError:
    from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.gridoptimization.record_models import (
    GridoptimizationRecord,
    GridoptimizationSpecification,
)
from qcportal.metadata_models import InsertMetadata
from qcportal.internal_jobs import InternalJob
from qcportal.molecules import Molecule


class GridoptimizationDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    additional_optimization_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class GridoptimizationDatasetEntry(GridoptimizationDatasetNewEntry):
    initial_molecule: Molecule


class GridoptimizationDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: GridoptimizationSpecification
    description: Optional[str] = None


class GridoptimizationDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[GridoptimizationRecord]


class GridoptimizationDataset(BaseDataset):
    dataset_type: Literal["gridoptimization"] = "gridoptimization"

    # Needed by the base class
    _entry_type = GridoptimizationDatasetEntry
    _new_entry_type = GridoptimizationDatasetNewEntry
    _specification_type = GridoptimizationDatasetSpecification
    _record_item_type = GridoptimizationDatasetRecordItem
    _record_type = GridoptimizationRecord

    def add_specification(
        self, name: str, specification: GridoptimizationSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        spec = GridoptimizationDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: Union[GridoptimizationDatasetNewEntry, Iterable[GridoptimizationDatasetNewEntry]]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: Union[GridoptimizationDatasetNewEntry, Iterable[GridoptimizationDatasetNewEntry]]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecule: Union[Molecule, int],
        additional_keywords: Optional[Dict[str, Any]] = None,
        additional_optimization_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if additional_optimization_keywords is None:
            additional_optimization_keywords = {}
        if attributes is None:
            attributes = {}

        ent = GridoptimizationDatasetNewEntry(
            name=name,
            initial_molecule=initial_molecule,
            additional_keywords=additional_keywords,
            additional_optimization_keywords=additional_optimization_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
