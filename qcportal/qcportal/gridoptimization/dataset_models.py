from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from qcportal.dataset_models import BaseDataset
from qcportal.gridoptimization.record_models import (
    GridoptimizationRecord,
    GridoptimizationSpecification,
)
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule


class GridoptimizationDatasetNewEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    initial_molecule: int | Molecule
    additional_keywords: dict[str, Any] = {}
    additional_optimization_keywords: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    comment: str | None = None


class GridoptimizationDatasetEntry(GridoptimizationDatasetNewEntry):
    initial_molecule: Molecule


class GridoptimizationDatasetSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    specification: GridoptimizationSpecification
    description: str | None = None


class GridoptimizationDatasetRecordItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_name: str
    specification_name: str
    record_id: int
    record: GridoptimizationRecord | None


class GridoptimizationDataset(BaseDataset):
    dataset_type: Literal["gridoptimization"] = "gridoptimization"

    # Needed by the base class
    _entry_type = GridoptimizationDatasetEntry
    _new_entry_type = GridoptimizationDatasetNewEntry
    _specification_type = GridoptimizationDatasetSpecification
    _record_item_type = GridoptimizationDatasetRecordItem
    _record_type = GridoptimizationRecord

    def add_specification(
        self, name: str, specification: GridoptimizationSpecification, description: str | None = None
    ) -> InsertMetadata:
        spec = GridoptimizationDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: GridoptimizationDatasetNewEntry | Iterable[GridoptimizationDatasetNewEntry]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: GridoptimizationDatasetNewEntry | Iterable[GridoptimizationDatasetNewEntry]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecule: int | Molecule,
        additional_keywords: dict[str, Any] | None = None,
        additional_optimization_keywords: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        comment: str | None = None,
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
