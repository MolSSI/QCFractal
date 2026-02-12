from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from qcportal.dataset_models import BaseDataset
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationRecord, OptimizationSpecification


class OptimizationDatasetNewEntry(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    initial_molecule: Molecule | int
    additional_keywords: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    comment: str | None = None


class OptimizationDatasetEntry(OptimizationDatasetNewEntry):
    initial_molecule: Molecule


class OptimizationDatasetSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    specification: OptimizationSpecification
    description: str | None = None


class OptimizationDatasetRecordItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_name: str
    specification_name: str
    record_id: int
    record: OptimizationRecord | None


class OptimizationDataset(BaseDataset):
    dataset_type: Literal["optimization"] = "optimization"

    # Needed by the base class
    _entry_type = OptimizationDatasetEntry
    _new_entry_type = OptimizationDatasetNewEntry
    _specification_type = OptimizationDatasetSpecification
    _record_item_type = OptimizationDatasetRecordItem
    _record_type = OptimizationRecord

    def add_specification(
        self, name: str, specification: OptimizationSpecification, description: str | None = None
    ) -> InsertMetadata:
        spec = OptimizationDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: OptimizationDatasetNewEntry | Iterable[OptimizationDatasetNewEntry]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: OptimizationDatasetNewEntry | Iterable[OptimizationDatasetNewEntry]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecule: Molecule | int,
        additional_keywords: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        comment: str | None = None,
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
