from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from qcportal.dataset_models import BaseDataset
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.torsiondrive.record_models import TorsiondriveRecord, TorsiondriveSpecification


class TorsiondriveDatasetNewEntry(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    initial_molecules: list[int | Molecule]
    additional_keywords: dict[str, Any] = {}
    additional_optimization_keywords: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    comment: str | None = None


class TorsiondriveDatasetEntry(TorsiondriveDatasetNewEntry):
    initial_molecules: list[Molecule]


# Torsiondrive dataset specifications are just optimization specifications
# The torsiondrive keywords are stored in the entries ^^
class TorsiondriveDatasetSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    specification: TorsiondriveSpecification
    description: str | None = None


class TorsiondriveDatasetRecordItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_name: str
    specification_name: str
    record_id: int
    record: TorsiondriveRecord | None


class TorsiondriveDataset(BaseDataset):
    dataset_type: Literal["torsiondrive"] = "torsiondrive"

    # Needed by the base class
    _entry_type = TorsiondriveDatasetEntry
    _new_entry_type = TorsiondriveDatasetNewEntry
    _specification_type = TorsiondriveDatasetSpecification
    _record_item_type = TorsiondriveDatasetRecordItem
    _record_type = TorsiondriveRecord

    def add_specification(
        self, name: str, specification: TorsiondriveSpecification, description: str | None = None
    ) -> InsertMetadata:
        spec = TorsiondriveDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: TorsiondriveDatasetNewEntry | Iterable[TorsiondriveDatasetNewEntry]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: TorsiondriveDatasetNewEntry | Iterable[TorsiondriveDatasetNewEntry]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecules: list[int | Molecule],
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

        ent = TorsiondriveDatasetNewEntry(
            name=name,
            initial_molecules=initial_molecules,
            additional_keywords=additional_keywords,
            additional_optimization_keywords=additional_optimization_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
