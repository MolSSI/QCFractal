from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from qcportal.dataset_models import BaseDataset
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.neb.record_models import (
    NEBRecord,
    NEBSpecification,
)


class NEBDatasetNewEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    initial_chain: list[int | Molecule]
    additional_keywords: dict[str, Any] = {}
    additional_singlepoint_keywords: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    comment: str | None = None


class NEBDatasetEntry(NEBDatasetNewEntry):
    initial_chain: list[Molecule]


# NEB dataset specification
class NEBDatasetSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    specification: NEBSpecification
    description: str | None = None


class NEBDatasetRecordItem(BaseModel):

    entry_name: str
    specification_name: str
    record_id: int
    record: NEBRecord | None


class NEBDataset(BaseDataset):
    dataset_type: Literal["neb"] = "neb"

    # Needed by the base class
    _entry_type = NEBDatasetEntry
    _new_entry_type = NEBDatasetNewEntry
    _specification_type = NEBDatasetSpecification
    _record_item_type = NEBDatasetRecordItem
    _record_type = NEBRecord

    def add_specification(
        self, name: str, specification: NEBSpecification, description: str | None = None
    ) -> InsertMetadata:
        spec = NEBDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(self, entries: NEBDatasetNewEntry | Iterable[NEBDatasetNewEntry]) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(self, entries: NEBDatasetNewEntry | Iterable[NEBDatasetNewEntry]) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_chain: list[int | Molecule],
        additional_keywords: dict[str, Any] | None = None,
        additional_singlepoint_keywords: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        comment: str | None = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if additional_singlepoint_keywords is None:
            additional_singlepoint_keywords = {}
        if attributes is None:
            attributes = {}

        ent = NEBDatasetNewEntry(
            name=name,
            initial_chain=initial_chain,
            additional_keywords=additional_keywords,
            additional_singlepoint_keywords=additional_singlepoint_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
