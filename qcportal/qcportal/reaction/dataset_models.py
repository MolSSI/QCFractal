from collections.abc import Iterable
from typing import Any
from typing import Literal

from pydantic import BaseModel, ConfigDict

from qcportal.dataset_models import BaseDataset
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.reaction.record_models import ReactionRecord, ReactionSpecification


class ReactionDatasetEntryStoichiometry(BaseModel):
    coefficient: float
    molecule: Molecule


class ReactionDatasetNewEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    stoichiometries: list[ReactionDatasetEntryStoichiometry | tuple[float, int | Molecule]]
    additional_keywords: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    comment: str | None = None


class ReactionDatasetEntry(ReactionDatasetNewEntry):
    model_config = ConfigDict(extra="forbid")

    stoichiometries: list[ReactionDatasetEntryStoichiometry]


class ReactionDatasetSpecification(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    specification: ReactionSpecification
    description: str | None = None


class ReactionDatasetRecordItem(BaseModel):

    model_config = ConfigDict(extra="forbid")

    entry_name: str
    specification_name: str
    record_id: int
    record: ReactionRecord | None


class ReactionDataset(BaseDataset):
    dataset_type: Literal["reaction"] = "reaction"

    # Needed by the base class
    _entry_type = ReactionDatasetEntry
    _new_entry_type = ReactionDatasetNewEntry
    _specification_type = ReactionDatasetSpecification
    _record_item_type = ReactionDatasetRecordItem
    _record_type = ReactionRecord

    def add_specification(
        self, name: str, specification: ReactionSpecification, description: str | None = None
    ) -> InsertMetadata:
        spec = ReactionDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(self, entries: ReactionDatasetNewEntry | Iterable[ReactionDatasetNewEntry]) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: ReactionDatasetNewEntry | Iterable[ReactionDatasetNewEntry]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        stoichiometries: list[tuple[float, int | Molecule]],
        additional_keywords: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        comment: str | None = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if attributes is None:
            attributes = {}

        ent = ReactionDatasetNewEntry(
            name=name,
            stoichiometries=stoichiometries,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
