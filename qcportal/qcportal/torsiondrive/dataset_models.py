from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator

from qcportal.dataset_models import BaseDataset
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
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


class TorsiondriveDatasetEntriesFrom(BaseModel):

    dataset_id: int | None = None
    dataset_type: str | None = None
    dataset_name: str | None = None
    specification_name: str | None = None

    @model_validator(mode="after")
    def validate_input(self):
        # Dataset id must be specified, or dataset type and name
        if self.dataset_id is None:
            if self.dataset_type is None or self.dataset_name is None:
                raise ValueError("Either dataset_id or dataset_type and dataset_name must be specified.")

        if self.dataset_type == "optimization" and self.specification_name is None:
            raise ValueError("specification_name must be given for obtaining entries from an optimization dataset")

        return self


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

    def add_entries_from(
        self,
        *,
        dataset_type: str | None = None,
        dataset_name: str | None = None,
        dataset_id: int | None = None,
        specification_name: str | None = None,
    ) -> InsertCountsMetadata:
        """
        Adds entries to this dataset by copying them from another dataset

        The source dataset may be another torsiondrive dataset, or an optimization dataset.

        When copying from another torsiondrive dataset, entries are copied whole - including their
        ``additional_keywords`` and ``additional_optimization_keywords``, and therefore their scan
        definitions.

        When copying from an optimization dataset, ``specification_name`` is required, and the single
        initial molecule of each new entry is the optimized molecule of the corresponding record.
        Entries whose record is not complete are skipped. Note that an optimization entry has no
        scan definition to copy, so ``additional_keywords`` and ``additional_optimization_keywords``
        of the new entries are empty - the dihedrals to scan must come from the dataset
        specification, or the entries must be added manually instead.

        Entries whose name already exists in this dataset are ignored.

        Parameters
        ----------
        dataset_type
            Type of the dataset to copy entries from. Must be given together with ``dataset_name``
        dataset_name
            Name of the dataset to copy entries from. Must be given together with ``dataset_type``
        dataset_id
            ID of the dataset to copy entries from. May be given instead of the type and name
        specification_name
            Specification of the source dataset to take molecules from. Required when copying
            from an optimization dataset, and unused otherwise

        Returns
        -------
        :
            Metadata about how many entries were added
        """

        body = TorsiondriveDatasetEntriesFrom(
            dataset_type=dataset_type,
            dataset_name=dataset_name,
            dataset_id=dataset_id,
            specification_name=specification_name,
        )

        return self._client.make_request(
            "post",
            f"api/v1/datasets/{self.dataset_type}/{self.id}/entries/addFrom",
            InsertCountsMetadata,
            body=body,
        )
