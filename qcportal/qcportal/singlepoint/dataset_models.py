from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, model_validator, ConfigDict

from qcportal.dataset_models import BaseDataset
from qcportal.internal_jobs import InternalJob
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.singlepoint.record_models import (
    SinglepointRecord,
    QCSpecification,
)


class SinglepointDatasetNewEntry(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    molecule: Molecule | int
    additional_keywords: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    comment: str | None = None
    local_results: dict[str, Any] | None = None


class SinglepointDatasetEntry(SinglepointDatasetNewEntry):
    molecule: Molecule


class SinglepointDatasetSpecification(BaseModel):

    model_config = ConfigDict(extra="forbid")

    name: str
    specification: QCSpecification
    description: str | None = None


class SinglepointDatasetRecordItem(BaseModel):

    model_config = ConfigDict(extra="forbid")

    entry_name: str
    specification_name: str
    record_id: int
    record: SinglepointRecord | None


class SinglepointDatasetEntriesFrom(BaseModel):

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


class SinglepointDataset(BaseDataset):
    dataset_type: Literal["singlepoint"] = "singlepoint"

    # Needed by the base class
    _entry_type = SinglepointDatasetEntry
    _new_entry_type = SinglepointDatasetNewEntry
    _specification_type = SinglepointDatasetSpecification
    _record_item_type = SinglepointDatasetRecordItem
    _record_type = SinglepointRecord

    def add_specification(
        self, name: str, specification: QCSpecification, description: str | None = None
    ) -> InsertMetadata:
        spec = SinglepointDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(self, entries: SinglepointDatasetNewEntry | Iterable[SinglepointDatasetNewEntry]) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: SinglepointDatasetNewEntry | Iterable[SinglepointDatasetNewEntry]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        molecule: Molecule | int,
        additional_keywords: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        comment: str | None = None,
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

    def add_entries_from(
        self,
        *,
        dataset_type: str | None = None,
        dataset_name: str | None = None,
        dataset_id: str | None = None,
        specification_name: str | None = None,
    ) -> InsertCountsMetadata:
        body = SinglepointDatasetEntriesFrom(
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
