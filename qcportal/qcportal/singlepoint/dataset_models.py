from typing import Dict, Any, Union, Optional, Iterable

try:
    from pydantic.v1 import BaseModel, Extra, root_validator
except ImportError:
    from pydantic import BaseModel, Extra, root_validator
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.internal_jobs import InternalJob
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


class SinglepointDatasetEntriesFrom(BaseModel):

    dataset_id: Optional[int] = None
    dataset_type: Optional[str] = None
    dataset_name: Optional[str] = None
    specification_name: Optional[str] = None

    @root_validator
    def validate_input(cls, values):
        # Dataset id must be specified, or dataset type and name
        if values.get("dataset_id") is None:
            if values.get("dataset_type") is None or values.get("dataset_name") is None:
                raise ValueError("Either dataset_id or dataset_type and dataset_name must be specified.")

        if values.get("dataset_type") == "optimization" and values.get("specification_name") is None:
            raise ValueError("specification_name must be given for obtaining entries from an optimization dataset")

        return values


class SinglepointDataset(BaseDataset):
    dataset_type: Literal["singlepoint"] = "singlepoint"

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

    def background_add_entries(
        self, entries: Union[SinglepointDatasetNewEntry, Iterable[SinglepointDatasetNewEntry]]
    ) -> InternalJob:
        return self._background_add_entries(entries)

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

    def add_entries_from(
        self,
        *,
        dataset_type: Optional[str] = None,
        dataset_name: Optional[str] = None,
        dataset_id: Optional[str] = None,
        specification_name: Optional[str] = None,
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
