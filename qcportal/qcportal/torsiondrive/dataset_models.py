from typing import Dict, Any, Union, Optional, List, Iterable

try:
    from pydantic.v1 import BaseModel, Extra, root_validator
except ImportError:
    from pydantic import BaseModel, Extra, root_validator
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.molecules import Molecule
from qcportal.internal_jobs import InternalJob
from qcportal.torsiondrive.record_models import TorsiondriveRecord, TorsiondriveSpecification


class TorsiondriveDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecules: List[Union[Molecule, int]]
    additional_keywords: Dict[str, Any] = {}
    additional_optimization_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class TorsiondriveDatasetEntry(TorsiondriveDatasetNewEntry):
    initial_molecules: List[Molecule]


# Torsiondrive dataset specifications are just optimization specifications
# The torsiondrive keywords are stored in the entries ^^
class TorsiondriveDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: TorsiondriveSpecification
    description: Optional[str] = None


class TorsiondriveDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[TorsiondriveRecord]


class TorsiondriveDatasetEntriesFrom(BaseModel):

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


class TorsiondriveDataset(BaseDataset):
    dataset_type: Literal["torsiondrive"] = "torsiondrive"

    # Needed by the base class
    _entry_type = TorsiondriveDatasetEntry
    _new_entry_type = TorsiondriveDatasetNewEntry
    _specification_type = TorsiondriveDatasetSpecification
    _record_item_type = TorsiondriveDatasetRecordItem
    _record_type = TorsiondriveRecord

    def add_specification(
        self, name: str, specification: TorsiondriveSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        spec = TorsiondriveDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(
        self, entries: Union[TorsiondriveDatasetNewEntry, Iterable[TorsiondriveDatasetNewEntry]]
    ) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: Union[TorsiondriveDatasetNewEntry, Iterable[TorsiondriveDatasetNewEntry]]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_molecules: List[Union[Molecule, int]],
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
        dataset_type: Optional[str] = None,
        dataset_name: Optional[str] = None,
        dataset_id: Optional[int] = None,
        specification_name: Optional[str] = None,
    ) -> InsertCountsMetadata:
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
