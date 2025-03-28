from typing import Dict, Any, Union, Optional, List, Iterable

try:
    from pydantic.v1 import BaseModel, Extra
except ImportError:
    from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.internal_jobs import InternalJob
from qcportal.molecules import Molecule
from qcportal.neb.record_models import (
    NEBRecord,
    NEBSpecification,
)


class NEBDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_chain: List[Union[int, Molecule]]
    additional_keywords: Dict[str, Any] = {}
    additional_singlepoint_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class NEBDatasetEntry(NEBDatasetNewEntry):
    initial_chain: List[Molecule]


# NEB dataset specification
class NEBDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: NEBSpecification
    description: Optional[str] = None


class NEBDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[NEBRecord]


class NEBDataset(BaseDataset):
    dataset_type: Literal["neb"] = "neb"

    # Needed by the base class
    _entry_type = NEBDatasetEntry
    _new_entry_type = NEBDatasetNewEntry
    _specification_type = NEBDatasetSpecification
    _record_item_type = NEBDatasetRecordItem
    _record_type = NEBRecord

    def add_specification(
        self, name: str, specification: NEBSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        spec = NEBDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(self, entries: Union[NEBDatasetNewEntry, Iterable[NEBDatasetNewEntry]]) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(self, entries: Union[NEBDatasetNewEntry, Iterable[NEBDatasetNewEntry]]) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        initial_chain: List[Union[Molecule, int]],
        additional_keywords: Optional[Dict[str, Any]] = None,
        additional_singlepoint_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
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
