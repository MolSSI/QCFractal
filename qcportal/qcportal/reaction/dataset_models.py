from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

try:
    from pydantic.v1 import BaseModel, Extra
except ImportError:
    from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.internal_jobs import InternalJob
from qcportal.reaction.record_models import ReactionRecord, ReactionSpecification


class ReactionDatasetEntryStoichiometry(BaseModel):
    coefficient: float
    molecule: Molecule


class ReactionDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    stoichiometries: List[Union[ReactionDatasetEntryStoichiometry, Tuple[float, Union[int, Molecule]]]]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class ReactionDatasetEntry(ReactionDatasetNewEntry):
    class Config:
        extra = Extra.forbid

    stoichiometries: List[ReactionDatasetEntryStoichiometry]


class ReactionDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: ReactionSpecification
    description: Optional[str] = None


class ReactionDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[ReactionRecord]


class ReactionDataset(BaseDataset):
    dataset_type: Literal["reaction"] = "reaction"

    # Needed by the base class
    _entry_type = ReactionDatasetEntry
    _new_entry_type = ReactionDatasetNewEntry
    _specification_type = ReactionDatasetSpecification
    _record_item_type = ReactionDatasetRecordItem
    _record_type = ReactionRecord

    def add_specification(
        self, name: str, specification: ReactionSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        spec = ReactionDatasetSpecification(name=name, specification=specification, description=description)
        return self._add_specifications(spec)

    def add_entries(self, entries: Union[ReactionDatasetEntry, Iterable[ReactionDatasetNewEntry]]) -> InsertMetadata:
        return self._add_entries(entries)

    def background_add_entries(
        self, entries: Union[ReactionDatasetNewEntry, Iterable[ReactionDatasetNewEntry]]
    ) -> InternalJob:
        return self._background_add_entries(entries)

    def add_entry(
        self,
        name: str,
        stoichiometries: List[Tuple[float, Union[int, Molecule]]],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
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
