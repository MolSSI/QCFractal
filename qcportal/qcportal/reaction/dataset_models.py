from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.reaction.record_models import ReactionRecord, ReactionSpecification
from qcportal.utils import make_list


class ReactionDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    stoichiometries: List[Tuple[float, Union[int, Molecule]]]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class ReactionDatasetEntryStoichiometry(BaseModel):
    coefficient: float
    molecule: Molecule


class ReactionDatasetEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    comment: Optional[str] = None
    stoichiometries: List[ReactionDatasetEntryStoichiometry]
    additional_keywords: Optional[Dict[str, Any]] = {}
    attributes: Optional[Dict[str, Any]] = {}


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

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, ReactionDatasetSpecification] = {}
    entries_: Dict[str, ReactionDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], ReactionRecord] = {}

    # Needed by the base class
    _entry_type = ReactionDatasetEntry
    _specification_type = ReactionDatasetSpecification
    _record_item_type = ReactionDatasetRecordItem
    _record_type = ReactionRecord

    def add_specification(
        self, name: str, specification: ReactionSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        spec = ReactionDatasetSpecification(name=name, specification=specification, description=description)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/reaction/{self.id}/specifications",
            InsertMetadata,
            body=[spec],
        )

        self._post_add_specification(name)
        return ret

    def add_entries(self, entries: Union[ReactionDatasetEntry, Iterable[ReactionDatasetNewEntry]]) -> InsertMetadata:

        entries = make_list(entries)
        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/reaction/{self.id}/entries/bulkCreate",
            InsertMetadata,
            body=make_list(entries),
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret

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
