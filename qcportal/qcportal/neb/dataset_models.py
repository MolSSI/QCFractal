from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.neb.record_models import (
    NEBRecord,
    NEBSpecification,
)
from qcportal.utils import make_list


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

    specifications_: Dict[str, NEBDatasetSpecification] = {}
    entries_: Dict[str, NEBDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], NEBRecord] = {}

    # Needed by the base class
    _entry_type = NEBDatasetEntry
    _specification_type = NEBDatasetSpecification
    _record_item_type = NEBDatasetRecordItem
    _record_type = NEBRecord

    def add_specification(
        self, name: str, specification: NEBSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        spec = NEBDatasetSpecification(name=name, specification=specification, description=description)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/neb/{self.id}/specifications",
            InsertMetadata,
            body=[spec],
        )

        self._post_add_specification(name)
        return ret

    def add_entries(self, entries: Union[NEBDatasetNewEntry, Iterable[NEBDatasetNewEntry]]) -> InsertMetadata:

        entries = make_list(entries)
        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/neb/{self.id}/entries/bulkCreate",
            InsertMetadata,
            body=entries,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret

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
