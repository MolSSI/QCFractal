from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.singlepoint.record_models import (
    SinglepointRecord,
    QCSpecification,
)
from qcportal.utils import make_list


class SinglepointDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class SinglepointDatasetEntry(SinglepointDatasetNewEntry):
    molecule: Molecule
    local_results: Optional[Dict[str, Any]] = None


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


class SinglepointDataset(BaseDataset):
    dataset_type: Literal["singlepoint"] = "singlepoint"

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, SinglepointDatasetSpecification] = {}
    entries_: Dict[str, SinglepointDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], SinglepointRecord] = {}

    # Needed by the base class
    _entry_type = SinglepointDatasetEntry
    _specification_type = SinglepointDatasetSpecification
    _record_item_type = SinglepointDatasetRecordItem
    _record_type = SinglepointRecord

    def add_specification(
        self, name: str, specification: QCSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        spec = SinglepointDatasetSpecification(name=name, specification=specification, description=description)

        ret = self._client.make_request(
            "post",
            f"v1/datasets/singlepoint/{self.id}/specifications",
            InsertMetadata,
            body=[spec],
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[SinglepointDatasetNewEntry, Iterable[SinglepointDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)
        ret = self._client.make_request(
            "post",
            f"v1/datasets/singlepoint/{self.id}/entries/bulkCreate",
            InsertMetadata,
            body=entries,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)

        return ret

    def add_entry(
        self,
        name: str,
        molecule: Union[Molecule, int],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):

        additional_keywords = {} if additional_keywords is None else additional_keywords
        attributes = {} if attributes is None else attributes

        ent = SinglepointDatasetNewEntry(
            name=name,
            molecule=molecule,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )
        return self.add_entries(ent)
