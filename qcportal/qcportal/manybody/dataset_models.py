from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.manybody.record_models import ManybodyRecord, ManybodySpecification
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.utils import make_list


class ManybodyDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class ManybodyDatasetEntry(ManybodyDatasetNewEntry):
    initial_molecule: Molecule


class ManybodyDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: ManybodySpecification
    description: Optional[str] = None


class ManybodyDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[ManybodyRecord]


class ManybodyDataset(BaseDataset):
    dataset_type: Literal["manybody"] = "manybody"

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, ManybodyDatasetSpecification] = {}
    entries_: Dict[str, ManybodyDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], ManybodyRecord] = {}

    # Needed by the base class
    _entry_type = ManybodyDatasetEntry
    _specification_type = ManybodyDatasetSpecification
    _record_item_type = ManybodyDatasetRecordItem
    _record_type = ManybodyRecord

    def add_specification(
        self, name: str, specification: ManybodySpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        initial_molecules: Optional[List[Molecule]]

        spec = ManybodyDatasetSpecification(name=name, specification=specification, description=description)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/manybody/{self.id}/specifications",
            InsertMetadata,
            body=[spec],
        )

        self._post_add_specification(name)
        return ret

    def add_entries(self, entries: Union[ManybodyDatasetNewEntry, Iterable[ManybodyDatasetNewEntry]]) -> InsertMetadata:

        entries = make_list(entries)
        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/manybody/{self.id}/entries/bulkCreate",
            InsertMetadata,
            body=entries,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret

    def add_entry(
        self,
        name: str,
        initial_molecule: Union[int, Molecule],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):
        if additional_keywords is None:
            additional_keywords = {}
        if attributes is None:
            attributes = {}

        ent = ManybodyDatasetNewEntry(
            name=name,
            initial_molecule=initial_molecule,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
