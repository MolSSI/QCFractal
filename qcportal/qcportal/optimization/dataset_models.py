from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationRecord, OptimizationSpecification
from qcportal.utils import make_list


class OptimizationDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class OptimizationDatasetEntry(OptimizationDatasetNewEntry):
    initial_molecule: Molecule


class OptimizationDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: OptimizationSpecification
    description: Optional[str] = None


class OptimizationDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[OptimizationRecord]


class OptimizationDataset(BaseDataset):
    dataset_type: Literal["optimization"] = "optimization"

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, OptimizationDatasetSpecification] = {}
    entries_: Dict[str, OptimizationDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], OptimizationRecord] = {}

    # Needed by the base class
    _entry_type = OptimizationDatasetEntry
    _specification_type = OptimizationDatasetSpecification
    _record_item_type = OptimizationDatasetRecordItem
    _record_type = OptimizationRecord

    def add_specification(
        self, name: str, specification: OptimizationSpecification, description: Optional[str] = None
    ) -> InsertMetadata:
        initial_molecules: Optional[List[Molecule]]

        spec = OptimizationDatasetSpecification(name=name, specification=specification, description=description)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/optimization/{self.id}/specifications",
            InsertMetadata,
            body=[spec],
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[OptimizationDatasetNewEntry, Iterable[OptimizationDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)

        ret = self._client.make_request(
            "post",
            f"api/v1/datasets/optimization/{self.id}/entries/bulkCreate",
            InsertMetadata,
            body=entries,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret

    def add_entry(
        self,
        name: str,
        initial_molecule: Union[Molecule, int],
        additional_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):

        if additional_keywords is None:
            additional_keywords = {}
        if attributes is None:
            attributes = {}

        ent = OptimizationDatasetNewEntry(
            name=name,
            initial_molecule=initial_molecule,
            additional_keywords=additional_keywords,
            attributes=attributes,
            comment=comment,
        )
        return self.add_entries(ent)
