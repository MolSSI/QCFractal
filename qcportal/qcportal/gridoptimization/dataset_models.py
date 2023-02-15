from typing import Dict, Any, Union, Optional, List, Iterable, Tuple

from pydantic import BaseModel, Extra
from typing_extensions import Literal

from qcportal.dataset_models import BaseDataset
from qcportal.gridoptimization.record_models import (
    GridoptimizationRecord,
    GridoptimizationSpecification,
)
from qcportal.metadata_models import InsertMetadata
from qcportal.molecules import Molecule
from qcportal.utils import make_list


class GridoptimizationDatasetNewEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    additional_optimization_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}
    comment: Optional[str] = None


class GridoptimizationDatasetEntry(GridoptimizationDatasetNewEntry):
    initial_molecule: Molecule


class GridoptimizationDatasetSpecification(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    specification: GridoptimizationSpecification
    description: Optional[str] = None


class GridoptimizationDatasetRecordItem(BaseModel):
    class Config:
        extra = Extra.forbid

    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[GridoptimizationRecord]


class GridoptimizationDataset(BaseDataset):
    dataset_type: Literal["gridoptimization"] = "gridoptimization"

    ########################################
    # Caches of information
    ########################################
    specifications_: Dict[str, GridoptimizationDatasetSpecification] = {}
    entries_: Dict[str, GridoptimizationDatasetEntry] = {}
    record_map_: Dict[Tuple[str, str], GridoptimizationRecord] = {}

    # Needed by the base class
    _entry_type = GridoptimizationDatasetEntry
    _specification_type = GridoptimizationDatasetSpecification
    _record_item_type = GridoptimizationDatasetRecordItem
    _record_type = GridoptimizationRecord

    def add_specification(
        self, name: str, specification: GridoptimizationSpecification, description: Optional[str] = None
    ) -> InsertMetadata:

        spec = GridoptimizationDatasetSpecification(name=name, specification=specification, description=description)

        ret = self._client.make_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/specifications",
            InsertMetadata,
            body=[spec],
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[GridoptimizationDatasetNewEntry, Iterable[GridoptimizationDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)
        ret = self._client.make_request(
            "post",
            f"v1/datasets/gridoptimization/{self.id}/entries/bulkCreate",
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
        additional_optimization_keywords: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        comment: Optional[str] = None,
    ):

        ent = GridoptimizationDatasetNewEntry(
            name=name,
            initial_molecule=initial_molecule,
            additional_keywords=additional_keywords,
            additional_optimization_keywords=additional_optimization_keywords,
            attributes=attributes,
            comment=comment,
        )

        return self.add_entries(ent)
