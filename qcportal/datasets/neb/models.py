from typing import Dict, Any, Union, Optional, List, Iterable, Set

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.molecules import Molecule
from qcportal.metadata_models import InsertMetadata
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.neb import (
    NEBRecord,
    NEBKeywords,
    NEBSpecification,
)
from qcportal.utils import make_list
from ..models import BaseDataset


class NEBDatasetNewEntry(BaseModel):
    name: str
    comment: Optional[str] = None
    initial_chain: List[Union[Molecule, int]]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class NEBDatasetEntry(NEBDatasetNewEntry):
    #initial_chain_ids: List[int]
    initial_chain: List[Molecule]


# NEB dataset specifications are just qc specifications
class NEBDatasetSpecification(BaseModel):
    name: str
    specification: NEBSpecification
    description: Optional[str] = None


class NEBDatasetRecordItem(BaseModel):
    entry_name: str
    specification_name: str
    record_id: int
    record: Optional[NEBRecord._DataModel]


class NEBDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        dataset_type: Literal["neb"] = "neb"

        specifications: Optional[Dict[str, NEBDatasetSpecification]] = None
        entries: Optional[Dict[str, NEBDatasetEntry]] = None
        record_map: Optional[List[NEBRecord]] = None

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["neb"] = "neb"
    raw_data: _DataModel

    # Needed by the base class
    _entry_type = NEBDatasetEntry
    _specification_type = NEBDatasetSpecification
    _record_item_type = NEBDatasetRecordItem
    _record_type = NEBRecord

    #@staticmethod
    #def transform_entry_includes(includes: Optional[Iterable[str]]) -> Optional[Set[str]]:
    #    if includes is None:
    #        return None

    #    ret = BaseDataset.transform_entry_includes(includes)

    #    ret |= {"initial_chain", "initial_chain.molecule"}
    #    return ret

    def add_specification(
        self, name: str, specification: NEBSpecification, description: Optional[str] = None
    )-> InsertMetadata:

        payload = NEBDatasetSpecification(name=name, specification=specification, description=description)

        ret = self.client._auto_request(
            "post",
            f"v1/datasets/neb/{self.id}/specifications",
            List[NEBDatasetSpecification],
            None,
            InsertMetadata,
            [payload],
            None,
        )

        self._post_add_specification(name)
        return ret

    def add_entries(
        self, entries: Union[NEBDatasetNewEntry, Iterable[NEBDatasetNewEntry]]
    ) -> InsertMetadata:

        entries = make_list(entries)
        ret = self.client._auto_request(
            "post",
            f"v1/datasets/neb/{self.id}/entries/bulkCreate",
            List[NEBDatasetNewEntry],
            None,
            InsertMetadata,
            entries,
            None,
        )

        new_names = [x.name for x in entries]
        self._post_add_entries(new_names)
        return ret