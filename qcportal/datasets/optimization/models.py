from datetime import datetime
from typing import Dict, Any, Union, Optional, List, Iterable

from pydantic import BaseModel
from typing_extensions import Literal

from qcportal.base_models import RestModelBase
from qcportal.molecules import Molecule
from qcportal.records.optimization import OptimizationRecord, OptimizationInputSpecification, OptimizationSpecification
from .. import BaseDataset, DatasetGetEntryBody, DatasetGetRecordItemsBody
from ...records import PriorityEnum


class OptimizationDatasetNewEntry(BaseModel):
    name: str
    initial_molecule: Union[Molecule, int]
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class OptimizationDatasetEntry(BaseModel):
    dataset_id: int
    name: str
    initial_molecule_id: int
    additional_keywords: Dict[str, Any] = {}
    attributes: Dict[str, Any] = {}


class OptimizationDatasetInputSpecification(BaseModel):
    name: str
    specification: OptimizationInputSpecification
    comment: Optional[str] = None


class OptimizationDatasetSpecification(BaseModel):
    dataset_id: int
    name: str
    specification: OptimizationSpecification
    comment: Optional[str] = None


class OptimizationDatasetRecordItem(BaseModel):
    dataset_id: int
    entry_name: str
    specification_name: str
    record_id: int


class OptimizationDataset(BaseDataset):
    class _DataModel(BaseDataset._DataModel):
        entry_names: Optional[List[str]]

        # Specifications are always loaded
        specifications: Dict[str, OptimizationDatasetSpecification]
        entries: Optional[List[OptimizationDatasetEntry]]
        record_items: Optional[List[OptimizationDatasetRecordItem]]

        # Actual optimization records, mapped via id
        record_cache: Optional[Dict[int, OptimizationRecord]]

    # This is needed for disambiguation by pydantic
    dataset_type: Literal["optimization"]
    raw_data: _DataModel

    def fetch_entries(self, entry_names: Optional[Iterable[str]] = None):
        url_params = {"name": entry_names}

        self.raw_data.entries = self.client._auto_request(
            "get",
            f"v1/dataset/optimization/{self.id}/entry",
            None,
            DatasetGetEntryBody,
            List[OptimizationDatasetEntry],
            None,
            url_params,
        )

    def fetch_specifications(self):
        self.raw_data.specifications = self.client._auto_request(
            "get",
            f"v1/dataset/optimization/{self.id}/specification",
            None,
            None,
            Dict[str, OptimizationDatasetSpecification],
            None,
            None,
        )

    def fetch_record_items(
        self,
        specification_names: Optional[Iterable[str]] = None,
        entry_names: Optional[Iterable[str]] = None,
    ):

        url_params = {
            "specification_name": specification_names,
            "entry_name": entry_names,
        }

        record_info = self.client._auto_request(
            "get",
            f"v1/dataset/optimization/{self.id}/record",
            None,
            DatasetGetRecordItemsBody,
            List[OptimizationDatasetRecordItem],
            None,
            url_params,
        )

        if self.raw_data.record_items is None:
            self.raw_data.record_items = record_info
        else:
            # Merge in newly-downloaded records
            # what spec names and entries did we just download
            new_info = [(x.specification_name, x.entry_name) for x in record_info]

            # Remove any items that match what we just downloaded, and then extend the list with the new items
            self.raw_data.record_items = [
                x for x in self.raw_data.record_items if (x.specification_name, x.entry_name) not in new_info
            ]
            self.raw_data.record_items.extend(record_info)

    def fetch_records(
        self,
        specification_names: Optional[Iterable[str]] = None,
        entry_names: Optional[Iterable[str]] = None,
        modified_after: Optional[datetime] = None,
    ):

        if self.raw_data.record_cache is None:
            self.raw_data.record_cache = {}

        record_items = self.raw_data.record_items.copy()

        if specification_names is not None:
            record_items = [x for x in record_items if x.specification_name in specification_names]

        if entry_names is not None:
            record_items = [x for x in record_items if x.entry_name in entry_names]

        opt_ids = [x.record_id for x in record_items]

        to_skip = 0
        while True:
            meta, fetched_opts = self.client.query_optimizations(
                record_id=opt_ids, modified_after=modified_after, skip=to_skip
            )

            if not meta.success:
                raise RuntimeError(meta.error_string)

            if meta.n_returned == 0:
                break

            self.raw_data.record_cache.update({x.id: x for x in fetched_opts})
            to_skip += meta.n_returned

    def status(self) -> Dict[str, Any]:
        return self.client._auto_request(
            "get", f"v1/dataset/optimization/{self.id}/status", None, None, Dict[str, Any], None, None
        )

    @property
    def default_tag(self):
        return self.raw_data.default_tag

    @property
    def default_priority(self):
        return self.raw_data.default_priority

    @property
    def specifications(self):
        return self.raw_data.specifications

    @property
    def entry_names(self):
        return self.raw_data.entry_names

    @property
    def entries(self):
        if self.raw_data.entries is None:
            self.fetch_entries()

        return self.raw_data.entries

    @property
    def record_items(self):
        if self.raw_data.record_items is None:
            self.fetch_record_items()
        return self.raw_data.record_items

    @property
    def records(self):
        # full map of all entry/spec combinations
        record_map = {
            spec_name: {entry_name: None for entry_name in self.entry_names} for spec_name in self.specifications.keys()
        }

        # fill in the records we have
        if self.raw_data.record_items is None:
            self.fetch_record_items()
        if self.raw_data.record_cache is None:
            self.fetch_records()

        for item in self.raw_data.record_items:
            record_map[item.specification_name][item.entry_name] = self.raw_data.record_cache.get(item.record_id, None)

        return record_map

    def add_specification(
        self, name: str, specification: OptimizationInputSpecification, comment: Optional[str] = None
    ):

        payload = OptimizationDatasetInputSpecification(name=name, specification=specification, comment=comment)

        self.client._auto_request(
            "post",
            f"v1/dataset/optimization/{self.id}/specification",
            List[OptimizationDatasetInputSpecification],
            None,
            None,
            [payload],
            None,
        )

    def add_entries(self, entry: Iterable[OptimizationDatasetNewEntry]):

        self.client._auto_request(
            "post",
            f"v1/dataset/optimization/{self.id}/entry",
            List[OptimizationDatasetNewEntry],
            None,
            None,
            list(entry),
            None,
        )

    def submit(
        self,
        specification_names: Optional[Iterable[str]] = None,
        entry_names: Optional[Iterable[str]] = None,
        tag: Optional[str] = None,
        priority: PriorityEnum = None,
    ):

        payload = {
            "specification_name": specification_names,
            "entry_name": entry_names,
            "tag": tag,
            "priority": priority,
        }

        return self.client._auto_request(
            "post", f"v1/dataset/optimization/{self.id}/submit", OptimizationDatasetSubmitBody, None, Any, payload, None
        )


#######################
# Web API models
#######################


class OptimizationDatasetAddBody(RestModelBase):
    name: str
    description: Optional[str] = None
    tagline: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None
    group: Optional[str] = None
    provenance: Optional[Dict[str, Any]]
    visibility: bool = True
    default_tag: Optional[str] = None
    default_priority: PriorityEnum = PriorityEnum.normal


class OptimizationDatasetDeleteEntryBody(RestModelBase):
    names: List[str]
    delete_records: bool = False


class OptimizationDatasetDeleteSpecificationBody(RestModelBase):
    names: List[str]
    delete_records: bool = False


class OptimizationDatasetSubmitBody(RestModelBase):
    specification_name: Optional[List[str]] = None
    entry_name: Optional[List[str]] = None
    tag: Optional[str] = None
    priority: Optional[PriorityEnum] = None
