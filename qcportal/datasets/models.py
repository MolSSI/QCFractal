from __future__ import annotations

import abc
from datetime import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Extra, validator

from qcportal.base_models import validate_list_to_single, RestModelBase
from qcportal.records import PriorityEnum


class BaseDataset(abc.ABC, BaseModel):
    class _DataModel(BaseModel):
        class Config:
            extra = Extra.forbid
            allow_mutation = True
            validate_assignment = True

        id: int
        name: str
        collection: str
        collection_type: str
        lname: str
        description: Optional[str]
        tags: Optional[Dict[str, Any]]
        tagline: Optional[str]
        group: Optional[str]
        visibility: bool
        provenance: Optional[Dict[str, Any]]

        default_tag: Optional[str]
        default_priority: PriorityEnum

        extra: Optional[Dict[str, Any]] = None

    client: Any
    raw_data: _DataModel  # Meant to be overridden by derived classes

    @property
    def id(self) -> int:
        return self.raw_data.id


class DatasetQueryModel(RestModelBase):
    dataset_type: Optional[str] = None
    name: Optional[str] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class DatasetGetEntryBody(RestModelBase):
    name: Optional[List[str]] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    missing_ok: bool = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class DatasetGetRecordItemsBody(RestModelBase):
    specification_name: Optional[List[str]] = None
    entry_name: Optional[List[str]] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    modified_after: datetime = None

    @validator("modified_after", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)
