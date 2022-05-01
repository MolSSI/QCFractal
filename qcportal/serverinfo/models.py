from datetime import datetime
from enum import Enum
from typing import Optional, List

from pydantic import validator

from qcportal.base_models import RestModelBase, QueryProjModelBase, QueryModelBase, validate_list_to_single


class GroupByEnum(str, Enum):
    user = "user"
    day = "day"
    hour = "hour"
    country = "country"
    subdivision = "subdivision"


class DeleteBeforeDateBody(RestModelBase):
    before: Optional[datetime] = None


class AccessLogQueryFilters(QueryProjModelBase):
    access_type: Optional[List[str]] = None
    access_method: Optional[List[str]] = None
    username: Optional[List[str]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None


class ErrorLogQueryFilters(QueryModelBase):
    error_id: Optional[List[int]] = None
    username: Optional[List[str]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None


class ServerStatsQueryFilters(QueryModelBase):
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class AccessLogSummaryFilters(RestModelBase):
    group_by: GroupByEnum = GroupByEnum.day
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", "group_by", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)
