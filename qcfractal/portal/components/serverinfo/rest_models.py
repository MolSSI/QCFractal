from datetime import datetime
from enum import Enum
from typing import Optional, List

from pydantic import validator

from qcfractal.portal.common_rest import (
    validate_list_to_single,
    QueryProjParametersBase,
    QueryParametersBase,
    RestModelBase,
)


class GroupByEnum(str, Enum):
    user = "user"
    day = "day"
    hour = "hour"
    country = "country"
    subdivision = "subdivision"


class DeleteBeforeDateParameters(RestModelBase):
    before: Optional[datetime] = None

    @validator("before", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class AccessLogQueryParameters(QueryProjParametersBase):
    access_type: Optional[List[str]] = None
    access_method: Optional[List[str]] = None
    username: Optional[List[str]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class ErrorLogQueryParameters(QueryParametersBase):
    id: Optional[List[int]] = None
    username: Optional[List[str]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class ServerStatsQueryParameters(QueryParametersBase):
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class AccessLogQuerySummaryParameters(RestModelBase):
    group_by: GroupByEnum = GroupByEnum.day
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", "group_by", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)
