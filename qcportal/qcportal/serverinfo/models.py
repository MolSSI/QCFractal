from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Union

from dateutil.parser import parse as date_parser

try:
    from pydantic.v1 import BaseModel, Extra, validator, IPvAnyAddress, constr
except ImportError:
    from pydantic import BaseModel, Extra, validator, IPvAnyAddress, constr

from qcportal.base_models import (
    RestModelBase,
    QueryProjModelBase,
    QueryModelBase,
    validate_list_to_single,
    QueryIteratorBase,
)


class GroupByEnum(str, Enum):
    user = "user"
    day = "day"
    hour = "hour"
    country = "country"
    subdivision = "subdivision"


class DeleteBeforeDateBody(RestModelBase):
    before: Optional[datetime] = None


class AccessLogQueryFilters(QueryProjModelBase):
    module: Optional[List[constr(to_lower=True)]] = None
    method: Optional[List[constr(to_lower=True)]] = None
    user: Optional[List[Union[int, str]]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class AccessLogEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    timestamp: datetime
    method: str
    module: Optional[str]
    full_uri: Optional[str]

    request_duration: Optional[float]
    request_bytes: Optional[float]
    response_bytes: Optional[float]

    user: Optional[str]

    ip_address: Optional[IPvAnyAddress]
    user_agent: Optional[str]

    country_code: Optional[str]
    subdivision: Optional[str]
    city: Optional[str]
    ip_lat: Optional[float]
    ip_long: Optional[float]


class AccessLogQueryIterator(QueryIteratorBase[AccessLogEntry]):
    """
    Iterator for access log queries

    This iterator transparently handles batching and pagination over the results
    of an access log query
    """

    def __init__(self, client, query_filters: AccessLogQueryFilters):
        """
        Construct an iterator

        Parameters
        ----------
        client
            QCPortal client object used to contact/retrieve data from the server
        query_filters
            The actual query information to send to the server
        """

        batch_limit = client.api_limits["get_access_logs"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, batch_limit)

    def _request(self) -> List[AccessLogEntry]:
        return self._client.make_request(
            "post",
            "api/v1/access_logs/query",
            List[AccessLogEntry],
            body=self._query_filters,
        )


class AccessLogSummaryFilters(RestModelBase):
    group_by: GroupByEnum = GroupByEnum.day
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", "group_by", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)

    @validator("before", "after", pre=True)
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class AccessLogSummaryEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    module: Optional[str]
    method: str
    count: int
    request_duration_info: List[float]
    response_bytes_info: List[float]


class AccessLogSummary(BaseModel):
    class Config:
        extra = Extra.forbid

    entries: Dict[str, List[AccessLogSummaryEntry]]


class ErrorLogQueryFilters(QueryModelBase):
    error_id: Optional[List[int]] = None
    user: Optional[List[Union[int, str]]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class ErrorLogEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    error_date: datetime
    qcfractal_version: str
    error_text: Optional[str]
    user: Optional[str]

    request_path: Optional[str]
    request_headers: Optional[str]
    request_body: Optional[str]


class ErrorLogQueryIterator(QueryIteratorBase[ErrorLogEntry]):
    """
    Iterator for error log queries

    This iterator transparently handles batching and pagination over the results
    of an error log query
    """

    def __init__(self, client, query_filters: ErrorLogQueryFilters):
        """
        Construct an iterator

        Parameters
        ----------
        client
            QCPortal client object used to contact/retrieve data from the server
        query_filters
            The actual query information to send to the server
        """

        batch_limit = client.api_limits["get_error_logs"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, batch_limit)

    def _request(self) -> List[ErrorLogEntry]:
        return self._client.make_request(
            "post",
            "api/v1/server_errors/query",
            List[ErrorLogEntry],
            body=self._query_filters,
        )
