from datetime import datetime
from enum import Enum

from dateutil.parser import parse as date_parser
from pydantic import BaseModel, field_validator, IPvAnyAddress, ConfigDict

from qcportal.base_models import (
    RestModelBase,
    QueryProjModelBase,
    QueryModelBase,
    validate_list_to_single,
    QueryIteratorBase,
)
from qcportal.common_types import LowerStr


class GroupByEnum(str, Enum):
    user = "user"
    day = "day"
    hour = "hour"
    country = "country"
    subdivision = "subdivision"


class DeleteBeforeDateBody(RestModelBase):
    before: datetime | None = None

    @field_validator("before", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class AccessLogQueryFilters(QueryProjModelBase):
    module: list[LowerStr] | None = None
    method: list[LowerStr] | None = None
    user: list[int | str] | None = None
    before: datetime | None = None
    after: datetime | None = None

    @field_validator("before", "after", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class AccessLogEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    timestamp: datetime
    method: str
    module: str | None
    full_uri: str | None

    request_duration: float | None
    request_bytes: float | None
    response_bytes: float | None

    user: str | None

    ip_address: IPvAnyAddress | None
    user_agent: str | None

    country_code: str | None
    subdivision: str | None
    city: str | None
    ip_lat: float | None
    ip_long: float | None


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

    def _request(self) -> list[AccessLogEntry]:
        return self._client.make_request(
            "post",
            "api/v1/access_logs/query",
            list[AccessLogEntry],
            body=self._query_filters,
        )


class AccessLogSummaryFilters(RestModelBase):
    group_by: GroupByEnum = GroupByEnum.day
    before: datetime | None = None
    after: datetime | None = None

    @field_validator("before", "after", "group_by", mode="before")
    @classmethod
    def validate_lists(cls, v):
        return validate_list_to_single(v)

    @field_validator("before", "after", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class AccessLogSummaryEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    module: str | None
    method: str
    count: int
    request_duration_info: list[float]
    response_bytes_info: list[float]


class AccessLogSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entries: dict[str, list[AccessLogSummaryEntry]]


class ErrorLogQueryFilters(QueryModelBase):
    error_id: list[int] | None = None
    user: list[int | str] | None = None
    before: datetime | None = None
    after: datetime | None = None

    @field_validator("before", "after", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class ErrorLogEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    error_date: datetime
    qcfractal_version: str
    error_text: str | None
    user: str | None

    request_path: str | None
    request_headers: str | None
    request_body: str | None


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

    def _request(self) -> list[ErrorLogEntry]:
        return self._client.make_request(
            "post",
            "api/v1/server_errors/query",
            list[ErrorLogEntry],
            body=self._query_filters,
        )
