from datetime import datetime
from enum import Enum
from typing import Optional, List, Tuple, Dict, Any

from pydantic import BaseModel, Extra, validator, IPvAnyAddress, constr

from qcportal.base_models import (
    RestModelBase,
    QueryProjModelBase,
    QueryModelBase,
    validate_list_to_single,
    QueryIteratorBase,
)
from qcportal.metadata_models import QueryMetadata


class GroupByEnum(str, Enum):
    user = "user"
    day = "day"
    hour = "hour"
    country = "country"
    subdivision = "subdivision"


class DeleteBeforeDateBody(RestModelBase):
    before: Optional[datetime] = None


class AccessLogQueryFilters(QueryProjModelBase):
    access_type: Optional[List[constr(to_lower=True)]] = None
    access_method: Optional[List[constr(to_lower=True)]] = None
    username: Optional[List[str]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None


class AccessLogEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    access_date: datetime
    access_method: str
    access_type: str
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


class AccessLogQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: AccessLogQueryFilters):
        api_limit = client.api_limits["get_access_logs"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[AccessLogEntry]]:
        return self.client._auto_request(
            "post",
            "v1/access_logs/query",
            AccessLogQueryFilters,
            None,
            Tuple[Optional[QueryMetadata], List[AccessLogEntry]],
            self.query_filters,
            None,
        )


class AccessLogSummaryFilters(RestModelBase):
    group_by: GroupByEnum = GroupByEnum.day
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", "group_by", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class AccessLogSummaryEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    access_type: str
    access_method: str
    count: int
    request_duration_info: List[float]
    response_bytes_info: List[float]


class AccessLogSummary(BaseModel):
    class Config:
        extra = Extra.forbid

    entries: Dict[str, List[AccessLogSummaryEntry]]

    # TODO - lots of stuff here about analysis, plotting, etc


class ErrorLogQueryFilters(QueryModelBase):
    error_id: Optional[List[int]] = None
    username: Optional[List[str]] = None
    before: Optional[datetime] = None
    after: Optional[datetime] = None


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


class ErrorLogQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: ErrorLogQueryFilters):
        api_limit = client.api_limits["get_error_logs"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[ErrorLogEntry]]:
        return self.client._auto_request(
            "post",
            "v1/server_errors/query",
            ErrorLogQueryFilters,
            None,
            Tuple[Optional[QueryMetadata], List[ErrorLogEntry]],
            self.query_filters,
            None,
        )


class ServerStatsQueryFilters(QueryModelBase):
    before: Optional[datetime] = None
    after: Optional[datetime] = None

    @validator("before", "after", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class ServerStatsEntry(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    timestamp: datetime

    collection_count: Optional[int]
    molecule_count: Optional[int]
    record_count: Optional[int]
    outputstore_count: Optional[int]
    access_count: Optional[int]
    error_count: Optional[int]

    task_queue_status: Optional[Dict[str, Any]]
    service_queue_status: Optional[Dict[str, Any]]

    db_total_size: Optional[int]
    db_table_size: Optional[int]
    db_index_size: Optional[int]
    db_table_information: Dict[str, Any]


class ServerStatsQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: ServerStatsQueryFilters):
        api_limit = client.api_limits["get_server_stats"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[ServerStatsEntry]]:
        return self.client._auto_request(
            "post",
            "v1/server_stats/query",
            ServerStatsQueryFilters,
            None,
            Tuple[Optional[QueryMetadata], List[ServerStatsEntry]],
            self.query_filters,
            None,
        )
