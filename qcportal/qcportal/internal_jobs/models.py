from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Union

from dateutil.parser import parse as date_parser

try:
    from pydantic.v1 import BaseModel, Extra, validator
except ImportError:
    from pydantic import BaseModel, Extra, validator

from qcportal.base_models import QueryProjModelBase
from ..base_models import QueryIteratorBase


class InternalJobStatusEnum(str, Enum):
    """
    The state of a record object. The states which are available are a finite set.
    """

    complete = "complete"
    waiting = "waiting"
    running = "running"
    error = "error"
    cancelled = "cancelled"

    @classmethod
    def _missing_(cls, name):
        """Attempts to find the correct status in a case-insensitive way

        If a string being converted to an InternalJobStatusEnum is missing, then this function
        will convert the case and try to find the appropriate status.
        """
        name = name.lower()

        # Search this way rather than doing 'in' since we are comparing
        # a string to an enum
        for status in cls:
            if name == status:
                return status


class InternalJob(BaseModel):
    class Config:
        extra = Extra.forbid

    id: int
    name: str
    status: InternalJobStatusEnum
    added_date: datetime
    scheduled_date: datetime
    started_date: Optional[datetime]
    last_updated: Optional[datetime]
    ended_date: Optional[datetime]
    runner_hostname: Optional[str]
    runner_uuid: Optional[str]

    progress: int

    function: str
    kwargs: Dict[str, Any]
    after_function: Optional[str]
    after_function_kwargs: Optional[Dict[str, Any]]
    result: Any
    user: Optional[str]


class InternalJobQueryFilters(QueryProjModelBase):
    job_id: Optional[List[int]] = None
    name: Optional[List[str]] = None
    user: Optional[List[Union[int, str]]] = None
    runner_hostname: Optional[List[str]] = None
    status: Optional[List[InternalJobStatusEnum]] = None
    last_updated_before: Optional[datetime] = None
    last_updated_after: Optional[datetime] = None
    added_before: Optional[datetime] = None
    added_after: Optional[datetime] = None
    scheduled_before: Optional[datetime] = None
    scheduled_after: Optional[datetime] = None

    @validator(
        "last_updated_before",
        "last_updated_after",
        "added_before",
        "added_after",
        "scheduled_before",
        "scheduled_after",
        pre=True,
    )
    def parse_dates(cls, v):
        if isinstance(v, str):
            return date_parser(v)
        return v


class InternalJobQueryIterator(QueryIteratorBase[InternalJob]):
    """
    Iterator for internal job queries

    This iterator transparently handles batching and pagination over the results
    of an internal job query.
    """

    def __init__(self, client, query_filters: InternalJobQueryFilters):
        """
        Construct an iterator

        Parameters
        ----------
        client
            QCPortal client object used to contact/retrieve data from the server
        query_filters
            The actual query information to send to the server
        """

        batch_limit = client.api_limits["get_internal_jobs"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, batch_limit)

    def _request(self) -> List[InternalJob]:
        return self._client.make_request(
            "post",
            "api/v1/internal_jobs/query",
            List[InternalJob],
            body=self._query_filters,
        )
