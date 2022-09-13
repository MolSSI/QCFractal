from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple

from pydantic import BaseModel, Extra

from qcportal.base_models import QueryProjModelBase
from ..base_models import QueryIteratorBase
from ..metadata_models import QueryMetadata


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
    hostname: Optional[List[str]] = None
    status: Optional[List[InternalJobStatusEnum]] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None
    added_before: Optional[datetime] = None
    added_after: Optional[datetime] = None


class InternalJobQueryIterator(QueryIteratorBase):
    def __init__(self, client, query_filters: InternalJobQueryFilters):
        api_limit = client.api_limits["get_internal_jobs"] // 4
        QueryIteratorBase.__init__(self, client, query_filters, api_limit)

    def _request(self) -> Tuple[Optional[QueryMetadata], List[InternalJob]]:
        return self.client._auto_request(
            "post",
            "v1/internal_jobs/query",
            InternalJobQueryFilters,
            None,
            Tuple[Optional[QueryMetadata], List[InternalJob]],
            self.query_filters,
            None,
        )
