import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List, Union

from dateutil.parser import parse as date_parser

try:
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr
except ImportError:
    from pydantic import BaseModel, Extra, validator, PrivateAttr

from qcportal.base_models import QueryProjModelBase
from qcportal.utils import seconds_to_hms
from ..base_models import QueryIteratorBase
from tqdm import tqdm


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
    repeat_delay: Optional[int]
    serial_group: Optional[str]

    progress: int
    progress_description: Optional[str] = None

    function: str
    kwargs: Dict[str, Any]
    after_function: Optional[str]
    after_function_kwargs: Optional[Dict[str, Any]]
    result: Any
    user: Optional[str]

    _client: Any = PrivateAttr(None)
    _refresh_url: Optional[str] = PrivateAttr(None)

    def __init__(self, client=None, refresh_url=None, **kwargs):
        BaseModel.__init__(self, **kwargs)
        self._client = client
        self._refresh_url = refresh_url

    def refresh(self):
        """
        Updates the data of this object with information from the server
        """

        if self._client is None:
            raise RuntimeError("Client is not set")

        if self._refresh_url is None:
            server_data = self._client.get_internal_job(self.id)
        else:
            server_data = self._client.make_request("get", self._refresh_url, InternalJob)

        for k, v in server_data:
            setattr(self, k, v)

    def watch(self, interval: float = 2.0, timeout: Optional[float] = None):
        """
        Watch an internal job for completion

        Will poll every `interval` seconds until the job is finished (complete, error, cancelled, etc).

        Parameters
        ----------
        interval
            Time (in seconds) between polls on the server
        timeout
            Max amount of time (in seconds) to wait. If None, will wait forever.

        Returns
        -------

        """

        if self.status not in [InternalJobStatusEnum.waiting, InternalJobStatusEnum.running]:
            return

        begin_time = time.time()

        end_time = None
        if timeout is not None:
            end_time = begin_time + timeout

        pbar = tqdm(initial=self.progress, total=100, desc=self.progress_description)

        while True:
            t = time.time()

            self.refresh()
            pbar.update(self.progress - pbar.n)
            pbar.set_description(self.progress_description)

            if end_time is not None and t >= end_time:
                raise TimeoutError("Timed out waiting for job to complete")

            if self.status == InternalJobStatusEnum.error:
                print("Internal job resulted in an error:")
                print(self.result)
                break
            elif self.status not in [InternalJobStatusEnum.waiting, InternalJobStatusEnum.running]:
                print(f"Internal job final status: {self.status.value}")
                break

            curtime = time.time()

            if end_time is not None:
                #  sleep the normal interval, or up to the timeout time
                time.sleep(min(interval, end_time - curtime + 0.1))
            else:
                time.sleep(interval)

    @property
    def duration(self) -> timedelta:
        if self.started_date is None:
            return timedelta()

        if self.ended_date is None:
            return datetime.now() - self.started_date

        return self.ended_date - self.started_date

    @property
    def duration_str(self) -> str:
        return seconds_to_hms(self.duration.total_seconds())


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
        ij_dicts = self._client.make_request(
            "post",
            "api/v1/internal_jobs/query",
            List[Dict[str, Any]],
            body=self._query_filters,
        )

        return [InternalJob(client=self, **ij_dict) for ij_dict in ij_dicts]
