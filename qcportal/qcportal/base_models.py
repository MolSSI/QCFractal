from __future__ import annotations

from typing import Optional, List, Iterator, Generic, TypeVar

try:
    from pydantic.v1 import BaseModel, validator, Extra
except ImportError:
    from pydantic import BaseModel, validator, Extra

T = TypeVar("T")


def validate_list_to_single(v):
    """
    Converts a list to a single value (the last element of the list)

    Query parameters (in a URI) can be specified multiple times. Therefore, we will always
    convert them to a list in flask. But that means we have to convert them
    to single values here
    """
    if isinstance(v, list):
        # take the last value, if specified multiple times
        return v[-1]
    else:
        return v


class RestModelBase(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True


class CommonBulkGetBody(RestModelBase):
    """
    Common URL parameters for "get_*" functions

    These functions typically take a list for ids, and a bool for missing_ok
    """

    ids: List[int]
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    missing_ok: bool = False


class CommonBulkGetNamesBody(RestModelBase):
    """
    Common URL parameters for "get_*" functions

    These functions typically take a list for ids, and a bool for missing_ok
    """

    names: List[str]
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    missing_ok: bool = False


class ProjURLParameters(RestModelBase):
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class QueryModelBase(RestModelBase):
    """
    Common parameters for query_* functions, without include/exclude

    These can be either URL parameters or part of a POST body
    """

    limit: Optional[int] = None
    cursor: Optional[int] = None

    @validator("limit", "cursor", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class QueryProjModelBase(QueryModelBase, ProjURLParameters):
    """
    Common parameters for query_* functions, with include/exclude (projection)

    These can be either URL parameters or part of a POST body
    """

    pass


class QueryIteratorBase(Generic[T]):
    """
    Base class for all query result iterators

    Query iterators are used to iterate intelligently over the result of a query.
    This handles pagination, where only batches are downloaded from the server.
    """

    def __init__(self, client, query_filters: QueryModelBase, batch_limit: int):
        self._query_filters = query_filters
        self._client = client

        # The limit for a single batch
        self._batch_limit = batch_limit

        # Total number of rows/whatever we want to fetch
        self._total_limit = query_filters.limit

        self.reset()

    def reset(self):
        """
        Starts retrieval of results from the beginning again
        """

        self._current_batch: Optional[List[T]] = None
        self._fetched: int = 0

        self._fetch_batch()

    def _request(self) -> List[T]:
        raise NotImplementedError("_request must be overridden by a derived class")

    def _fetch_batch(self) -> None:
        # We have already fetched something before
        # Add the cursor to the query filters
        if self._current_batch:
            self._query_filters.cursor = self._current_batch[-1].id

        self._current_pos = 0

        # Have we fetched everything?
        if self._total_limit is not None and self._fetched >= self._total_limit:
            self._current_batch = []
            return

        # adjust how many to get in this batch, taking into account any limit
        # specified by the user
        if self._total_limit is not None:
            new_limit = min(self._total_limit - self._fetched, self._batch_limit)
        else:
            new_limit = self._batch_limit

        self._query_filters.limit = new_limit

        self._current_batch = self._request()
        self._fetched += len(self._current_batch)

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        # This can happen if there is none returned on the first iteration
        # Check here so we don't fetch twice
        if len(self._current_batch) == 0:
            raise StopIteration

        if self._current_pos >= len(self._current_batch):
            # At the end of the current batch. Fetch the next
            self._fetch_batch()

            # If we didn't get any, then that's all there is
            if len(self._current_batch) == 0:
                raise StopIteration

        ret = self._current_batch[self._current_pos]
        self._current_pos += 1
        return ret
