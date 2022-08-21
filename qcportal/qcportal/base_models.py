from typing import Optional, List, Any, Tuple

from pydantic import BaseModel, validator, Extra

from qcportal.metadata_models import QueryMetadata


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
    Common URL parameters for get_ functions

    These functions typically take a list for ids, and a bool for missing_ok
    """

    ids: List[int]
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    missing_ok: bool = False


class CommonBulkGetNamesBody(RestModelBase):
    """
    Common URL parameters for get_ functions

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
    include_metadata: bool = True

    @validator("limit", "cursor", "include_metadata", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class QueryProjModelBase(QueryModelBase, ProjURLParameters):
    """
    Common parameters for query_* functions, with include/exclude (projection)

    These can be either URL parameters or part of a POST body
    """

    pass


class QueryIteratorBase:
    def __init__(self, client, query_filters: QueryModelBase, api_limit: int):
        self.query_filters = query_filters
        self.client = client

        # The limit for a single batch
        self.batch_limit = api_limit

        # Total number of rows/whatever we want to fetch
        self.total_limit = query_filters.limit

        self.reset()

    def reset(self):
        # Fetch metadata on first iteration
        self.query_filters.include_metadata = True

        self.current_batch: Optional[List[Any]] = None
        self.current_meta: Optional[QueryMetadata] = None
        self.fetched: int = 0

        self._fetch_batch()

    def _request(self) -> Tuple[Optional[QueryMetadata], List[Any]]:
        raise NotImplementedError("_request must be overridden by a derived class")

    def _fetch_batch(self):
        # We have already fetched something before
        # Add the cursor to the query filters, and don't update metadata
        if self.current_batch:
            self.query_filters.cursor = self.current_batch[-1].id
            self.query_filters.include_metadata = False

        self.current_pos = 0

        # Have we fetched everything?
        if self.total_limit is not None and self.fetched >= self.total_limit:
            self.current_batch = []
            if self.current_meta is None:
                self.current_meta = QueryMetadata()
            return

        # adjust how many to get in this batch, taking into account any limit
        # specified by the user
        if self.total_limit is not None:
            new_limit = min(self.total_limit - self.fetched, self.batch_limit)
        else:
            new_limit = self.batch_limit

        self.query_filters.limit = new_limit

        meta, batch = self._request()

        if meta is not None:
            self.current_meta = meta

        self.current_batch = batch
        self.fetched += len(batch)

    def __iter__(self):
        return self

    def __next__(self):
        # This can happen if there is none returned on the first iteration
        # Check here so we don't fetch twice
        if len(self.current_batch) == 0:
            raise StopIteration

        if self.current_pos >= len(self.current_batch):
            # At the end of the current batch. Fetch the next
            self._fetch_batch()

            # If we didn't get any, then that's all there is
            if len(self.current_batch) == 0:
                raise StopIteration

        ret = self.current_batch[self.current_pos]
        self.current_pos += 1
        return ret
