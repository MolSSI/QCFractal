from typing import Optional, List

from pydantic import BaseModel, validator, Extra


def validate_list_to_single(v):
    """
    Converts a list to a single value (the last element of the list)

    Query parameters can be specified multiple times. Therefore, we will always
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
    Common parameters for query_* functions, with out include/exclude

    These can be either URL parameters or part of a POST body
    """

    limit: Optional[int] = None
    skip: int = 0

    @validator("limit", "skip", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class QueryProjModelBase(QueryModelBase, ProjURLParameters):
    """
    Common parameters for query_* functions, with include/exclude (projection)

    These can be either URL parameters or part of a POST body
    """

    pass
