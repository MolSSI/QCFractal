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


class CommonDeleteURLParameters(RestModelBase):
    """
    Common URL parameters for delete functionality

    These functions typically only take a list of ids
    """

    id: Optional[List[int]] = None


class CommonGetURLParameters(RestModelBase):
    """
    Common URL parameters for get_ functions

    These functions typically take a list for ids, and a bool for missing_ok
    """

    id: Optional[List[int]] = None
    missing_ok: bool = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


class ProjURLParameters(RestModelBase):
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class CommonGetProjURLParameters(CommonGetURLParameters):
    """
    Common URL parameters for get_ functions, with projection
    """

    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None


class CommonGetURLParametersName(RestModelBase):
    """
    Common URL parameters for get_ functions

    This version is for get_ with string arguments (as opposed to int ids)
    """

    name: Optional[List[str]] = None
    missing_ok: bool = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)


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


class QueryProjModelBase(QueryModelBase):
    """
    Common parameters for query_* functions, with include/exclude (projection)

    These can be either URL parameters or part of a POST body
    """

    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
