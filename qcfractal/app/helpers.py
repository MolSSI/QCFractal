from typing import List, Optional, Callable, Iterable

from qcportal.metadata_models import DeleteMetadata


def get_helper(
    id: Optional[int],
    id_args: Optional[List[int]],
    include: Optional[Iterable[str]],
    exclude: Optional[Iterable[str]],
    missing_ok: bool,
    func: Callable,
):
    """
    A general helper for calling a get_* function of a component

    All these functions share the same signature and have the same behavior, so we can
    handle that in a common function.

    The main point of this function is to handle the two different ways ids can be passed in
    (either as part of the url (/record/1234) or as query parameters (/record?id=123&id=456)
    """

    # If an empty list was specified in the query params, it won't be sent
    # and the id member of the args will be None
    if id is None and id_args is None:
        return []

    # Don't pass include/exclude if they aren't specified. Not all
    # get_* functions support projection
    kwargs = {}
    if include is not None:
        kwargs["include"] = include
    if exclude is not None:
        kwargs["exclude"] = exclude

    # If an id was specified in the url (keyword/1234) then use that
    # Otherwise, grab from the query parameters
    if id is not None:
        return func([id], missing_ok=missing_ok, **kwargs)[0]
    else:
        return func(id_args, missing_ok=missing_ok, **kwargs)


def delete_helper(id: Optional[int], id_args: Optional[List[int]], func: Callable, **kwargs) -> DeleteMetadata:
    """
    A general helper for calling a delete_* function of a component

    All these functions share the same signature and have the same behavior, so we can
    handle that in a common function
    """

    # If an id was specified in the url (keyword/1234) then use that
    # Otherwise, grab from the query parameters
    if id is not None:
        return func([id])
    else:
        return func(id_args, **kwargs)
