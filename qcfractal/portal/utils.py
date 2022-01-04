from typing import Optional, Union, Sequence, List, TypeVar

_T = TypeVar("_T")


def make_list(obj: Optional[Union[_T, Sequence[_T]]]) -> Optional[List[_T]]:
    """
    Returns a list containing obj if obj is not a list or sequence type object
    """

    if obj is None:
        return None
    # Be careful. strings are sequences
    if isinstance(obj, str):
        return [obj]
    if not isinstance(obj, Sequence):
        return [obj]
    return list(obj)


def make_str(obj: Optional[Union[_T, Sequence[_T]]]) -> Optional[List[_T]]:
    """
    Returns a list containing obj if obj is not a list or sequence type object
    """

    if obj is None:
        return None
    # Be careful. strings are sequences
    if isinstance(obj, str):
        return obj
    if not isinstance(obj, Sequence):
        return str(obj)
    if isinstance(obj, list):
        return [str(i) for i in obj]
    if isinstance(obj, tuple):
        return tuple(str(i) for i in obj)
    else:
        raise ValueError("`obj` must be `None`, a str, list, tuple, or non-sequence")
