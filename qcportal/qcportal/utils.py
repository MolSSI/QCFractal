from __future__ import annotations

from typing import Optional, Union, Sequence, List, TypeVar, Any, Dict

import numpy as np

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


def recursive_normalizer(value: Any, **kwargs: Dict[str, Any]) -> Any:
    """
    Prepare a structure for hashing by lowercasing all values and round all floats
    """
    digits = kwargs.get("digits", 10)
    lowercase = kwargs.get("lowercase", True)

    if isinstance(value, (int, type(None))):
        pass

    elif isinstance(value, str):
        if lowercase:
            value = value.lower()

    elif isinstance(value, list):
        value = [recursive_normalizer(x, **kwargs) for x in value]

    elif isinstance(value, tuple):
        value = tuple(recursive_normalizer(x, **kwargs) for x in value)

    elif isinstance(value, dict):
        ret = {}
        for k, v in value.items():
            if lowercase:
                k = k.lower()
            ret[k] = recursive_normalizer(v, **kwargs)
        value = ret

    elif isinstance(value, np.ndarray):
        if digits:
            # Round array
            value = np.around(value, digits)
            # Flip zeros
            value[np.abs(value) < 5 ** (-(digits + 1))] = 0

    elif isinstance(value, float):
        if digits:
            value = round(value, digits)
            if value == -0.0:
                value = 0
            if value == 0.0:
                value = 0

    else:
        raise TypeError("Invalid type in recursive normalizer ({type(value)}), only simple Python types are allowed.")

    return value


def calculate_limit(max_limit: int, given_limit: Optional[int]) -> int:
    """Get the allowed limit on results to return for a particular or type of object

    If 'given_limit' is given (ie, by the user), this will return min(limit, max_limit)
    where max_limit is the set value for the table/type of object
    """

    if given_limit is None:
        return max_limit

    return min(given_limit, max_limit)
