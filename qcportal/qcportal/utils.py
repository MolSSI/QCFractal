from __future__ import annotations

import io
import itertools
import json
import logging
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from hashlib import sha256
from typing import Optional, Union, Sequence, List, TypeVar, Any, Dict, Generator, Iterable

import numpy as np

from qcportal.serialization import _JSONEncoder

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


def chunk_iterable(it: Iterable[_T], chunk_size: int) -> Generator[List[_T], None, None]:
    """
    Split an iterable (such as a list) into batches/chunks
    """

    if chunk_size < 1:
        raise ValueError("chunk size must be >= 1")
    i = iter(it)

    batch = list(itertools.islice(i, chunk_size))
    while batch:
        yield batch
        batch = list(itertools.islice(i, chunk_size))


def seconds_to_hms(seconds: int) -> str:
    """
    Converts a number of seconds (as an integer) to a string representing hh:mm:ss
    """

    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def recursive_normalizer(value: Any, digits: int = 10, lowercase: bool = True) -> Any:
    """
    Prepare a structure for hashing by lowercasing all values and round all floats
    """

    if isinstance(value, (int, type(None))):
        pass

    elif isinstance(value, str):
        if lowercase:
            value = value.lower()

    elif isinstance(value, list):
        value = [recursive_normalizer(x, digits, lowercase) for x in value]

    elif isinstance(value, tuple):
        value = tuple(recursive_normalizer(x, digits, lowercase) for x in value)

    elif isinstance(value, dict):
        ret = {}
        for k, v in value.items():
            if lowercase:
                k = k.lower()
            ret[k] = recursive_normalizer(v, digits, lowercase)
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


def hash_dict(d: Dict[str, Any]) -> str:
    j = json.dumps(d, ensure_ascii=True, sort_keys=True, cls=_JSONEncoder).encode("utf-8")
    return sha256(j).hexdigest()


@contextmanager
def capture_all_output(top_logger: str):
    """Captures all output, including stdout, stderr, and logging"""

    stdout_io = io.StringIO()
    stderr_io = io.StringIO()

    logger = logging.getLogger(top_logger)
    old_handlers = logger.handlers.copy()
    old_prop = logger.propagate

    logger.handlers.clear()
    logger.propagate = False

    # Make logging go to the string io
    handler = logging.StreamHandler(stdout_io)
    handler.terminator = ""
    logger.addHandler(handler)

    # Also redirect stdout/stderr to the string io objects
    with redirect_stdout(stdout_io) as rdout, redirect_stderr(stderr_io) as rderr:
        yield rdout, rderr

        logger.handlers.clear()
        logger.handlers = old_handlers
        logger.propagate = old_prop
