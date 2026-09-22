from __future__ import annotations

import collections
import threading
import concurrent.futures
import datetime
import functools
import io
import itertools
import json
import logging
import math
import random
import re
import time
from collections.abc import Callable, Collection, Generator, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from hashlib import sha256
from typing import Any, TypeVar, overload

import numpy as np

from qcportal.serialization import _JSONEncoder

_T = TypeVar("_T")
_U = TypeVar("_U")
_M = TypeVar("_M", bound=Mapping)
_S = TypeVar("_S", bound=str)


def is_scalar(obj: Any) -> bool:
    """
    Returns True if `obj` represents a single value rather than a collection of values

    This is the rule used by `make_list`, and by functions that accept either a single value
    or a collection of values (and whose return value has a matching shape). With the
    exception of None, `is_scalar(obj)` is True exactly when `make_list(obj)` wraps `obj` in a
    new list instead of expanding it.

    Anything sized and iterable (a list, tuple, set, frozenset, range, numpy array, or a view
    such as `dict.keys()`) is a collection of values. Strings and mappings are not - they are
    iterable, but are always treated as single values. Neither are objects that are merely
    iterable without being sized, such as generators or pydantic models.

    None is a special case. It is neither a scalar nor a collection - `make_list` passes it
    through unchanged - and `is_scalar(None)` is True.
    """

    # Strings and dicts are iterable, but we always treat them as single values.
    # Note that pydantic models are iterable too, but they are not Collections (they are
    # not sized), so they fall out of the check below as scalars
    if isinstance(obj, (str, Mapping)):
        return True

    # numpy arrays are not registered as Collections, but are certainly collections of values
    return not isinstance(obj, (np.ndarray, Collection))


# NOTE: The overloads below mirror what this function actually does at runtime, and their order
#       matters. Strings and mappings are handled before the collection cases, and the catch-all
#       (anything that is not a collection gets wrapped in a list) must come last.
#       Note that there is deliberately no overload for a general Iterable - iterables that are
#       not sized (generators, map/filter objects) are wrapped, not expanded
@overload
def make_list(obj: None) -> None: ...


# Note the TypeVar - str subclasses (str-based enums, for example) must not be widened to str
@overload
def make_list(obj: _S) -> list[_S]: ...


@overload
def make_list(obj: _M) -> list[_M]: ...


@overload
def make_list(obj: AbstractSet[_T]) -> list[_T]: ...


@overload
def make_list(obj: Sequence[_T]) -> list[_T]: ...


@overload
def make_list(obj: Any) -> list[Any]: ...


def make_list(obj: Any) -> Any:
    """
    Returns a list of the values in obj, or a list containing obj if it is a single value

    See `is_scalar` for what counts as a single value. Sets, numpy arrays, and views such as
    `dict.keys()` are all expanded into a list. None is passed through unchanged.
    """

    if isinstance(obj, list):
        return obj
    if obj is None:
        return None
    if is_scalar(obj):
        return [obj]

    # tolist() also converts numpy scalar types (np.int64 and friends) to plain python types.
    # atleast_1d handles 0-d arrays, whose tolist() returns a scalar rather than a list
    if isinstance(obj, np.ndarray):
        return np.atleast_1d(obj).tolist()

    return list(obj)


def chunk_iterable(it: Iterable[_T], chunk_size: int) -> Generator[list[_T], None, None]:
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


def chunk_iterable_time(
    it: Iterable[_T], chunk_time: float, max_chunk_size: int, initial_chunk_size: int
) -> Generator[list[_T], None, None]:
    """
    Split an iterable into chunks, trying to keep a constant time per chunk

    This function keeps track of the time it takes to process each chunk and tries to keep the time per chunk
    as close to 'chunk_time' as possible, increasing or decreasing the chunk size as needed (up to 'max_chunk_size')

    The first chunk will be of size 'initial_chunk_size' (assuming there is enough elements in the iterable to fill it).
    """

    if chunk_time <= 0:
        raise ValueError("chunk_time must be > 0")
    if max_chunk_size < 1:
        raise ValueError("max_chunk_size must be >= 1")
    if initial_chunk_size < 1 or initial_chunk_size > max_chunk_size:
        raise ValueError("initial_chunk_size must be >= 1 and <= max_chunk_size")

    i = iter(it)

    batch = list(itertools.islice(i, initial_chunk_size))

    while batch:
        # Time how long it takes the caller to process the first chunk
        start = time.time()
        yield batch
        end = time.time()

        # How many elements could we fit in the desired chunk_time
        time_per_element = (end - start) / len(batch)
        chunk_size = math.floor(int(chunk_time / time_per_element))

        # Clamp to a valid size
        chunk_size = max(1, min(chunk_size, max_chunk_size))

        # Get the next chunk
        batch = list(itertools.islice(i, chunk_size))


def process_chunk_iterable(
    fn: Callable[[list[_T]], _U],
    it: Iterable[_T],
    chunk_time: float,
    max_chunk_size: int,
    initial_chunk_size: int,
    max_workers: int = 1,
    *,
    keep_order: bool = False,
) -> Generator[_U, None, None]:
    """
    Process an iterable in chunks, trying to keep a constant time per chunk

    This function keeps track of the time it takes to process each chunk and tries to keep the time per chunk
    as close to 'chunk_time' as possible, increasing or decreasing the chunk size as needed (up to 'max_chunk_size')

    The first chunk will be of size 'initial_chunk_size' (assuming there is enough elements in the iterable to fill it).

    This function yields whatever `fn` returned for each chunk. If 'keep_order' is True, the results
    will be returned in the same order as the original iterable. If 'keep_order' is False, the results will be returned
    in the order they are completed.
    """

    # NOTE: You might think that we should spin up another thread to handle all the processing and submission
    #       to the thread pool. However, if the user takes a long time processing the chunk (returned via yield) on
    #       their end then this would effectively just process all the data and hold that in the cache. This might be
    #       undesirable if the user is trying to process a large amount of data. Also, the effect is largely the same
    #       in terms of timing.
    #       So this function more or less tries to pre-process enough so that the user is never waiting, striking a
    #       balance between downloading all the data and doing things completely serially.

    if chunk_time <= 0.0:
        raise ValueError("chunk_time must be > 0.0")
    if max_chunk_size < 1:
        raise ValueError("max_chunk_size must be >= 1")
    if initial_chunk_size < 1 or initial_chunk_size > max_chunk_size:
        raise ValueError("initial_chunk_size must be >= 1 and <= max_chunk_size")
    if max_workers < 1:
        raise ValueError("max_workers must be >= 1")

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    # Get initial chunks to be submitted to the pool
    i = iter(it)
    chunks = [list(itertools.islice(i, initial_chunk_size)) for _ in range(max_workers)]

    # Remove empty chunks
    chunks = [b for b in chunks if b]

    # Wrap the provided function so that we get timing and chunk id
    def _process(chunk, chunk_id):
        start = time.time()
        ret = fn(chunk)
        end = time.time()
        return (end - start) / len(chunk), chunk_id, ret

    # chunk id we should submit next
    cur_chunk_idx = 0

    # Current chunk id we are returning (if order is kept)
    cur_ret_chunk_id = 0

    # Dictionary keeping the results (indexed by chunk id)
    results_cache = {}

    # Submit the given function with the given chunks to the thread pool
    futures = [pool.submit(_process, chunk, cur_chunk_idx + i) for i, chunk in enumerate(chunks)]
    cur_chunk_idx += len(chunks)

    while True:
        if len(futures) == 0:
            break

        # Wait for any of the futures
        done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

        # Get the result of the first completed future
        average_per_element = 0.0

        for future in done:
            avg_time, chunk_idx, ret = future.result()
            average_per_element += avg_time  # Average per element of the iterable
            assert cur_chunk_idx not in results_cache
            results_cache[chunk_idx] = ret

        if len(done) != 0:
            # compute the next chunk size
            time_per_element = average_per_element / len(done)  # Average of the averages

            # How many elements could we fit in the desired chunk_time
            chunk_size = math.floor(int(chunk_time / time_per_element))

            # Clamp to a valid size
            chunk_size = max(1, min(chunk_size, max_chunk_size))

            # next chunks
            chunks = [list(itertools.islice(i, chunk_size)) for _ in range(len(done))]

            # Remove empty chunks
            chunks = [b for b in chunks if b]

            # Submit to the thread pool
            futures = list(not_done) + [
                pool.submit(_process, chunk, cur_chunk_idx + i) for i, chunk in enumerate(chunks)
            ]
            cur_chunk_idx += len(chunks)

        done_results = list(results_cache.keys())
        if keep_order:
            while cur_ret_chunk_id in done_results:
                yield results_cache[cur_ret_chunk_id]
                del results_cache[cur_ret_chunk_id]
                cur_ret_chunk_id += 1
        else:
            for k in done_results:
                yield results_cache[k]
                del results_cache[k]

    assert len(results_cache) == 0


def process_iterable(
    fn: Callable[[list[_T]], Iterable[_U]],
    it: Iterable[_T],
    chunk_time: float,
    max_chunk_size: int,
    initial_chunk_size: int,
    max_workers: int = 1,
    *,
    keep_order: bool = False,
) -> Generator[_U, None, None]:
    """
    Similar to process_chunk_iterable, but returns individual elements rather than chunks
    """

    for chunk in process_chunk_iterable(
        fn, it, chunk_time, max_chunk_size, initial_chunk_size, max_workers, keep_order=keep_order
    ):
        yield from chunk


def seconds_to_hms(seconds: float | int) -> str:
    """
    Converts a number of seconds (as an integer) to a string representing hh:mm:ss
    """

    if isinstance(seconds, float):
        fraction = seconds % 1
        seconds = int(seconds)
    else:
        fraction = None

    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    if fraction is None:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds+fraction:02.2f}"


def duration_to_seconds(s: int | str | float) -> int:
    """
    Parses a string in dd:hh:mm:ss or 1d2h3m4s to an integer number of seconds
    """

    # Is already an int
    if isinstance(s, int):
        return s

    # Is a float but represents an integer
    if isinstance(s, float):
        if s.is_integer():
            return int(s)
        else:
            raise ValueError(f"Invalid duration format: {s} - cannot represent fractional seconds")

    # Plain number of seconds (as a string)
    if s.isdigit():
        return int(s)

    try:
        f = float(s)
        if f.is_integer():
            return int(f)
        else:
            raise ValueError(f"Invalid duration format: {s} - cannot represent fractional seconds")
    except ValueError:
        pass

    # Handle dd:hh:mm:ss format
    if ":" in s:
        parts = list(map(int, s.split(":")))
        while len(parts) < 4:  # Pad missing parts with zeros
            parts.insert(0, 0)
        days, hours, minutes, seconds = parts
        return days * 86400 + hours * 3600 + minutes * 60 + seconds

    # Handle format like 3d4h7m10s
    pattern = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")
    match = pattern.fullmatch(s)
    if not match:
        raise ValueError(f"Invalid duration format: {s}")

    days, hours, minutes, seconds = map(lambda x: int(x) if x else 0, match.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


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


def calculate_limit(max_limit: int, given_limit: int | None) -> int:
    """Get the allowed limit on results to return for a particular or type of object

    If 'given_limit' is given (ie, by the user), this will return min(limit, max_limit)
    where max_limit is the set value for the table/type of object
    """

    if given_limit is None:
        return max_limit

    return min(given_limit, max_limit)


def hash_dict(d: dict[str, Any]) -> str:
    j = json.dumps(d, ensure_ascii=True, sort_keys=True, cls=_JSONEncoder).encode("utf-8")
    return sha256(j).hexdigest()


def reshape_molecule(a: list) -> list[list[float]]:
    """
    Converts a flattened list with length N to a nested list of dimensions (N,3)
    """

    if len(a) % 3 != 0:
        raise ValueError(f"Length of input list must be divisible by 3, got {len(a)}")

    return [a[i : i + 3] for i in range(0, len(a), 3)]


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


def now_at_utc() -> datetime.datetime:
    """Get the current time as a timezone-aware datetime object"""

    # Note that the utcnow() function is deprecated, and does not result in a
    # timezone-aware datetime object
    return datetime.datetime.now(datetime.timezone.utc)


@functools.lru_cache
def _is_included(key: str, include: tuple[str, ...] | None, exclude: tuple[str, ...] | None, default: bool) -> bool:
    if exclude is None:
        exclude = ()

    if include is not None:
        in_include = ("*" in include and default) or "**" in include or key in include
    else:
        in_include = default

    in_exclude = key in exclude

    return in_include and not in_exclude


def is_included(key: str, include: Iterable[str] | None, exclude: Iterable[str] | None, default: bool) -> bool:
    """
    Determine if a field should be included given the include and exclude lists

    Handles "*" and "**" as well
    """

    if include is not None:
        include = tuple(sorted(include))
    if exclude is not None:
        exclude = tuple(sorted(exclude))

    return _is_included(key, include, exclude, default)


def update_nested_dict(d: dict[str, Any], u: dict[str, Any]):
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = update_nested_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def apply_jitter(t: int | float, jitter_fraction: float) -> float:
    f = random.uniform(-jitter_fraction, jitter_fraction)
    return max(t * (1 + f), 0.0)


def time_based_cache(seconds: int = 10, maxsize: int | None = None):
    def decorator(func):
        cache = collections.OrderedDict()

        # The cache may be shared across threads (e.g. multiple waitress worker threads), so all
        # access to it must be serialized - otherwise the cleanup below can iterate the dict while
        # another thread mutates it (RuntimeError: dictionary changed size during iteration)
        lock = threading.Lock()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, frozenset(kwargs.items()))
            # Use a monotonic clock: this measures elapsed time for expiry, so it must not be
            # affected by wall-clock adjustments (NTP steps, manual changes). A backward wall-clock
            # step would otherwise keep stale entries (e.g. a revoked token) alive past the TTL.
            now = time.monotonic()

            with lock:
                # Clean up old items
                expiration_time = now - seconds
                keys_to_delete = [k for k, (timestamp, _) in cache.items() if timestamp < expiration_time]
                for k in keys_to_delete:
                    del cache[k]

                # Return from cache if valid
                if key in cache:
                    return cache[key][1]

            # Compute outside the lock - func may be slow (e.g. a database query), and holding the
            # lock across it would serialize all callers. A concurrent duplicate computation is
            # harmless (last writer wins)
            result = func(*args, **kwargs)

            with lock:
                cache[key] = (now, result)

                # Enforce max size
                if len(cache) > maxsize:
                    cache.popitem(last=False)  # Remove oldest

            return result

        def cache_clear():
            with lock:
                cache.clear()

        wrapper.cache_clear = cache_clear
        return wrapper

    return decorator
