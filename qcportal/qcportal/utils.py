from __future__ import annotations

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
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from hashlib import sha256
from typing import Optional, Union, Sequence, List, TypeVar, Any, Dict, Generator, Iterable, Callable, Set, Tuple

import numpy as np

from qcportal.serialization import _JSONEncoder

_T = TypeVar("_T")


def make_list(obj: Optional[Union[_T, Sequence[_T], Set[_T]]]) -> Optional[List[_T]]:
    """
    Returns a list containing obj if obj is not a list or other iterable type object

    This will also work with sets
    """

    # NOTE - you might be tempted to change this to work with Iterable rather than Sequence. However,
    # pydantic models and dicts and stuff are sequences, too, which we usually just want to return
    # within a list

    if isinstance(obj, list):
        return obj
    if obj is None:
        return None
    # Be careful. strings are sequences
    if isinstance(obj, str):
        return [obj]
    if isinstance(obj, set):
        return list(obj)
    if not isinstance(obj, Sequence):
        return [obj]
    return list(obj)


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


def chunk_iterable_time(
    it: Iterable[_T], chunk_time: float, max_chunk_size: int, initial_chunk_size: int
) -> Generator[List[_T], None, None]:
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
    fn: Callable[[Iterable[_T]], Any],
    it: Iterable[_T],
    chunk_time: float,
    max_chunk_size: int,
    initial_chunk_size: int,
    max_workers: int = 1,
    *,
    keep_order: bool = False,
) -> Generator[List[_T], None, None]:
    """
    Process an iterable in chunks, trying to keep a constant time per chunk

    This function keeps track of the time it takes to process each chunk and tries to keep the time per chunk
    as close to 'chunk_time' as possible, increasing or decreasing the chunk size as needed (up to 'max_chunk_size')

    The first chunk will be of size 'initial_chunk_size' (assuming there is enough elements in the iterable to fill it).

    This function returns the results as chunks (lists) of the original iterable. If 'keep_order' is True, the results
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
    fn: Callable[[Iterable[_T]], Any],
    it: Iterable[_T],
    chunk_time: float,
    max_chunk_size: int,
    initial_chunk_size: int,
    max_workers: int = 1,
    *,
    keep_order: bool = False,
) -> Generator[List[_T], None, None]:
    """
    Similar to process_chunk_iterable, but returns individual elements ranther than chunks
    """

    for chunk in process_chunk_iterable(
        fn, it, chunk_time, max_chunk_size, initial_chunk_size, max_workers, keep_order=keep_order
    ):
        yield from chunk


def seconds_to_hms(seconds: Union[float, int]) -> str:
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


def duration_to_seconds(s: Union[int, str, float]) -> int:
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


def now_at_utc() -> datetime.datetime:
    """Get the current time as a timezone-aware datetime object"""

    # Note that the utcnow() function is deprecated, and does not result in a
    # timezone-aware datetime object
    return datetime.datetime.now(datetime.timezone.utc)


@functools.lru_cache
def _is_included(
    key: str, include: Optional[Tuple[str, ...]], exclude: Optional[Tuple[str, ...]], default: bool
) -> bool:
    if exclude is None:
        exclude = []

    if include is not None:
        in_include = ("*" in include and default) or "**" in include or key in include
    else:
        in_include = default

    in_exclude = key in exclude

    return in_include and not in_exclude


def is_included(key: str, include: Optional[Iterable[str]], exclude: Optional[Iterable[str]], default: bool) -> bool:
    """
    Determine if a field should be included given the include and exclude lists

    Handles "*" and "**" as well
    """

    if include is not None:
        include = tuple(sorted(include))
    if exclude is not None:
        exclude = tuple(sorted(exclude))

    return _is_included(key, include, exclude, default)


def update_nested_dict(d: Dict[str, Any], u: Dict[str, Any]):
    for k, v in u.items():
        if isinstance(v, dict):
            d[k] = update_nested_dict(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def apply_jitter(t: Union[int, float], jitter_fraction: float) -> float:
    f = random.uniform(-jitter_fraction, jitter_fraction)
    return max(t * (1 + f), 0.0)
