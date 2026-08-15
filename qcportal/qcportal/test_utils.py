import numpy as np
import pytest

from qcportal.record_models import RecordQueryFilters, RecordStatusEnum
from qcportal.utils import chunk_iterable, seconds_to_hms, duration_to_seconds, is_included, is_scalar, make_list

# Things that represent a single value, even if some of them are iterable
# (RecordStatusEnum is a str-based enum, so it is a single value the same way a str is)
scalar_objects = [7, np.int64(7), "abc", "", RecordStatusEnum.complete, {"a": 1}, {}, RecordQueryFilters()]

# Things that represent multiple values
collection_objects = [
    [1, 2],
    [],
    (1, 2),
    (),
    {1, 2},
    frozenset([1, 2]),
    ["abc"],
    range(3),
    {1: "a", 2: "b"}.keys(),
    {1: "a", 2: "b"}.values(),
    np.array([1, 2, 3]),
    np.array(5),  # 0-d array - a scalar as far as numpy is concerned, but still expanded
]


@pytest.mark.parametrize("obj", scalar_objects)
def test_is_scalar_true(obj):
    assert is_scalar(obj) is True


@pytest.mark.parametrize("obj", collection_objects)
def test_is_scalar_false(obj):
    assert is_scalar(obj) is False


@pytest.mark.parametrize("obj", scalar_objects + collection_objects)
def test_is_scalar_matches_make_list(obj):
    # is_scalar and make_list must always agree about what counts as a single value.
    # If they disagree, functions taking "one or many" silently return the wrong shape
    made = make_list(obj)

    if is_scalar(obj):
        assert made == [obj]
    else:
        assert made == list(np.atleast_1d(obj) if isinstance(obj, np.ndarray) else obj)


def test_make_list_numpy():
    # Arrays are expanded, and elements come back as plain python types (not numpy scalars)
    made = make_list(np.array([1, 2, 3]))
    assert made == [1, 2, 3]
    assert all(type(x) is int for x in made)

    # 0-d arrays hold a single value, but the return must still be a list
    assert make_list(np.array(5)) == [5]

    # Higher dimensions are kept as nested lists
    assert make_list(np.array([[1, 2], [3, 4]])) == [[1, 2], [3, 4]]

    # A numpy scalar is a single value
    assert make_list(np.int64(5)) == [5]


def test_make_list_dict_views():
    d = {1: "a", 2: "b"}

    assert make_list(d.keys()) == [1, 2]
    assert make_list(d.values()) == ["a", "b"]
    assert make_list(d.items()) == [(1, "a"), (2, "b")]

    # ... but the dict itself is a single value
    assert make_list(d) == [d]


def test_make_list_generator():
    # Generators are iterable but not sized, and are currently wrapped rather than expanded.
    # If that changes, is_scalar must change with it (see test_is_scalar_matches_make_list)
    gen = (x for x in [1, 2])
    assert make_list(gen) == [gen]


def test_chunk_iterable():
    # A list
    a = list(range(10))
    chunks = list(chunk_iterable(a, 3))
    assert chunks == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    # iterable without slicing
    chunks = list(chunk_iterable(range(12), 5))
    assert chunks == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11]]

    # chunk_size > len(iterable)
    chunks = list(chunk_iterable(range(12), 15))
    assert chunks == [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]


def test_seconds_to_hms():
    assert seconds_to_hms(0) == "00:00:00"
    assert seconds_to_hms(1) == "00:00:01"
    assert seconds_to_hms(60) == "00:01:00"
    assert seconds_to_hms(3600) == "01:00:00"
    assert seconds_to_hms(3601) == "01:00:01"
    assert seconds_to_hms(3600 * 2 + 50) == "02:00:50"
    assert seconds_to_hms(3600 * 25 + 9) == "25:00:09"

    assert seconds_to_hms(31.0) == "00:00:31.00"
    assert seconds_to_hms(3670.12) == "01:01:10.12"


def test_duration_to_seconds():
    assert duration_to_seconds(0) == 0
    assert duration_to_seconds("0") == 0
    assert duration_to_seconds(17) == 17
    assert duration_to_seconds("17") == 17
    assert duration_to_seconds(17.0) == 17
    assert duration_to_seconds("17.0") == 17

    assert duration_to_seconds("17s") == 17
    assert duration_to_seconds("70s") == 70
    assert duration_to_seconds("8m17s") == 497
    assert duration_to_seconds("80m72s") == 4872
    assert duration_to_seconds("3h8m17s") == 11297
    assert duration_to_seconds("03h08m07s") == 11287
    assert duration_to_seconds("03h08m070s") == 11350
    assert duration_to_seconds("9d03h08m070s") == 788950

    assert duration_to_seconds("9d") == 777600
    assert duration_to_seconds("10m") == 600
    assert duration_to_seconds("90m") == 5400
    assert duration_to_seconds("04h") == 14400
    assert duration_to_seconds("4h5s") == 14405
    assert duration_to_seconds("1d9s") == 86409

    assert duration_to_seconds("8:17") == 497
    assert duration_to_seconds("80:72") == 4872
    assert duration_to_seconds("3:8:17") == 11297
    assert duration_to_seconds("03:08:07") == 11287
    assert duration_to_seconds("03:08:070") == 11350
    assert duration_to_seconds("9:03:08:07") == 788887


def test_is_included():
    assert is_included("test", None, None, True) is True
    assert is_included("test", None, None, False) is False
    assert is_included("test", None, [], True) is True
    assert is_included("test", None, [], False) is False

    assert is_included("test", [], [], True) is False
    assert is_included("test", [], [], False) is False

    for d in (True, False):
        assert is_included("test", ["test"], None, d) is True
        assert is_included("test", ["test"], [], d) is True
        assert is_included("test", ["**"], None, d) is True
        assert is_included("test", ["**"], [], d) is True
        assert is_included("test", ["*"], None, d) is d
        assert is_included("test", ["*"], [], d) is d

    for d in (True, False):
        assert is_included("test", [], None, d) is False
        assert is_included("test", ["test2"], None, d) is False

    # Being in exclude overrides all
    for d in (True, False):
        assert is_included("test", None, ["test"], d) is False
        assert is_included("test", [], ["test"], d) is False
        assert is_included("test", ["*"], ["test"], d) is False
        assert is_included("test", ["**"], ["test"], d) is False
        assert is_included("test", ["test"], ["test"], d) is False
        assert is_included("test", ["test2"], ["test"], d) is False
