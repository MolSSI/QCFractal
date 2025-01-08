from qcportal.utils import chunk_iterable, seconds_to_hms, duration_to_seconds, is_included


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
