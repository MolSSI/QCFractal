from qcportal.utils import chunk_iterable


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
