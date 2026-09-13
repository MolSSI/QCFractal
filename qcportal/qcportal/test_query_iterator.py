from __future__ import annotations

from qcportal.base_models import QueryIteratorBase, QueryModelBase


class _FakeRow:
    """Minimal stand-in for a queried object - query iterators only need an id"""

    def __init__(self, id: int):
        self.id = id

    def __repr__(self) -> str:
        return f"_FakeRow({self.id})"


class _FakeIterator(QueryIteratorBase[_FakeRow]):
    """
    Query iterator over a fixed set of ids, mimicking how the server paginates

    Rows are returned in id order, starting after the cursor, up to the requested limit.
    The number of requests made is recorded so that batching can be tested.
    """

    def __init__(self, all_ids: list[int], batch_limit: int, limit: int | None = None):
        self._all_rows = [_FakeRow(i) for i in all_ids]
        self.n_requests = 0

        query_filters = QueryModelBase(limit=limit)
        QueryIteratorBase.__init__(self, None, query_filters, batch_limit)

    def _request(self) -> list[_FakeRow]:
        self.n_requests += 1

        cursor = self._query_filters.cursor
        limit = self._query_filters.limit

        rows = [r for r in self._all_rows if cursor is None or r.id > cursor]
        return rows[:limit]


def test_query_iterator_single_batch():
    it = _FakeIterator([1, 2, 3], batch_limit=10)
    assert [r.id for r in it] == [1, 2, 3]


def test_query_iterator_multiple_batches():
    it = _FakeIterator([1, 2, 3, 4, 5], batch_limit=2)
    assert [r.id for r in it] == [1, 2, 3, 4, 5]

    # 2 + 2 + 1, then one more that comes back empty
    assert it.n_requests > 1


def test_query_iterator_empty():
    it = _FakeIterator([], batch_limit=2)
    assert [r.id for r in it] == []


def test_query_iterator_limit():
    it = _FakeIterator([1, 2, 3, 4, 5], batch_limit=2, limit=3)
    assert [r.id for r in it] == [1, 2, 3]


def test_query_iterator_reset():
    # Reset must clear the pagination cursor, otherwise iterating again picks up
    # where the previous pass left off (that is, returns nothing)
    it = _FakeIterator([1, 2, 3, 4, 5], batch_limit=2)
    assert [r.id for r in it] == [1, 2, 3, 4, 5]

    it.reset()
    assert [r.id for r in it] == [1, 2, 3, 4, 5]

    # Also works when resetting part-way through
    it.reset()
    assert next(it).id == 1
    assert next(it).id == 2
    it.reset()
    assert [r.id for r in it] == [1, 2, 3, 4, 5]


def test_query_iterator_reset_with_limit():
    # The user-specified limit applies again from the start after a reset
    it = _FakeIterator([1, 2, 3, 4, 5], batch_limit=2, limit=3)
    assert [r.id for r in it] == [1, 2, 3]

    it.reset()
    assert [r.id for r in it] == [1, 2, 3]
