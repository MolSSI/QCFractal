"""
Tests the keywords subsocket
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.exceptions import MissingDataError

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_keywords_socket_basic(storage_socket: SQLAlchemySocket):
    kw1 = {"o": 5}
    kw2 = {"o": 6}
    kw3 = {"o": 7}

    meta, added_ids = storage_socket.keywords.add([kw1, kw2, kw3])
    assert len(added_ids) == 3
    assert meta.n_inserted == 3
    assert meta.inserted_idx == [0, 1, 2]


def test_keywords_socket_add_duplicate(storage_socket: SQLAlchemySocket):

    # kw1 == kw2 == kw3 == kw5 == kw7
    # kw4 and kw6 are unique
    kw1 = {"o": 5, "f": 1.111111111111111}
    kw2 = {"o": 5, "f": 1.111111111111111}
    kw3 = {"o": 5, "f": 1.111111111121111, "t": "hi"}
    kw4 = {"O": 5, "f": 1.111111111121111, "T": "hi"}
    kw5 = {"O": 5, "f": 1.111111111121111, "t": "HI"}

    meta, ret = storage_socket.keywords.add([kw1, kw2, kw3, kw4, kw5])
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.existing_idx == [1, 3, 4]
    assert meta.inserted_idx == [0, 2]
    assert ret[0] == ret[1]
    assert ret[2] == ret[3]
    assert ret[2] == ret[4]
    assert ret[0] != ret[2]

    # add again in a different order
    meta, ret2 = storage_socket.keywords.add(list(reversed([kw1, kw2, kw3, kw4, kw5])))
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert ret2 == list(reversed(ret))

    # add again in a different order, and with an extra
    kw8 = {"[": 5, "f": 1.111111111111111}
    meta, ret2 = storage_socket.keywords.add([kw5, kw8, kw4, kw3, kw2, kw1])
    assert meta.n_inserted == 1
    assert meta.n_existing == 5
