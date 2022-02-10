"""
Tests the keywords subsocket
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal import PortalRequestError
from qcportal.keywords import KeywordSet

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_keywords_client_basic(snowflake_client: PortalClient):
    kw1 = KeywordSet(values={"o": 5})
    kw2 = KeywordSet(values={"o": 6})
    kw3 = KeywordSet(values={"o": 7})

    meta, added_ids = snowflake_client.add_keywords([kw1, kw2, kw3])
    assert len(added_ids) == 3
    assert meta.n_inserted == 3
    assert meta.inserted_idx == [0, 1, 2]

    ret = snowflake_client.get_keywords(added_ids)
    assert ret[0].id == added_ids[0]
    assert ret[1].id == added_ids[1]
    assert ret[2].id == added_ids[2]

    # Should return a single KeywordSet
    ret = snowflake_client.get_keywords(added_ids[0])
    assert isinstance(ret, KeywordSet)
    assert ret.id == added_ids[0]

    # Delete two
    meta = snowflake_client.delete_keywords(added_ids[:2])
    assert meta.success
    assert meta.n_deleted == 2
    assert meta.deleted_idx == [0, 1]

    # Deleted keywords are no longer available
    ret = snowflake_client.get_keywords(added_ids[:3], missing_ok=True)
    assert ret[0] is None
    assert ret[1] is None
    assert ret[2].id == added_ids[2]


def test_keywords_client_add_duplicate(snowflake_client: PortalClient):
    # kw1 == kw2 == kw3 == kw5 == kw7
    # kw4 and kw6 are unique
    kw1 = KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw2 = KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw3 = KeywordSet(values={"o": 5, "f": 1.111111111121111}, exact_floats=False)
    kw4 = KeywordSet(values={"o": 5, "f": 1.111111111121111}, exact_floats=True)
    kw5 = KeywordSet(values={"O": 5, "f": 1.111111111121111})
    kw6 = KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)
    kw7 = KeywordSet(values={"O": 5, "f": 1.111111111121111})

    meta, ret = snowflake_client.add_keywords([kw1, kw2, kw3, kw4, kw5, kw6, kw7])
    assert meta.success
    assert meta.n_inserted == 3
    assert meta.existing_idx == [1, 2, 4, 6]
    assert meta.inserted_idx == [0, 3, 5]
    assert ret[0] == ret[1]
    assert ret[0] == ret[2]
    assert ret[0] == ret[4]
    assert ret[0] == ret[6]

    # add again in a different order, and with an extra
    kw8 = KeywordSet(values={"q": 5, "f": 1.111111111111111})
    meta, ret2 = snowflake_client.add_keywords([kw7, kw6, kw5, kw8, kw4, kw3, kw2, kw1])
    assert meta.n_inserted == 1
    assert meta.n_existing == 7
    assert ret2[0] == ret[6]
    assert ret2[1] == ret[5]
    assert ret2[2] == ret[4]
    assert ret2[4] == ret[3]
    assert ret2[5] == ret[2]
    assert ret2[6] == ret[1]
    assert ret2[7] == ret[0]


def test_keywords_client_delete_nonexist(snowflake_client: PortalClient):
    kw1 = KeywordSet(values={"o": 5, "f": 1.111111111111111})
    meta, ids = snowflake_client.add_keywords([kw1])
    assert meta.n_inserted == 1

    meta = snowflake_client.delete_keywords([456, ids[0], ids[0], 123, 789])
    assert meta.success is False
    assert meta.n_deleted == 1
    assert meta.n_errors == 4
    assert meta.error_idx == [0, 2, 3, 4]
    assert meta.deleted_idx == [1]


def test_keywords_client_get_nonexist(snowflake_client: PortalClient):
    kw1 = KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw6 = KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)
    meta, ids = snowflake_client.add_keywords([kw1, kw6])
    assert meta.n_inserted == 2

    meta = snowflake_client.delete_keywords([ids[0]])
    assert meta.success
    assert meta.n_deleted == 1

    # We now have one keyword set in the database and one that has been deleted
    # Try to get both with missing_ok = True. This should have None in the returned list
    kw = snowflake_client.get_keywords([ids[0], ids[1], ids[1], ids[0]], missing_ok=True)
    assert len(kw) == 4
    assert kw[0] is None
    assert kw[3] is None
    assert kw[1].id == ids[1]
    assert kw[2].id == ids[1]

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        snowflake_client.get_keywords([ids[0], ids[1], ids[1], ids[0]], missing_ok=False)


def test_keywords_client_get_empty(snowflake_client: PortalClient):
    kw1 = KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw6 = KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)
    meta, ids = snowflake_client.add_keywords([kw1, kw6])
    assert meta.n_inserted == 2

    assert snowflake_client.get_keywords([]) == []
