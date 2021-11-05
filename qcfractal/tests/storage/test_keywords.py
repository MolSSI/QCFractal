"""
Tests the keywords subsocket
"""

import pytest
import qcfractal.interface as ptl


def test_keywords_basic(storage_socket):
    kw1 = ptl.models.KeywordSet(values={"o": 5})
    kw2 = ptl.models.KeywordSet(values={"o": 6})
    kw3 = ptl.models.KeywordSet(values={"o": 7})

    meta, added_ids = storage_socket.keywords.add([kw1, kw2, kw3])
    assert len(added_ids) == 3
    assert meta.n_inserted == 3
    assert meta.inserted_idx == [0, 1, 2]

    ret = storage_socket.keywords.get(added_ids)
    assert "hash_index" in ret[0]
    assert "hash_index" in ret[1]
    assert "hash_index" in ret[2]
    assert ret[0]["id"] == added_ids[0]
    assert ret[1]["id"] == added_ids[1]
    assert ret[2]["id"] == added_ids[2]

    # Should be able to create objects
    kw = [ptl.models.KeywordSet(**x) for x in ret]
    assert kw[0].hash_index == ret[0]["hash_index"]
    assert kw[1].hash_index == ret[1]["hash_index"]
    assert kw[2].hash_index == ret[2]["hash_index"]
    assert kw[0].id == added_ids[0]
    assert kw[1].id == added_ids[1]
    assert kw[2].id == added_ids[2]

    assert kw[0].values == kw1.values
    assert kw[1].values == kw2.values
    assert kw[2].values == kw3.values

    # Delete one
    meta = storage_socket.keywords.delete([added_ids[0]])
    assert meta.success
    assert meta.n_deleted == 1


def test_keywords_add_duplicate(storage_socket):

    # kw1 == kw2 == kw3 == kw5 == kw7
    # kw4 and kw6 are unique
    kw1 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw2 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw3 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111121111}, exact_floats=False)
    kw4 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111121111}, exact_floats=True)
    kw5 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111})
    kw6 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)
    kw7 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111})

    meta, ret = storage_socket.keywords.add([kw1, kw2, kw3, kw4, kw5, kw6, kw7])
    assert meta.success
    assert meta.n_inserted == 3
    assert meta.existing_idx == [1, 2, 4, 6]
    assert meta.inserted_idx == [0, 3, 5]
    assert ret[0] == ret[1]
    assert ret[0] == ret[2]
    assert ret[0] == ret[4]
    assert ret[0] == ret[6]

    # add again in a different order
    meta, ret2 = storage_socket.keywords.add(list(reversed([kw1, kw2, kw3, kw4, kw5, kw6, kw7])))
    assert meta.n_inserted == 0
    assert meta.n_existing == 7
    assert ret2 == list(reversed(ret))


def test_keywords_add_mixed_1(storage_socket):

    # kw1 == kw7
    # kw4 is unique
    kw1 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw4 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111121111}, exact_floats=True)
    kw7 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111})

    meta, ret = storage_socket.keywords.add_mixed([kw1])
    assert meta.success
    assert meta.n_inserted == 1

    # Add using the id as well as the KeywordSet
    meta, ret = storage_socket.keywords.add_mixed([ret[0], kw4, kw1, kw7, ret[0]])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [1]  # kw4 is unique. All others are duplicates
    assert meta.existing_idx == [0, 2, 3, 4]


def test_keywords_add_mixed_bad(storage_socket):
    kw1 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw6 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)

    meta, ret = storage_socket.keywords.add_mixed([kw1])
    assert meta.success
    assert meta.n_inserted == 1

    # Add using a bad id
    meta, ret = storage_socket.keywords.add_mixed([kw1, 12345, kw6])
    assert meta.success is False
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [2]
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert meta.n_errors == 1
    assert meta.error_idx == [1]
    assert "KeywordsORM object with id=12345 was not found" in meta.errors[0][1]


def test_keywords_delete_nonexist(storage_socket):
    kw1 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    meta, ids = storage_socket.keywords.add([kw1])
    assert meta.n_inserted == 1

    meta = storage_socket.keywords.delete([456, ids[0], ids[0], 123, 789])
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_missing == 4
    assert meta.missing_idx == [0, 2, 3, 4]
    assert meta.deleted_idx == [1]


def test_keywords_get_nonexist(storage_socket):
    kw1 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw6 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)
    meta, ids = storage_socket.keywords.add([kw1, kw6])
    assert meta.n_inserted == 2

    meta = storage_socket.keywords.delete([ids[0]])
    assert meta.success
    assert meta.n_deleted == 1

    # We now have one keyword set in the database and one that has been deleted
    # Try to get both with missing_ok = True. This should have None in the returned list
    kw = storage_socket.keywords.get([ids[0], ids[1], ids[1], ids[0]], missing_ok=True)
    assert len(kw) == 4
    assert kw[0] is None
    assert kw[3] is None

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(RuntimeError, match=r"Could not find all requested keywords records"):
        storage_socket.keywords.get([ids[0], ids[1], ids[1], ids[0]], missing_ok=False)


def test_keywords_get_empty(storage_socket):
    kw1 = ptl.models.KeywordSet(values={"o": 5, "f": 1.111111111111111})
    kw6 = ptl.models.KeywordSet(values={"O": 5, "f": 1.111111111121111}, lowercase=False)
    meta, ids = storage_socket.keywords.add([kw1, kw6])
    assert meta.n_inserted == 2

    assert storage_socket.keywords.get([]) == []
