"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

from datetime import datetime
from time import time

import numpy as np
import pytest
import sqlalchemy
import sqlalchemy.exc

import qcfractal.interface as ptl

bad_id1 = "99999000"
bad_id2 = "99999001"


def test_storage_repr(storage_socket):

    assert isinstance(repr(storage_socket), str)


def test_collections_add(storage_socket):

    collection = "TorsionDriveRecord"
    name = "Torsion123"
    db = {
        "collection": collection,
        "name": name,
        "something": "else",
        "array": ["54321"],
        "visibility": True,
        "view_available": False,
        "group": "default",
    }

    ret = storage_socket.collection.add(db)

    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.collection.get(collection, name)

    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_found"] == 1
    assert db["something"] == ret["data"][0]["something"]

    ret = storage_socket.collection.delete(collection, name)
    assert ret == 1

    ret = storage_socket.collection.get(collection, "bleh")
    # assert len(ret["meta"]["missing"]) == 1
    assert ret["meta"]["n_found"] == 0


def test_collections_overwrite(storage_socket):

    collection = "TorsionDriveRecord"
    name = "Torsion123"
    db = {
        "collection": collection,
        "name": name,
        "something": "else",
        "array": ["54321"],
        "visibility": True,
        "view_available": False,
        "group": "default",
    }

    ret = storage_socket.collection.add(db)

    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.collection.get(collection, name)
    assert ret["meta"]["n_found"] == 1

    view_url = "fooRL"
    db_update = {
        # "id": ret["data"][0]["id"],
        "collection": "TorsionDriveRecord",  # no need to include
        "name": "Torsion123",  # no need to include
        "group": "default",
        "something": "New",
        "something2": "else",
        "view_available": True,
        "view_url": view_url,
        "array2": ["54321"],
    }
    ret = storage_socket.collection.add(db_update, overwrite=True)
    assert ret["meta"]["success"] == True

    ret = storage_socket.collection.get(collection, name)
    assert ret["meta"]["n_found"] == 1

    # Check to make sure the field were replaced and not updated
    db_result = ret["data"][0]
    # existing fields will not be removed, the collection will be updated
    # You will need to remove the old collection and create a new one
    # assert "something" not in db_result
    assert "something" in db_result
    assert "something2" in db_result
    assert db_result["view_available"] is True
    assert db_result["view_url"] == view_url
    assert db_update["something"] == db_result["something"]

    ret = storage_socket.collection.delete(collection, name)
    assert ret == 1


def test_dataset_add_delete_cascade(storage_socket):

    collection = "dataset"
    collection2 = "reactiondataset"
    name = "Dataset123"
    name2 = name + "_2"

    # Add two waters
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")
    _, mol_insert = storage_socket.molecule.add([water, water2])

    db = {
        "collection": collection,
        "name": name,
        "visibility": True,
        "view_available": False,
        "group": "default",
        "records": [
            {"name": "He1", "molecule_id": mol_insert[0], "comment": None, "local_results": {}},
            {"name": "He2", "molecule_id": mol_insert[1], "comment": None, "local_results": {}},
        ],
        "contributed_values": {
            "contrib1": {
                "name": "contrib1",
                "theory_level": "PBE0",
                "units": "kcal/mol",
                "values": [5, 10],
                "index": ["He2", "He1"],
                "values_structure": {},
            }
        },
    }

    ret = storage_socket.collection.add(db.copy())
    # print(ret["meta"]["error_description"])
    assert ret["meta"]["n_inserted"] == 1, ret["meta"]["error_description"]

    ret = storage_socket.collection.get(collection=collection, name=name)
    assert ret["meta"]["success"] is True
    assert len(ret["data"][0]["records"]) == 2

    ret = storage_socket.collection.get(collection=collection, name=name, include=["records"])
    assert ret["meta"]["success"] is True

    db["contributed_values"] = {
        "contrib1": {
            "name": "contrib1",
            "theory_level": "PBE0 FHI-AIMS",
            "units": "kcal/mol",
            "values": np.array([5, 10], dtype=np.int16),
            "index": ["He2", "He1"],
            "values_structure": {},
        },
        "contrib2": {
            "name": "contrib2",
            "theory_level": "PBE0 FHI-AIMS tight",
            "units": "kcal/mol",
            "values": [np.random.rand(2, 3), np.random.rand(2, 3)],
            "index": ["He2", "He1"],
            "values_structure": {},
        },
    }

    ret = storage_socket.collection.add(db.copy(), overwrite=True)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.collection.get(collection=collection, name=name)
    assert ret["meta"]["success"] is True
    assert len(ret["data"][0]["contributed_values"].keys()) == 2

    #  reactiondataset

    db["name"] = name2
    db["collection"] = collection2
    db.pop("records")

    ret = storage_socket.collection.add(db.copy())
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.collection.get(collection=collection2, name=name2)
    assert ret["meta"]["success"] is True
    assert len(ret["data"][0]["contributed_values"].keys()) == 2
    assert len(ret["data"][0]["records"]) == 0

    # cleanup
    # Can't delete molecule when datasets reference it (no cascade)
    ret = storage_socket.molecule.delete(mol_insert)
    assert not ret.success
    assert ret.n_errors == 2
    for e in ret.errors:
        assert "Attempting to delete resulted in error" in e[1]

    # should cascade delete entries and records when dataset is deleted
    assert storage_socket.collection.delete(collection=collection, name=name) == 1
    assert storage_socket.collection.delete(collection=collection2, name=name2) == 1

    # Now okay to delete molecules
    ret = storage_socket.molecule.delete(mol_insert)
    assert ret.success


def test_reset_task_blocks(storage_socket):
    """
    Ensures queue_reset_status must have some search variables so that it does not reset everything.
    """

    with pytest.raises(ValueError):
        storage_socket.procedure.reset_tasks(reset_running=True)

    with pytest.raises(ValueError):
        storage_socket.procedure.reset_tasks(reset_error=True)


def test_collections_include_exclude(storage_socket):

    collection = "Dataset"
    name = "Dataset123"
    name2 = name + "_2"

    # Add two waters
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")
    _, mol_insert = storage_socket.molecule.add([water, water2])

    db = {
        "collection": collection,
        "name": name,
        "visibility": True,
        "view_available": False,
        "group": "default",
        "records": [
            {"name": "He1", "molecule_id": mol_insert[0], "comment": None, "local_results": {}},
            {"name": "He2", "molecule_id": mol_insert[1], "comment": None, "local_results": {}},
        ],
    }

    db2 = {
        "collection": collection,
        "name": name2,
        "visibility": True,
        "view_available": False,
        "records": [],
        "group": "default",
    }

    ret = storage_socket.collection.add(db)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.collection.add(db2)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.collection.get(collection=collection, name=name)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    # print('All: ', ret["data"])

    include = {"records", "name"}
    ret = storage_socket.collection.get(collection=collection, name=name, include=include)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    assert set(ret["data"][0].keys()) == include
    assert len(ret["data"][0]["records"]) == 2
    # print('With projection: ', ret["data"])

    include = {"records", "name"}
    ret = storage_socket.collection.get(collection=collection, name="none_existing", include=include)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 0
    # print('With projection: ', ret["data"])

    include = {"records", "name", "id"}
    ret = storage_socket.collection.get(collection=collection, name=name2, include=include)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    assert set(ret["data"][0].keys()) == include
    assert len(ret["data"][0]["records"]) == 0
    # print('With projection: ', ret["data"])

    exclude = {"records", "name"}
    ret = storage_socket.collection.get(collection=collection, name=name, exclude=exclude)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    assert len(set(ret["data"][0].keys()) & exclude) == 0

    # cleanup
    storage_socket.collection.delete(collection=collection, name=name)
    storage_socket.collection.delete(collection=collection, name=name2)
