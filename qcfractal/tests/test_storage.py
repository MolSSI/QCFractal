"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

from datetime import datetime
from time import time

import numpy as np
import pytest
import sqlalchemy

import qcfractal.interface as ptl
from qcfractal.interface.models.task_models import TaskStatusEnum
from qcfractal.services.services import TorsionDriveService
from qcfractal.testing import sqlalchemy_socket_fixture as storage_socket

bad_id1 = "99999000"
bad_id2 = "99999001"


def test_storage_repr(storage_socket):

    assert isinstance(repr(storage_socket), str)


def test_molecules_add(storage_socket):

    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret1 = storage_socket.add_molecules([water])
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1

    # Try duplicate adds
    ret2 = storage_socket.add_molecules([water])
    assert ret2["meta"]["success"] is True
    assert ret2["meta"]["n_inserted"] == 0
    assert ret2["meta"]["duplicates"][0] == ret1["data"][0]

    # Assert the ids match
    assert ret1["data"][0] == ret2["data"][0]

    # Pull molecule from the DB for tests
    db_json = storage_socket.get_molecules(molecule_hash=water.get_hash())["data"][0]
    water.compare(db_json)

    # Cleanup adds
    ret = storage_socket.del_molecules(molecule_hash=water.get_hash())
    assert ret == 1


def test_identical_mol_insert(storage_socket):
    """
    Tests as edge case where to identical molecules are added under different tags.
    """

    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    # Add two identical molecules
    ret1 = storage_socket.add_molecules([water, water])
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1
    assert ret1["data"][0] == ret1["data"][1]

    # Should only find one molecule
    ret2 = storage_socket.get_molecules(molecule_hash=[water.get_hash()])
    assert ret2["meta"]["n_found"] == 1

    ret = storage_socket.del_molecules(molecule_hash=water.get_hash())
    assert ret == 1


def test_molecules_add_many(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])
    assert ret == 2

    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = storage_socket.del_molecules(id=ret["data"])
    assert ret == 2


def test_molecules_get(storage_socket):

    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = storage_socket.add_molecules([water])
    assert ret["meta"]["n_inserted"] == 1
    water_id = ret["data"][0]

    # Pull molecule from the DB for tests
    water2 = storage_socket.get_molecules(id=water_id)["data"][0]
    water2.compare(water)

    # Cleanup adds
    ret = storage_socket.del_molecules(id=water_id)
    assert ret == 1


def test_molecules_duplicate_insert(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["n_inserted"] == 2

    ret2 = storage_socket.add_molecules([water, water2])
    assert ret2["meta"]["n_inserted"] == 0
    assert ret["data"][0] == ret2["data"][0]
    assert ret["data"][1] == ret2["data"][1]

    ret3 = storage_socket.add_molecules([water, water])
    assert ret2["meta"]["n_inserted"] == 0
    assert ret["data"][0] == ret3["data"][0]
    assert ret["data"][0] == ret3["data"][1]

    # Cleanup adds
    ret = storage_socket.del_molecules(id=ret["data"])
    assert ret == 2


def test_molecules_mixed_add_get(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    del_ids = []
    water2_id = storage_socket.add_molecules([water2])["data"][0]
    del_ids.append(water2_id)

    ret = storage_socket.get_add_molecules_mixed([bad_id1, water, bad_id2, water2_id])
    assert ret["data"][0] is None
    assert ret["data"][1].identifiers.molecule_hash == water.get_hash()
    assert ret["data"][2] is None
    assert ret["data"][3].id == water2_id
    assert set(ret["meta"]["missing"]) == {0, 2}

    # Cleanup adds
    del_ids.append(ret["data"][1].id)
    ret = storage_socket.del_molecules(id=del_ids)
    assert ret == 2


def test_molecules_bad_get(storage_socket):

    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = storage_socket.add_molecules([water])
    water_id = ret["data"][0]

    # Pull molecule from the DB for tests
    ret = storage_socket.get_molecules(id=[water_id, bad_id1, bad_id2])

    assert ret["data"][0].id == water_id
    assert ret["meta"]["n_found"] == 1

    # Cleanup adds
    ret = storage_socket.del_molecules(id=water_id)
    assert ret == 1


def test_keywords_add(storage_socket):

    kw = ptl.models.KeywordSet(**{"values": {"o": 5}, "hash_index": "something_unique"})

    ret = storage_socket.add_keywords([kw, kw.copy()])
    assert len(ret["data"]) == 2
    assert ret["meta"]["n_inserted"] == 1
    assert ret["data"][0] == ret["data"][1]

    ret = storage_socket.add_keywords([kw])
    assert ret["meta"]["n_inserted"] == 0

    ret = storage_socket.get_keywords(hash_index="something_unique")
    ret_kw = ret["data"][0]
    assert ret["meta"]["n_found"] == 1
    assert ret_kw.values == kw.values

    assert 1 == storage_socket.del_keywords(id=ret_kw.id)


def test_keywords_mixed_add_get(storage_socket):

    opts1 = ptl.models.KeywordSet(values={"o": 5})
    id1 = storage_socket.add_keywords([opts1])["data"][0]

    opts2 = ptl.models.KeywordSet(values={"o": 6})
    opts = storage_socket.get_add_keywords_mixed([opts1, opts2, id1, bad_id1, bad_id2])["data"]
    assert opts[0].id == id1
    assert opts[1].values["o"] == 6
    assert opts[2].id == id1
    assert opts[3] is None
    assert opts[4] is None

    assert 1 == storage_socket.del_keywords(id=id1)
    assert 1 == storage_socket.del_keywords(id=opts[1].id)


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

    ret = storage_socket.add_collection(db)

    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection, name)

    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_found"] == 1
    assert db["something"] == ret["data"][0]["something"]

    ret = storage_socket.del_collection(collection, name)
    assert ret == 1

    ret = storage_socket.get_collections(collection, "bleh")
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

    ret = storage_socket.add_collection(db)

    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection, name)
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
    ret = storage_socket.add_collection(db_update, overwrite=True)
    assert ret["meta"]["success"] == True

    ret = storage_socket.get_collections(collection, name)
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

    ret = storage_socket.del_collection(collection, name)
    assert ret == 1


def test_dataset_add_delete_cascade(storage_socket):

    collection = "dataset"
    collection2 = "reactiondataset"
    name = "Dataset123"
    name2 = name + "_2"

    # Add two waters
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules([water, water2])

    db = {
        "collection": collection,
        "name": name,
        "visibility": True,
        "view_available": False,
        "group": "default",
        "records": [
            {"name": "He1", "molecule_id": mol_insert["data"][0], "comment": None, "local_results": {}},
            {"name": "He2", "molecule_id": mol_insert["data"][1], "comment": None, "local_results": {}},
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

    ret = storage_socket.add_collection(db.copy())
    print(ret["meta"]["error_description"])
    assert ret["meta"]["n_inserted"] == 1, ret["meta"]["error_description"]

    ret = storage_socket.get_collections(collection=collection, name=name)
    assert ret["meta"]["success"] is True
    assert len(ret["data"][0]["records"]) == 2

    ret = storage_socket.get_collections(collection=collection, name=name, include=["records"])
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

    ret = storage_socket.add_collection(db.copy(), overwrite=True)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection=collection, name=name)
    assert ret["meta"]["success"] is True
    assert len(ret["data"][0]["contributed_values"].keys()) == 2

    #  reactiondataset

    db["name"] = name2
    db["collection"] = collection2
    db.pop("records")

    ret = storage_socket.add_collection(db.copy())
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection=collection2, name=name2)
    assert ret["meta"]["success"] is True
    assert len(ret["data"][0]["contributed_values"].keys()) == 2
    assert len(ret["data"][0]["records"]) == 0

    # cleanup
    # Can't delete molecule when datasets refernece it (no cascade)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        storage_socket.del_molecules(mol_insert["data"])

    # should cascade delete entries and records when dataset is deleted
    assert storage_socket.del_collection(collection=collection, name=name) == 1
    assert storage_socket.del_collection(collection=collection2, name=name2) == 1

    # Now okay to delete molecules
    storage_socket.del_molecules(mol_insert["data"])


def test_results_add(storage_socket):

    # Add two waters
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules([water, water2])

    kw1 = ptl.models.KeywordSet(**{"comments": "a", "values": {}})
    kwid1 = storage_socket.add_keywords([kw1])["data"][0]

    page1 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][0],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "energy",
            # "extras": {
            #     "other_data": 5
            # },
            "hash_index": 0,
        }
    )

    page2 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][1],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "energy",
            # "extras": {
            #     "other_data": 10
            # },
            "hash_index": 1,
        }
    )

    page3 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][1],
            "method": "M22",
            "basis": "B1",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
            # "extras": {
            #     "other_data": 10
            # },
            "hash_index": 2,
        }
    )

    ids = []
    ret = storage_socket.add_results([page1, page2])
    assert ret["meta"]["n_inserted"] == 2
    ids.extend(ret["data"])

    # add with duplicates:
    ret = storage_socket.add_results([page1, page2, page3])

    assert ret["meta"]["n_inserted"] == 1
    assert len(ret["data"]) == 3  # first 2 found are None
    assert len(ret["meta"]["duplicates"]) == 2

    for res_id in ret["data"]:
        if res_id is not None:
            ids.append(res_id)

    ret = storage_socket.del_results(ids)
    assert ret == 3
    ret = storage_socket.del_molecules(id=mol_insert["data"])
    assert ret == 2


### Build out a set of query tests


@pytest.fixture(scope="function")
def storage_results(storage_socket):
    # Add two waters

    assert len(storage_socket.get_molecules()["data"]) == 0
    mol_names = [
        "water_dimer_minima.psimol",
        "water_dimer_stretch.psimol",
        "water_dimer_stretch2.psimol",
        "neon_tetramer.psimol",
    ]

    molecules = []
    for mol_name in mol_names:
        mol = ptl.data.get_molecule(mol_name)
        molecules.append(mol)

    mol_insert = storage_socket.add_molecules(molecules)

    kw1 = ptl.models.KeywordSet(**{"values": {}})
    kwid1 = storage_socket.add_keywords([kw1])["data"][0]

    page1 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][0],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "energy",
            "return_result": 5,
            "hash_index": 0,
            "status": "COMPLETE",
        }
    )

    page2 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][1],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "energy",
            "return_result": 10,
            "hash_index": 1,
            "status": "COMPLETE",
        }
    )

    page3 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][0],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P2",
            "driver": "gradient",
            "return_result": 15,
            "hash_index": 2,
            "status": "COMPLETE",
        }
    )

    page4 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][0],
            "method": "M2",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P2",
            "driver": "gradient",
            "return_result": 15,
            "hash_index": 3,
            "status": "COMPLETE",
        }
    )

    page5 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][1],
            "method": "M2",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "gradient",
            "return_result": 20,
            "hash_index": 4,
            "status": "COMPLETE",
        }
    )

    page6 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert["data"][1],
            "method": "M3",
            "basis": "B1",
            "keywords": None,
            "program": "P1",
            "driver": "gradient",
            "return_result": 20,
            "hash_index": 5,
            "status": "COMPLETE",
        }
    )

    results_insert = storage_socket.add_results([page1, page2, page3, page4, page5, page6])
    assert results_insert["meta"]["n_inserted"] == 6

    yield storage_socket

    # Cleanup
    all_tasks = storage_socket.get_queue()["data"]
    storage_socket.del_tasks(id=[task.id for task in all_tasks])

    result_ids = [x for x in results_insert["data"]]
    ret = storage_socket.del_results(result_ids)
    assert ret == results_insert["meta"]["n_inserted"]

    ret = storage_socket.del_molecules(id=mol_insert["data"])
    assert ret == mol_insert["meta"]["n_inserted"]


def test_empty_get(storage_results):

    assert 0 == len(storage_results.get_molecules(id=[])["data"])
    assert 0 == len(storage_results.get_molecules(id=bad_id1)["data"])
    # Todo: This needs to return top limit of the table
    assert 4 == len(storage_results.get_molecules()["data"])

    assert 6 == len(storage_results.get_results()["data"])
    assert 1 == len(storage_results.get_results(keywords="null")["data"])
    assert 0 == len(storage_results.get_results(program="null")["data"])


def test_results_get_total(storage_results):

    assert 6 == len(storage_results.get_results()["data"])


def test_results_get_0(storage_results):
    assert 0 == len(storage_results.get_results(limit=0)["data"])


def test_get_results_by_ids(storage_results):
    results = storage_results.get_results()["data"]
    ids = [x["id"] for x in results]

    ret = storage_results.get_results(id=ids, return_json=False)
    assert ret["meta"]["n_found"] == 6
    assert len(ret["data"]) == 6

    ret = storage_results.get_results(id=ids, include=["status", "id"])
    assert ret["data"][0].keys() == {"id", "status"}


def test_results_get_method(storage_results):

    ret = storage_results.get_results(method=["M2", "M1"])
    assert ret["meta"]["n_found"] == 5

    ret = storage_results.get_results(method=["M2"])
    assert ret["meta"]["n_found"] == 2

    ret = storage_results.get_results(method="M2")
    assert ret["meta"]["n_found"] == 2


def test_results_get_dual(storage_results):

    ret = storage_results.get_results(method=["M2", "M1"], program=["P1", "P2"])
    assert ret["meta"]["n_found"] == 5

    ret = storage_results.get_results(method=["M2"], program="P2")
    assert ret["meta"]["n_found"] == 1

    ret = storage_results.get_results(method="M2", program="P2")
    assert ret["meta"]["n_found"] == 1


def test_results_get_project(storage_results):
    """See new changes in design here"""

    ret_true = storage_results.get_results(method="M2", program="P2", include=["return_result", "id"])["data"][0]
    assert set(ret_true.keys()) == {"id", "return_result"}
    assert ret_true["return_result"] == 15

    # Note: explicitly set with_ids=False to remove ids
    ret = storage_results.get_results(method="M2", program="P2", with_ids=False, include=["return_result"])["data"][0]
    assert set(ret.keys()) == {"return_result"}


def test_results_get_driver(storage_results):
    ret = storage_results.get_results(driver="energy")
    assert ret["meta"]["n_found"] == 2


# ------ New Task Queue tests ------
# No hash index, tasks are unique by their base_result


def test_queue_submit_sql(storage_results):

    result1 = storage_results.get_results()["data"][0]

    task1 = ptl.models.TaskRecord(
        **{
            # "hash_index": idx,  # not used anymore
            "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
            "tag": None,
            "program": "p1",
            "parser": "",
            "base_result": result1["id"],
        }
    )

    # Submit a new task
    ret = storage_results.queue_submit([task1])
    assert len(ret["data"]) == 1
    assert ret["meta"]["n_inserted"] == 1

    # submit a duplicate task with a hook
    ret = storage_results.queue_submit([task1])
    assert len(ret["data"]) == 1
    assert ret["meta"]["n_inserted"] == 0
    assert len(ret["meta"]["duplicates"]) == 1

    result2 = storage_results.get_results()["data"][1]

    task2 = ptl.models.TaskRecord(
        **{
            "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
            "tag": None,
            "program": "p1",
            "parser": "",
            "base_result": result2["id"],
        }
    )

    # submit repeated tasks
    ret = storage_results.queue_submit([task2, task2])
    assert len(ret["data"]) == 2
    assert ret["meta"]["n_inserted"] == 1
    assert ret["data"][0] == ret["data"][1]


# ----------------------------------------------------------

# Builds tests for the queue - Changed design


@pytest.mark.parametrize("status", ["COMPLETE", "ERROR"])
def test_storage_queue_roundtrip(storage_results, status):

    results = storage_results.get_results()["data"]

    task_template = {
        "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
        "tag": None,
        "program": "P1",
        "procedure": "P1",
        "parser": "",
        "base_result": None,
    }

    task_template["base_result"] = results[0]["id"]
    task1 = ptl.models.TaskRecord(**task_template)
    task_template["base_result"] = results[1]["id"]
    task2 = ptl.models.TaskRecord(**task_template)

    # Submit a task
    r = storage_results.queue_submit([task1, task2])
    assert len(r["data"]) == 2

    # Add manager 'test_manager'
    storage_results.manager_update("test_manager")
    storage_results.manager_update("test_manager2")
    # Query for next tasks
    r = storage_results.queue_get_next("test_manager", ["p1"], ["p1"], limit=1)
    assert r[0].spec.function == task1.spec.function
    queue_id = r[0].id

    queue_id2 = storage_results.queue_get_next("test_manager2", ["p1"], ["p1"], limit=1)[0].id

    if status == "ERROR":
        err1 = {"error_type": "test_error", "error_message": "Error msg"}
        err2 = {"error_type": "test_error", "error_message": "Error msg2"}
        r = storage_results.queue_mark_error([(queue_id, err1), (queue_id2, err2)])
    elif status == "COMPLETE":
        r = storage_results.queue_mark_complete([queue_id2, queue_id])
        # Check queue is empty
        tasks = storage_results.queue_get_next("test_manager", ["p1"], ["p1"])
        assert len(tasks) == 0

        # completed task should be deleted
        found = storage_results.queue_get_by_id([queue_id, queue_id2])
        assert len(found) == 0

    assert r == 2

    # Check results
    res = storage_results.get_results(id=results[0]["id"])["data"][0]
    assert res["status"] == status
    assert res["manager_name"] == "test_manager"
    if status == "ERROR":
        err_id = res["error"]
        err = storage_results.get_kvstore(err_id)
        js = err["data"][err_id].get_json()
        assert js["error_message"] == "Error msg"
        assert js["error_type"] == "test_error"


def test_queue_submit_many_order(storage_results):

    results = storage_results.get_results()["data"]

    task_template = {
        # "hash_index": idx,
        "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
        "tag": None,
        "program": "P1",
        "procedure": "P1",
        "parser": "",
    }

    task1 = ptl.models.TaskRecord(**task_template, base_result=results[3]["id"])
    task2 = ptl.models.TaskRecord(**task_template, base_result=results[4]["id"])
    task3 = ptl.models.TaskRecord(**task_template, base_result=results[5]["id"])

    # Submit tasks
    ret = storage_results.queue_submit([task1, task2, task3])
    assert len(ret["data"]) == 3
    assert ret["meta"]["n_inserted"] == 3

    # Add a manager
    storage_results.manager_update("test_manager")

    # Get tasks for manager 'test_manager'
    r = storage_results.queue_get_next("test_manager", ["p1"], ["p1"], limit=1)
    assert len(r) == 1
    # will get the first submitted result first
    assert r[0].base_result == results[3]["id"]

    # Todo: test more scenarios


# User testing


def test_user_duplicates(storage_socket):

    r, pw = storage_socket.add_user("george", "shortpw")
    assert r is True

    # Duplicate should bounce
    r, pw = storage_socket.add_user("george", "shortpw")
    assert r is False

    assert storage_socket.remove_user("george") is True

    assert storage_socket.remove_user("george") is False


def test_modify_user(storage_socket):

    r, pw = storage_socket.add_user("george", "oldpw", permissions=["write"])
    assert r is True

    # unknown user
    r, msg = storage_socket.modify_user("geoff", reset_password=True)
    assert r is False

    # update password...
    r, msg = storage_socket.modify_user("george", password="newpw")
    assert r is True
    # ... should update the password without changing permissions
    assert storage_socket.verify_user("george", "newpw", "write")[0] is True

    # update permissions...
    r, msg = storage_socket.modify_user("george", permissions=["read", "write"])
    assert r is True
    # ... should update the permissions without changing the password
    assert storage_socket.verify_user("george", "newpw", "read")[0] is True
    assert storage_socket.verify_user("george", "oldpw", "read")[0] is True

    r, msg = storage_socket.modify_user("george", reset_password=True)
    print(msg)
    assert r is True
    assert storage_socket.verify_user("george", "newpw", "write")[0] is False

    r, msg = storage_socket.modify_user("george", reset_password=True, password="foo")
    assert r is False

    assert storage_socket.remove_user("george") is True


def test_user_permissions_default(storage_socket):

    r, pw = storage_socket.add_user("george", "shortpw")
    assert r is True

    # Verify correct permission
    assert storage_socket.verify_user("george", "shortpw", "read")[0] is True

    # Verify incorrect permission
    assert storage_socket.verify_user("george", "shortpw", "admin")[0] is False

    assert storage_socket.remove_user("george") is True


def test_user_permissions_admin(storage_socket):

    r, pw = storage_socket.add_user("george", "shortpw", permissions=["read", "write", "compute", "admin"])
    assert r is True

    # Verify correct permissions
    assert storage_socket.verify_user("george", "shortpw", "read")[0] is True
    assert storage_socket.verify_user("george", "shortpw", "write")[0] is True
    assert storage_socket.verify_user("george", "shortpw", "compute")[0] is True
    assert storage_socket.verify_user("george", "shortpw", "admin")[0] is True

    assert storage_socket.remove_user("george") is True


def test_manager(storage_socket):

    assert storage_socket.manager_update(name="first_manager")
    assert storage_socket.manager_update(name="first_manager", submitted=100)
    assert storage_socket.manager_update(name="first_manager", submitted=50)

    ret = storage_socket.get_managers(name="first_manager")
    assert ret["data"][0]["submitted"] == 150

    ret = storage_socket.get_managers(name="first_manager", modified_before=datetime.utcnow())
    assert len(ret["data"]) == 1


def test_procedure_sql(storage_results):

    mol_ids = [int(mol.id) for mol in storage_results.get_molecules()["data"]]
    results = storage_results.get_results()["data"]

    assert len(storage_results.get_procedures(procedure="optimization", status=None)["data"]) == 0

    proc_template = {
        "procedure": "optimization",
        "initial_molecule": mol_ids[0],
        "program": "something",
        "hash_index": 123,
        # "trajectory": None,
        "trajectory": [results[0]["id"], results[1]["id"]],
        "qc_spec": {
            "driver": "gradient",
            "method": "HF",
            "basis": "sto-3g",
            # "keywords": None,
            "program": "psi4",
        },
    }

    # Optimization
    inserted = storage_results.add_procedures([ptl.models.OptimizationRecord(**proc_template)])
    assert inserted["meta"]["n_inserted"] == 1

    ret = storage_results.get_procedures(procedure="optimization", status=None)
    assert len(ret["data"]) == 1
    # assert ret['data'][0]['trajectory'] == [str(i) for i in proc_template['trajectory']]
    assert ret["data"][0]["trajectory"] == proc_template["trajectory"]

    new_proc = ret["data"][0]

    test_traj = [
        [results[0]["id"], results[1]["id"], results[2]["id"]],  # add
        # [results[0]['id']],  # remove
        # [results[0]['id']],  # no change
        # None  # empty
    ]
    # update relations
    for trajectory in test_traj:
        new_proc["trajectory"] = trajectory
        ret_count = storage_results.update_procedures([ptl.models.OptimizationRecord(**new_proc)])
        assert ret_count == 1

        ret = storage_results.get_procedures(procedure="optimization", status=None)
        assert len(ret["data"]) == 1
        assert ret["data"][0]["trajectory"] == trajectory

        opt_proc = ret["data"][0]

    # Torsiondrive procedures
    assert len(storage_results.get_procedures(procedure="torsiondrive", status=None)["data"]) == 0

    torsion_proc = {
        "procedure": "torsiondrive",
        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10]},
        "hash_index": 456,
        "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
        "qc_spec": {
            "driver": "gradient",
            "method": "HF",
            "basis": "sto-3g",
            # "keywords": None,
            "program": "psi4",
        },
        "initial_molecule": [mol_ids[0], mol_ids[1]],
        "final_energy_dict": {},
        "optimization_history": {},
        "minimum_positions": {},
        "provenance": {"creator": ""},
    }

    # Torsiondrive init molecule many to many
    inserted2 = storage_results.add_procedures([ptl.models.TorsionDriveRecord(**torsion_proc)])
    assert inserted2["meta"]["n_inserted"] == 1

    ret = storage_results.get_procedures(procedure="torsiondrive", status=None)
    assert len(ret["data"]) == 1
    torsion = ret["data"][0]

    init_mol_tests = [[mol_ids[0]], [mol_ids[0], mol_ids[2], mol_ids[3]]]  # del one

    for init_mol in init_mol_tests:
        torsion["initial_molecule"] = init_mol
        ret = storage_results.update_procedures([ptl.models.TorsionDriveRecord(**torsion)])
        assert ret == 1
        ret = storage_results.get_procedures(procedure="torsiondrive", status=None)
        assert set(ret["data"][0]["initial_molecule"]) == set([str(i) for i in init_mol])

    # optimization history
    opt_hist_tests = [
        {"90": [opt_proc["id"]]},  # add one
        {"90": [opt_proc["id"]], "44": [opt_proc["id"]]},
        {"5": [opt_proc["id"]]},
    ]

    for opt_hist in opt_hist_tests:
        torsion["optimization_history"] = opt_hist
        ret = storage_results.update_procedures([ptl.models.TorsionDriveRecord(**torsion)])
        assert ret == 1
        ret = storage_results.get_procedures(procedure="torsiondrive", status=None)
        assert ret["data"][0]["optimization_history"] == opt_hist

    # clean up
    storage_results.del_procedures(inserted["data"])
    storage_results.del_procedures(inserted2["data"])


def test_services_sql(storage_results):

    mol_ids = [int(mol.id) for mol in storage_results.get_molecules()["data"]]

    torsion_proc = {
        "procedure": "torsiondrive",
        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10]},
        "hash_index": 456,
        "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
        "qc_spec": {
            "driver": "gradient",
            "method": "HF",
            "basis": "sto-3g",
            # "keywords": None,
            "program": "psi4",
        },
        "initial_molecule": [mol_ids[0], mol_ids[1]],
        "final_energy_dict": {},
        "optimization_history": {},
        "minimum_positions": {},
        "provenance": {"creator": ""},
    }

    # Procedure
    proc_pydantic = ptl.models.TorsionDriveRecord(**torsion_proc)

    service_data = {
        "tag": "tag1 tag2",
        "hash_index": "123",
        "status": TaskStatusEnum.waiting,
        "optimization_program": "gaussian",
        # extra fields
        "torsiondrive_state": {},
        "dihedral_template": "1",
        "optimization_template": "2",
        "molecule_template": "",
        "logger": None,
        "storage_socket": storage_results,
        "task_priority": 0,
        "output": proc_pydantic,
    }

    service = TorsionDriveService(**service_data)
    ret = storage_results.add_services([service])
    assert len(ret["data"]) == 1

    ret = storage_results.get_services(procedure_id=ret["data"][0], status=TaskStatusEnum.waiting)
    assert ret["data"][0]["hash_index"] == service_data["hash_index"]

    # attributes in extra fields
    assert ret["data"][0]["dihedral_template"] == service_data["dihedral_template"]

    # Create Pydantic object from DB returned object
    py_obj = TorsionDriveService(**ret["data"][0], storage_socket=storage_results, logger=None)
    assert py_obj

    # Test update
    py_obj.task_priority = 3
    ret_count = storage_results.update_services([py_obj])
    assert ret_count == 1

    ret = storage_results.get_services(procedure_id=ret["data"][0]["procedure_id"], status=TaskStatusEnum.waiting)
    assert ret["data"][0]["task_priority"] == py_obj.task_priority


def test_project_name(storage_socket):
    assert "test" in storage_socket.get_project_name()


def test_results_pagination(storage_socket):
    """
    Test results pagination
    """

    # results = storage_socket.get_results()['data']
    # storage_socket.del_results([result['id'] for result in results])

    assert len(storage_socket.get_results()["data"]) == 0

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    mol = storage_socket.add_molecules([water])["data"][0]

    result_template = {
        "molecule": mol,
        "method": "M1",
        "basis": "B1",
        "keywords": None,
        "program": "P1",
        "driver": "energy",
    }

    # Save (~ 1-7 msec/doc)
    t1 = time()

    total_results = 50
    first_half = int(total_results / 2)
    limit = 10
    skip = 5

    results = []
    for i in range(first_half):
        tmp = result_template.copy()
        tmp["basis"] = str(i)
        results.append(ptl.models.ResultRecord(**tmp))

    result_template["method"] = "M2"
    for i in range(first_half, total_results):
        tmp = result_template.copy()
        tmp["basis"] = str(i)
        results.append(ptl.models.ResultRecord(**tmp))

    inserted = storage_socket.add_results(results)
    assert inserted["meta"]["n_inserted"] == total_results

    # total_time = (time() - t1) * 1000 / total_results
    # print('Inserted {} results in {:.2f} msec / doc'.format(total_results, total_time))
    #
    # query (~ 0.03 msec/doc)
    # t1 = time()

    ret = storage_socket.get_results(method="M2", status=None, limit=limit, skip=skip)

    # total_time = (time() - t1) * 1000 / first_half
    # print('Query {} results in {:.2f} msec /doc'.format(first_half, total_time))

    # count is total, but actual data size is the limit
    assert ret["meta"]["n_found"] == total_results - first_half
    assert len(ret["data"]) == limit

    assert int(ret["data"][0]["basis"]) == first_half + skip

    # get the last page when with fewer than limit are remaining
    ret = storage_socket.get_results(method="M1", skip=(int(first_half - limit / 2)), status=None)
    assert len(ret["data"]) == limit / 2

    # cleanup
    storage_socket.del_results(inserted["data"])
    storage_socket.del_molecules(mol)


def test_procedure_pagination(storage_socket):
    """
    Test procedure pagination
    """

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    mol = storage_socket.add_molecules([water])["data"][0]

    assert len(storage_socket.get_procedures(procedure="optimization")["data"]) == 0

    proc_template = {
        "initial_molecule": mol,
        "program": "something",
        "qc_spec": {"driver": "gradient", "method": "HF", "basis": "sto-3g", "keywords": None, "program": "psi4"},
    }

    total = 10
    limit = 5
    skip = 4

    procedures = []
    for i in range(total):
        tmp = proc_template.copy()
        tmp["hash_index"] = str(i)
        procedures.append(ptl.models.OptimizationRecord(**tmp))

    inserted = storage_socket.add_procedures(procedures)
    assert inserted["meta"]["n_inserted"] == total

    ret = storage_socket.get_procedures(procedure="optimization", status=None, limit=limit, skip=skip)

    # count is total, but actual data size is the limit
    assert ret["meta"]["n_found"] == total
    assert len(ret["data"]) == limit

    storage_socket.del_procedures(inserted["data"])
    storage_socket.del_molecules(mol)


def test_mol_pagination(storage_socket):
    """
    Test Molecule pagination
    """

    assert len(storage_socket.get_molecules()["data"]) == 0
    mol_names = [
        "water_dimer_minima.psimol",
        "water_dimer_stretch.psimol",
        "water_dimer_stretch2.psimol",
        "neon_tetramer.psimol",
    ]

    total = len(mol_names)
    molecules = []
    for mol_name in mol_names:
        mol = ptl.data.get_molecule(mol_name)
        molecules.append(mol)

    inserted = storage_socket.add_molecules(molecules)

    try:
        assert inserted["meta"]["n_inserted"] == total

        ret = storage_socket.get_molecules(skip=1)
        assert len(ret["data"]) == total - 1
        assert ret["meta"]["n_found"] == total

        ret = storage_socket.get_molecules(skip=total + 1)
        assert len(ret["data"]) == 0
        assert ret["meta"]["n_found"] == total

    finally:
        # cleanup
        storage_socket.del_molecules(inserted["data"])


def test_mol_formula(storage_socket):
    """
    Test Molecule pagination
    """

    assert len(storage_socket.get_molecules()["data"]) == 0
    mol_names = [
        "water_dimer_minima.psimol",
    ]
    total = len(mol_names)
    molecules = []
    for mol_name in mol_names:
        mol = ptl.data.get_molecule(mol_name)
        molecules.append(mol)

    inserted = storage_socket.add_molecules(molecules)
    try:
        assert inserted["meta"]["n_inserted"] == total

        ret = storage_socket.get_molecules(molecular_formula="H4O2")
        assert len(ret["data"]) == 1
        assert ret["meta"]["n_found"] == 1

        ret = storage_socket.get_molecules(molecular_formula="O2H4")
        assert len(ret["data"]) == 1
        assert ret["meta"]["n_found"] == 1

        ret = storage_socket.get_molecules(molecular_formula="H4o2")
        assert len(ret["data"]) == 0
        assert ret["meta"]["n_found"] == 0

    finally:
        # cleanup
        storage_socket.del_molecules(inserted["data"])


def test_reset_task_blocks(storage_socket):
    """
    Ensures queue_reset_status must have some search variables so that it does not reset everything.
    """

    with pytest.raises(ValueError):
        storage_socket.queue_reset_status(reset_running=True)

    with pytest.raises(ValueError):
        storage_socket.queue_reset_status(reset_error=True)


def test_server_log(storage_results):

    # Add something to double check the test
    mol_names = ["water_dimer_minima.psimol", "water_dimer_stretch.psimol", "water_dimer_stretch2.psimol"]

    molecules = [ptl.data.get_molecule(mol_name) for mol_name in mol_names]
    inserted = storage_results.add_molecules(molecules)

    ret = storage_results.log_server_stats()
    assert ret["db_table_size"] >= 1000
    assert ret["db_total_size"] >= 1000

    for row in ret["db_table_information"]["rows"]:
        if row[0] == "molecule":
            assert row[2] >= 1000

    # Check queries
    now = datetime.utcnow()
    ret = storage_results.get_server_stats_log(after=now)
    assert len(ret["data"]) == 0

    ret = storage_results.get_server_stats_log(before=now)
    assert len(ret["data"]) >= 1

    # Make sure we are sorting correctly
    storage_results.log_server_stats()
    ret = storage_results.get_server_stats_log(limit=1)
    assert ret["data"][0]["timestamp"] > now


def test_collections_include_exclude(storage_socket):

    collection = "Dataset"
    name = "Dataset123"
    name2 = name + "_2"

    # Add two waters
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules([water, water2])

    db = {
        "collection": collection,
        "name": name,
        "visibility": True,
        "view_available": False,
        "group": "default",
        "records": [
            {"name": "He1", "molecule_id": mol_insert["data"][0], "comment": None, "local_results": {}},
            {"name": "He2", "molecule_id": mol_insert["data"][1], "comment": None, "local_results": {}},
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

    ret = storage_socket.add_collection(db)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.add_collection(db2)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection=collection, name=name)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    # print('All: ', ret["data"])

    include = {"records", "name"}
    ret = storage_socket.get_collections(collection=collection, name=name, include=include)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    assert set(ret["data"][0].keys()) == include
    assert len(ret["data"][0]["records"]) == 2
    # print('With projection: ', ret["data"])

    include = {"records", "name"}
    ret = storage_socket.get_collections(collection=collection, name="none_existing", include=include)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 0
    # print('With projection: ', ret["data"])

    include = {"records", "name", "id"}
    ret = storage_socket.get_collections(collection=collection, name=name2, include=include)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    assert set(ret["data"][0].keys()) == include
    assert len(ret["data"][0]["records"]) == 0
    # print('With projection: ', ret["data"])

    exclude = {"records", "name"}
    ret = storage_socket.get_collections(collection=collection, name=name, exclude=exclude)
    assert ret["meta"]["success"] is True
    assert len(ret["data"]) == 1
    assert len(set(ret["data"][0].keys()) & exclude) == 0

    # cleanup
    storage_socket.del_collection(collection=collection, name=name)
    storage_socket.del_collection(collection=collection, name=name2)
    storage_socket.del_molecules(mol_insert["data"])
