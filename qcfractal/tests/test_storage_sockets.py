"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import pytest

import qcfractal.interface as portal
from qcfractal.testing import storage_socket_fixture as storage_socket


def test_molecules_add(storage_socket):

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret1 = storage_socket.add_molecules({"new_water": water.to_json()})
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1

    # Try duplicate adds
    ret2 = storage_socket.add_molecules({"new_water2": water.to_json()})
    assert ret2["meta"]["success"] is True
    assert ret2["meta"]["n_inserted"] == 0
    assert ret2["meta"]["duplicates"][0] == "new_water2"

    # Assert the ids match
    assert ret1["data"]["new_water"] == ret2["data"]["new_water2"]

    # Pull molecule from the DB for tests
    db_json = storage_socket.get_molecules(water.get_hash(), index="hash")["data"][0]
    water.compare(db_json)

    # Cleanup adds
    ret = storage_socket.del_molecules(water.get_hash(), index="hash")
    assert ret == 1


def test_identical_mol_insert(storage_socket):
    """
    Tests as edge case where to identical molecules are added under different tags.
    """

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Add two idential molecules
    ret1 = storage_socket.add_molecules({"w1": water.to_json(), "w2": water.to_json()})
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1
    assert ret1["data"]["w1"] == ret1["data"]["w2"]

    # Should only find one molecule
    ret2 = storage_socket.get_molecules([water.get_hash()], index="hash")
    assert ret2["meta"]["n_found"] == 1

    ret = storage_socket.del_molecules(water.get_hash(), index="hash")
    assert ret == 1


def test_molecules_add_many(storage_socket):
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = storage_socket.del_molecules([water.get_hash(), water2.get_hash()], index="hash")
    assert ret == 2

    ret = storage_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = storage_socket.del_molecules(list(ret["data"].values()), index="id")
    assert ret == 2


def test_molecules_get(storage_socket):

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = storage_socket.add_molecules({"water": water.to_json()})
    assert ret["meta"]["n_inserted"] == 1
    water_id = ret["data"]["water"]

    # Pull molecule from the DB for tests
    db_json = storage_socket.get_molecules(water_id, index="id")["data"][0]
    water_db = portal.Molecule.from_json(db_json)
    water_db.compare(water)

    # Cleanup adds
    ret = storage_socket.del_molecules(water_id, index="id")
    assert ret == 1


def test_molecules_bad_get(storage_socket):

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = storage_socket.add_molecules({"water": water.to_json()})
    assert ret["meta"]["n_inserted"] == 1
    water_id = ret["data"]["water"]

    # Pull molecule from the DB for tests
    ret = storage_socket.get_molecules([water_id, "something", 5, (3, 2)], index="id")
    assert len(ret["meta"]["errors"]) == 1
    assert ret["meta"]["errors"][0][0] == "Bad Ids"
    assert len(ret["meta"]["errors"][0][1]) == 3
    assert ret["meta"]["n_found"] == 1

    # Cleanup adds
    ret = storage_socket.del_molecules(water_id, index="id")
    assert ret == 1


def test_options_add(storage_socket):

    opts = portal.data.get_options("psi_default")

    ret = storage_socket.add_options([opts, opts.copy()])
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.add_options(opts)
    assert ret["meta"]["n_inserted"] == 0

    ret = storage_socket.get_options([(opts["program"], opts["name"])])
    del opts["id"]
    assert ret["meta"]["n_found"] == 1
    assert ret["data"][0] == opts

    assert 1 == storage_socket.del_option(opts["program"], opts["name"])


def test_options_error(storage_socket):
    opts = portal.data.get_options("psi_default")

    del opts["name"]
    ret = storage_socket.add_options(opts)
    assert ret["meta"]["n_inserted"] == 0
    assert len(ret["meta"]["validation_errors"]) == 1


def test_collections_add(storage_socket):

    db = {"category": "OpenFF", "name": "Torsion123", "something": "else", "array": ["54321"]}

    ret = storage_socket.add_collection(db)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections([(db["category"], db["name"])])
    assert ret["meta"]["success"] == True
    assert ret["meta"]["n_found"] == 1
    assert db == ret["data"][0]

    ret = storage_socket.del_collection(db["category"], db["name"])
    assert ret == 1

    ret = storage_socket.get_collections([(db["category"], "bleh")])
    assert len(ret["meta"]["missing"]) == 1
    assert ret["meta"]["n_found"] == 0


def test_collections_overwrite(storage_socket):

    db = {"category": "OpenFF", "name": "Torsion123", "something": "else", "array": ["54321"]}

    ret = storage_socket.add_collection(db)
    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections([(db["category"], db["name"])])
    assert ret["meta"]["n_found"] == 1

    db_update = {
        "id": ret["data"][0]["id"],
        "category": "OpenFF",
        "name": "Torsion123",
        "something2": "else",
        "array2": ["54321"]
    }
    ret = storage_socket.add_collection(db_update, overwrite=True)
    assert ret["meta"]["success"] == True

    ret = storage_socket.get_collections([(db["category"], db["name"])])
    assert ret["meta"]["n_found"] == 1

    # Check to make sure the field were replaced and not updated
    db_result = ret["data"][0]
    assert "something" not in db_result
    assert "something2" in db_result

    ret = storage_socket.del_collection(db["category"], db["name"])
    assert ret == 1


def test_results_add(storage_socket):

    # Add two waters
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})

    page1 = {
        "molecule_id": mol_insert["data"]["water1"],
        "method": "M1",
        "basis": "B1",
        "options": "default",
        "program": "P1",
        "driver": "energy",
        "other_data": 5
    }

    page2 = {
        "molecule_id": mol_insert["data"]["water2"],
        "method": "M1",
        "basis": "B1",
        "options": "default",
        "program": "P1",
        "driver": "energy",
        "other_data": 10
    }

    ret = storage_socket.add_results([page1, page2])
    assert ret["meta"]["n_inserted"] == 2

    result_ids = [x[1] for x in ret["data"]]
    ret = storage_socket.del_results(result_ids, index="id")
    assert ret == 2

    ret = storage_socket.del_molecules(list(mol_insert["data"].values()), index="id")
    assert ret == 2


### Build out a set of query tests


@pytest.fixture(scope="module")
def storage_results(storage_socket):
    # Add two waters
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})

    page1 = {
        "molecule_id": mol_insert["data"]["water1"],
        "method": "M1",
        "basis": "B1",
        "options": "default",
        "program": "P1",
        "driver": "energy",
        "return_result": 5
    }

    page2 = {
        "molecule_id": mol_insert["data"]["water2"],
        "method": "M1",
        "basis": "B1",
        "options": "default",
        "program": "P1",
        "driver": "energy",
        "return_result": 10
    }

    page3 = {
        "molecule_id": mol_insert["data"]["water1"],
        "method": "M1",
        "basis": "B1",
        "options": "default",
        "program": "P2",
        "driver": "gradient",
        "return_result": 15
    }

    page4 = {
        "molecule_id": mol_insert["data"]["water1"],
        "method": "M2",
        "basis": "B1",
        "options": "default",
        "program": "P2",
        "driver": "gradient",
        "return_result": 15
    }

    page5 = {
        "molecule_id": mol_insert["data"]["water2"],
        "method": "M2",
        "basis": "B1",
        "options": "default",
        "program": "P1",
        "driver": "gradient",
        "return_result": 20
    }

    results_insert = storage_socket.add_results([page1, page2, page3, page4, page5])

    yield storage_socket

    # Cleanup
    result_ids = [x[1] for x in results_insert["data"]]
    ret = storage_socket.del_results(result_ids, index="id")
    assert ret == results_insert["meta"]["n_inserted"]

    ret = storage_socket.del_molecules(list(mol_insert["data"].values()), index="id")
    assert ret == mol_insert["meta"]["n_inserted"]


def test_results_query_total(storage_results):

    assert 5 == len(storage_results.get_results({})["data"])


def test_results_query_method(storage_results):

    ret = storage_results.get_results({"method": ["M2", "M1"]})
    assert ret["meta"]["n_found"] == 5

    ret = storage_results.get_results({"method": ["M2"]})
    assert ret["meta"]["n_found"] == 2

    ret = storage_results.get_results({"method": "M2"})
    assert ret["meta"]["n_found"] == 2


def test_results_query_dual(storage_results):

    ret = storage_results.get_results({"method": ["M2", "M1"], "program": ["P1", "P2"]})
    assert ret["meta"]["n_found"] == 5

    ret = storage_results.get_results({"method": ["M2"], "program": "P2"})
    assert ret["meta"]["n_found"] == 1

    ret = storage_results.get_results({"method": "M2", "program": "P2"})
    assert ret["meta"]["n_found"] == 1


def test_results_query_project(storage_results):
    ret = storage_results.get_results({"method": "M2", "program": "P2"}, projection={"return_result": True})["data"][0]
    assert set(ret.keys()) == {"return_result"}
    assert ret["return_result"] == 15


def test_results_query_driver(storage_results):
    ret = storage_results.get_results({"driver": "energy"})
    assert ret["meta"]["n_found"] == 2


# Builds tests for the queue


def test_queue_roundtrip(storage_socket):

    idx = "unique_hash_idx123"
    task1 = {
        "hash_index": idx,
        "spec": {
            "function": "qcengine.compute_procedure",
            "args": [{
                "json_blob": "data"
            }],
            "kwargs": {},
        },
        "hooks": [("service", "")],
        "tag": None,
    }

    # Submit a job
    r = storage_socket.queue_submit([task1])
    assert len(r["data"]) == 1

    # Query for next jobs
    r = storage_socket.queue_get_next()
    assert r[0]["spec"]["function"] == task1["spec"]["function"]

    # Mark job as done
    r = storage_socket.queue_mark_complete([r[0]["id"]])
    assert r == 1

    r = storage_socket.queue_get_next()
    assert len(r) == 0


def test_queue_duplicate(storage_socket):

    idx = "unique_hash_idx123"
    task1 = {
        "hash_index": idx,
        "spec": {},
        "hooks": [("service", "123")],
        "tag": None,
    }
    r = storage_socket.queue_submit([task1])
    uid = r["data"][0][-1]
    assert len(r["data"]) == 1

    # Put the first job in a waiting state
    r = storage_socket.queue_get_next()
    assert len(r) == 1

    # Change hooks, only one submission due to hash_index conflict
    task1["hooks"] = [("service", "456")]
    r = storage_socket.queue_submit([task1])
    assert len(r["data"]) == 0

    # Pull out the data and check the hooks
    r = storage_socket.get_queue([uid], by_id=True)
    hooks = r["data"][0]["hooks"]
    assert len(hooks) == 2
    assert hooks[0][0] == "service"
    assert hooks[1][0] == "service"
    assert {"123", "456"} == {hooks[0][1], hooks[1][1]}

    # Cleanup
    r = storage_socket.queue_mark_complete([uid])
    assert r == 1


# User testing


def test_user_duplicates(storage_socket):

    r = storage_socket.add_user("george", "shortpw")
    assert r is True

    # Duplicate should bounce
    r = storage_socket.add_user("george", "shortpw")
    assert r is False

    assert storage_socket.remove_user("george") is True

    assert storage_socket.remove_user("george") is False


def test_user_permissions_default(storage_socket):

    r = storage_socket.add_user("george", "shortpw")
    assert r is True

    # Verify correct permission
    assert storage_socket.verify_user("george", "shortpw", "read")[0] is True

    # Verify incorrect permission
    assert storage_socket.verify_user("george", "shortpw", "admin")[0] is False

    assert storage_socket.remove_user("george") is True


def test_user_permissions_admin(storage_socket):

    r = storage_socket.add_user("george", "shortpw", permissions=["read", "write", "compute", "admin"])
    assert r is True

    # Verify correct permissions
    assert storage_socket.verify_user("george", "shortpw", "read")[0] is True
    assert storage_socket.verify_user("george", "shortpw", "write")[0] is True
    assert storage_socket.verify_user("george", "shortpw", "compute")[0] is True
    assert storage_socket.verify_user("george", "shortpw", "admin")[0] is True

    assert storage_socket.remove_user("george") is True
