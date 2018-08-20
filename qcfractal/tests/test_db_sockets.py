"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import pytest

# Import the DQM collection
import qcfractal.interface as qp
from qcfractal.testing import db_socket_fixture as db_socket


def test_molecules_add(db_socket):

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret1 = db_socket.add_molecules({"new_water": water.to_json()})
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1

    # Try duplicate adds
    ret2 = db_socket.add_molecules({"new_water2": water.to_json()})
    assert ret2["meta"]["success"] is True
    assert ret2["meta"]["n_inserted"] == 0
    assert ret2["meta"]["duplicates"][0] == "new_water2"

    # Assert the ids match
    assert ret1["data"]["new_water"] == ret2["data"]["new_water2"]

    # Pull molecule from the DB for tests
    db_json = db_socket.get_molecules(water.get_hash(), index="hash")["data"][0]
    water.compare(db_json)

    # Cleanup adds
    ret = db_socket.del_molecules(water.get_hash(), index="hash")
    assert ret == 1


def test_identical_mol_insert(db_socket):
    """
    Tests as edge case where to identical molecules are added under different tags.
    """

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Add two idential molecules
    ret1 = db_socket.add_molecules({"w1": water.to_json(), "w2": water.to_json()})
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1
    assert ret1["data"]["w1"] == ret1["data"]["w2"]

    # Should only find one molecule
    ret2 = db_socket.get_molecules([water.get_hash()], index="hash")
    assert ret2["meta"]["n_found"] == 1

    ret = db_socket.del_molecules(water.get_hash(), index="hash")
    assert ret == 1


def test_molecules_add_many(db_socket):
    water = qp.data.get_molecule("water_dimer_minima.psimol")
    water2 = qp.data.get_molecule("water_dimer_stretch.psimol")

    ret = db_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = db_socket.del_molecules([water.get_hash(), water2.get_hash()], index="hash")
    assert ret == 2

    ret = db_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = db_socket.del_molecules(list(ret["data"].values()), index="id")
    assert ret == 2


def test_molecules_get(db_socket):

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = db_socket.add_molecules({"water": water.to_json()})
    assert ret["meta"]["n_inserted"] == 1
    water_id = ret["data"]["water"]

    # Pull molecule from the DB for tests
    db_json = db_socket.get_molecules(water_id, index="id")["data"][0]
    water_db = qp.Molecule.from_json(db_json)
    water_db.compare(water)

    # Cleanup adds
    ret = db_socket.del_molecules(water_id, index="id")
    assert ret == 1


def test_options_add(db_socket):

    opts = qp.data.get_options("psi_default")

    ret = db_socket.add_options([opts, opts.copy()])
    assert ret["meta"]["n_inserted"] == 1

    ret = db_socket.add_options(opts)
    assert ret["meta"]["n_inserted"] == 0

    ret = db_socket.get_options([(opts["program"], opts["name"])])
    assert ret["meta"]["n_found"] == 1
    assert ret["data"][0] == opts

    assert 1 == db_socket.del_option(opts["program"], opts["name"])


def test_options_error(db_socket):
    opts = qp.data.get_options("psi_default")

    del opts["name"]
    ret = db_socket.add_options(opts)
    assert ret["meta"]["n_inserted"] == 0
    assert len(ret["meta"]["validation_errors"]) == 1


def test_databases_add(db_socket):

    db = {"category": "OpenFF", "name": "Torsion123", "something": "else", "array": ["54321"]}

    ret = db_socket.add_database(db)
    assert ret["meta"]["n_inserted"] == 1

    ret = db_socket.get_databases([(db["category"], db["name"])])
    assert ret["meta"]["success"] == True
    assert ret["meta"]["n_found"] == 1
    assert db == ret["data"][0]

    ret = db_socket.del_database(db["category"], db["name"])
    assert ret == 1

    ret = db_socket.get_databases([(db["category"], "bleh")])
    assert len(ret["meta"]["missing"]) == 1
    assert ret["meta"]["n_found"] == 0


def test_results_add(db_socket):

    # Add two waters
    water = qp.data.get_molecule("water_dimer_minima.psimol")
    water2 = qp.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = db_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})

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

    ret = db_socket.add_results([page1, page2])
    assert ret["meta"]["n_inserted"] == 2

    result_ids = [x[1] for x in ret["data"]]
    ret = db_socket.del_results(result_ids, index="id")
    assert ret == 2

    ret = db_socket.del_molecules(list(mol_insert["data"].values()), index="id")
    assert ret == 2


### Build out a set of query tests


@pytest.fixture(scope="module")
def db_results(db_socket):
    # Add two waters
    water = qp.data.get_molecule("water_dimer_minima.psimol")
    water2 = qp.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = db_socket.add_molecules({"water1": water.to_json(), "water2": water2.to_json()})

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

    results_insert = db_socket.add_results([page1, page2, page3, page4, page5])

    yield db_socket

    # Cleanup
    result_ids = [x[1] for x in results_insert["data"]]
    ret = db_socket.del_results(result_ids, index="id")
    assert ret == results_insert["meta"]["n_inserted"]

    ret = db_socket.del_molecules(list(mol_insert["data"].values()), index="id")
    assert ret == mol_insert["meta"]["n_inserted"]


def test_results_query_total(db_results):

    assert 5 == len(db_results.get_results({})["data"])


def test_results_query_method(db_results):

    ret = db_results.get_results({"method": ["M2", "M1"]})
    assert ret["meta"]["n_found"] == 5

    ret = db_results.get_results({"method": ["M2"]})
    assert ret["meta"]["n_found"] == 2

    ret = db_results.get_results({"method": "M2"})
    assert ret["meta"]["n_found"] == 2


def test_results_query_dual(db_results):

    ret = db_results.get_results({"method": ["M2", "M1"], "program": ["P1", "P2"]})
    assert ret["meta"]["n_found"] == 5

    ret = db_results.get_results({"method": ["M2"], "program": "P2"})
    assert ret["meta"]["n_found"] == 1

    ret = db_results.get_results({"method": "M2", "program": "P2"})
    assert ret["meta"]["n_found"] == 1


def test_results_query_project(db_results):
    ret = db_results.get_results({"method": "M2", "program": "P2"}, projection={"return_result": True})["data"][0]
    assert set(ret.keys()) == {"return_result"}
    assert ret["return_result"] == 15


def test_results_query_driver(db_results):
    ret = db_results.get_results({"driver": "energy"})
    assert ret["meta"]["n_found"] == 2


# Builds tests for the queue


def test_queue_roundtrip(db_socket):

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
    r = db_socket.queue_submit([task1])
    assert len(r["data"]) == 1

    # Query for next jobs
    r = db_socket.queue_get_next()
    assert r[0]["spec"]["function"] == task1["spec"]["function"]

    # Mark job as done
    r  = db_socket.queue_mark_complete([r[0]["id"]])
    assert r == 1

    r = db_socket.queue_get_next()
    assert len(r) == 0

def test_queue_duplicate(db_socket):

    idx = "unique_hash_idx123"
    task1 = {
        "hash_index": idx,
        "spec": {},
        "hooks": [("service", "123")],
        "tag": None,
    }
    r = db_socket.queue_submit([task1])
    uid = r["data"][0][-1]
    assert len(r["data"]) == 1

    # Put the first job in a waiting state
    r = db_socket.queue_get_next()
    assert len(r) == 1

    # Change hooks
    task1["hooks"] = [("service", "456")]
    r = db_socket.queue_submit([task1])
    assert len(r["data"]) == 0

    # Pull out the data and check the hooks
    r = db_socket.get_queue([uid], by_id=True)
    hooks = r["data"][0]["hooks"]
    assert len(hooks) == 2
    assert hooks[0][0] == "service"
    assert hooks[1][0] == "service"
    assert {"123", "456"} == {hooks[0][1], hooks[1][1]}

    # Cleanup
    r = db_socket.queue_mark_complete([uid])
    assert r == 1


