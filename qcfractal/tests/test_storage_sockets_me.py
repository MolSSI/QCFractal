"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import pytest

import qcfractal.interface as portal
from qcfractal.testing import mongoengine_socket_fixture as storage_socket


def test_molecules_add(storage_socket):

    water = portal.data.get_molecule("water_dimer_minima.psimol")

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
    ret1 = storage_socket.add_molecules([water, water])
    assert ret1["meta"]["success"] is True
    assert ret1["meta"]["n_inserted"] == 1
    assert ret1["data"][0] == ret1["data"][1]

    # Should only find one molecule
    ret2 = storage_socket.get_molecules([water.get_hash()], index="hash")
    assert ret2["meta"]["n_found"] == 1

    ret = storage_socket.del_molecules(water.get_hash(), index="hash")
    assert ret == 1


def test_molecules_add_many(storage_socket):
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = storage_socket.del_molecules([water.get_hash(), water2.get_hash()], index="hash")
    assert ret == 2

    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["n_inserted"] == 2

    # Cleanup adds
    ret = storage_socket.del_molecules(ret["data"], index="id")
    assert ret == 2


def test_molecules_get(storage_socket):

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = storage_socket.add_molecules([water])
    assert ret["meta"]["n_inserted"] == 1
    water_id = ret["data"][0]

    # Pull molecule from the DB for tests
    db_json = storage_socket.get_molecules(water_id, index="id")["data"][0]
    water2 = portal.Molecule(**db_json)
    water2.compare(water)

    # Cleanup adds
    ret = storage_socket.del_molecules(water_id, index="id")
    assert ret == 1


def test_molecules_mixed_add_get(storage_socket):
    water = portal.data.get_molecule("water_dimer_minima.psimol")

    ret = storage_socket.get_add_molecules_mixed(["bad_id", water, "bad_id2"])
    assert ret["data"][0] is None
    assert ret["data"][1]["identifiers"]["molecule_hash"] == water.get_hash()
    assert ret["data"][2] is None
    assert set(ret["meta"]["missing"]) == {0, 2}

    # Cleanup adds
    ret = storage_socket.del_molecules([ret["data"][1]["id"]], index="id")
    assert ret == 1


def test_molecules_bad_get(storage_socket):

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = storage_socket.add_molecules([water])
    assert ret["meta"]["n_inserted"] == 1
    water_id = ret["data"][0]

    # Pull molecule from the DB for tests
    ret = storage_socket.get_molecules([water_id, "something", 5, (3, 2)], index="id")
    assert len(ret["meta"]["errors"]) == 1
    assert ret["meta"]["errors"][0][0] == "Bad Ids"
    assert len(ret["meta"]["errors"][0][1]) == 3
    assert ret["meta"]["n_found"] == 1

    # Cleanup adds
    ret = storage_socket.del_molecules(water_id, index="id")
    assert ret == 1


def test_keywords_add(storage_socket):

    opts = {"program": "hello", "values": {"o": 5}, "hash_index": "something_unique"}

    ret = storage_socket.add_keywords([opts, opts.copy()])
    assert len(ret["data"]) == 2
    assert ret["meta"]["n_inserted"] == 1
    assert ret["data"][0] == ret["data"][1]

    ret = storage_socket.add_keywords(opts)
    assert ret["meta"]["n_inserted"] == 0

    ret = storage_socket.get_keywords(hash_index="something_unique")
    opts["id"] = ret["data"][0]["id"]
    assert ret["meta"]["n_found"] == 1
    assert ret["data"][0] == opts

    assert 1 == storage_socket.del_keywords(id=opts["id"])


def test_keywords_mixed_add_get(storage_socket):

    opts1 = portal.models.KeywordSet(**{"program": "hello", "values": {"o": 5}})
    id1 = storage_socket.add_keywords([opts1.json_dict()])["data"][0]

    opts2 = {"program": "hello", "values": {"o": 6}}
    opts = storage_socket.get_add_keywords_mixed([opts1, opts2, id1, "bad_id"])["data"]
    assert opts[0]["id"] == id1
    assert opts[1]["values"]["o"] == 6
    assert "id" in opts[1]
    assert opts[2]["id"] == id1
    assert opts[3] is None

    assert 1 == storage_socket.del_keywords(id=id1)
    assert 1 == storage_socket.del_keywords(id=opts[1]["id"])


def test_keywords_error(storage_socket):
    opts = {"program": "hello"}

    ret = storage_socket.add_keywords(opts)
    assert ret["meta"]["n_inserted"] == 0
    assert len(ret["meta"]["validation_errors"]) == 1


def test_collections_add(storage_socket):

    collection = 'TorsionDrive'
    name = 'Torsion123'
    db = {"something": "else", "array": ["54321"]}

    ret = storage_socket.add_collection(collection, name, db)

    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection, name)

    assert ret["meta"]["success"] == True
    assert ret["meta"]["n_found"] == 1
    assert db['something'] == ret["data"][0]['something']

    ret = storage_socket.del_collection(collection, name)
    assert ret == 1

    ret = storage_socket.get_collections(collection, "bleh")
    # assert len(ret["meta"]["missing"]) == 1
    assert ret["meta"]["n_found"] == 0


def test_collections_overwrite(storage_socket):

    collection = "TorsionDrive"
    name = "Torsion123"
    db = {"something": "else", "array": ["54321"]}

    ret = storage_socket.add_collection(collection, name, db)

    assert ret["meta"]["n_inserted"] == 1

    ret = storage_socket.get_collections(collection, name)
    assert ret["meta"]["n_found"] == 1

    db_update = {
        # "id": ret["data"][0]["id"],
        "collection": "TorsionDrive",  # no need to include
        "name": "Torsion123",  # no need to include
        "something": "New",
        "something2": "else",
        "array2": ["54321"]
    }
    ret = storage_socket.add_collection(collection, name, db_update, overwrite=True)
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
    assert db_update['something'] == db_result['something']

    ret = storage_socket.del_collection(collection, name)
    assert ret == 1


def test_results_add(storage_socket):

    # Add two waters
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules([water, water2])

    page1 = {
        "molecule": mol_insert["data"][0],
        "method": "M1",
        "basis": "B1",
        "keywords": "default",
        "program": "P1",
        "driver": "energy",
        "other_data": 5,
        "hash_index": 0,
    }

    page2 = {
        "molecule": mol_insert["data"][1],
        "method": "M1",
        "basis": "B1",
        "keywords": "default",
        "program": "P1",
        "driver": "energy",
        "other_data": 10,
        "hash_index": 1,
    }

    page3 = {
        "molecule": mol_insert["data"][1],
        "method": "M22",
        "basis": "B1",
        "keywords": "default",
        "program": "P1",
        "driver": "energy",
        "other_data": 10,
        "hash_index": 2,
    }
    ids = []
    ret = storage_socket.add_results([page1, page2])
    assert ret["meta"]["n_inserted"] == 2
    ids.extend(ret['data'])

    # add with duplicates:
    ret = storage_socket.add_results([page1, page2, page3])

    assert ret["meta"]["n_inserted"] == 1
    assert len(ret['data']) == 3  # first 2 found are None
    assert len(ret["meta"]['duplicates']) == 2

    for res_id in ret['data']:
        if res_id is not None:
            ids.append(res_id)

    ret = storage_socket.del_results(ids)
    assert ret == 3
    ret = storage_socket.del_molecules(mol_insert["data"], index="id")
    assert ret == 2


### Build out a set of query tests


@pytest.fixture(scope="module")
def storage_results(storage_socket):
    # Add two waters
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")
    mol_insert = storage_socket.add_molecules([water, water2])

    page1 = {
        "molecule": mol_insert["data"][0],
        "method": "M1",
        "basis": "B1",
        "keywords": "default",
        "program": "P1",
        "driver": "energy",
        "return_result": 5,
        "hash_index": 0,
        "status": 'COMPLETE'
    }

    page2 = {
        "molecule": mol_insert["data"][1],
        "method": "M1",
        "basis": "B1",
        "keywords": "default",
        "program": "P1",
        "driver": "energy",
        "return_result": 10,
        "hash_index": 1,
        "status": 'COMPLETE'
    }

    page3 = {
        "molecule": mol_insert["data"][0],
        "method": "M1",
        "basis": "B1",
        "keywords": "default",
        "program": "P2",
        "driver": "gradient",
        "return_result": 15,
        "hash_index": 2,
        "status": 'COMPLETE'
    }

    page4 = {
        "molecule": mol_insert["data"][0],
        "method": "M2",
        "basis": "B1",
        "keywords": "default",
        "program": "P2",
        "driver": "gradient",
        "return_result": 15,
        "hash_index": 3,
        "status": 'COMPLETE'
    }

    page5 = {
        "molecule": mol_insert["data"][1],
        "method": "M2",
        "basis": "B1",
        "keywords": "default",
        "program": "P1",
        "driver": "gradient",
        "return_result": 20,
        "hash_index": 4,
        "status": 'COMPLETE'
    }

    page6 = {
        "molecule": mol_insert["data"][1],
        "method": "M3",
        "basis": "B1",
        "keywords": None,
        "program": "P1",
        "driver": "gradient",
        "return_result": 20,
        "hash_index": 5,
        "status": 'COMPLETE'
    }

    results_insert = storage_socket.add_results([page1, page2, page3, page4, page5, page6])
    assert results_insert["meta"]["n_inserted"] == 6

    yield storage_socket

    # Cleanup
    result_ids = [x for x in results_insert["data"]]
    ret = storage_socket.del_results(result_ids)
    assert ret == results_insert["meta"]["n_inserted"]

    ret = storage_socket.del_molecules(mol_insert["data"], index="id")
    assert ret == mol_insert["meta"]["n_inserted"]


def test_empty_get(storage_results):

    assert 0 == len(storage_results.get_molecules(None)["data"])
    assert 0 == len(storage_results.get_molecules([])["data"])
    assert 0 == len(storage_results.get_molecules("")["data"])
    # Todo: This needs to return top limit of the table
    assert 0 == len(storage_results.get_molecules()["data"])

    assert 6 == len(storage_results.get_results()['data'])
    # assert 1 == len(storage_results.get_results(keywords='')['data'])
    # assert 0 == len(storage_results.get_results(program='')['data'])


def test_results_query_total(storage_results):

    assert 6 == len(storage_results.get_results()["data"])


def test_get_results_by_ids(storage_results):
    results = storage_results.get_results()["data"]
    ids = [x['id'] for x in results]

    ret = storage_results.get_results_by_id(ids, return_json=False)
    assert ret["meta"]["n_found"] == 6
    assert len(ret["data"]) == 6

    ret = storage_results.get_results_by_id(ids, projection=['status'])
    assert ret['data'][0].keys() == {'id', 'status'}


def test_results_query_method(storage_results):

    ret = storage_results.get_results(method=["M2", "M1"])
    assert ret["meta"]["n_found"] == 5

    ret = storage_results.get_results(method=["M2"])
    assert ret["meta"]["n_found"] == 2

    ret = storage_results.get_results(method="M2")
    assert ret["meta"]["n_found"] == 2


def test_results_query_dual(storage_results):

    ret = storage_results.get_results(method=["M2", "M1"], program=["P1", "P2"])
    assert ret["meta"]["n_found"] == 5

    ret = storage_results.get_results(method=["M2"], program="P2")
    assert ret["meta"]["n_found"] == 1

    ret = storage_results.get_results(method="M2", program="P2")
    assert ret["meta"]["n_found"] == 1


def test_results_query_project(storage_results):
    """See new changes in design here"""

    ret = storage_results.get_results(method="M2", program="P2", projection={"return_result"})["data"][0]
    assert set(ret.keys()) == {"id", "return_result"}
    assert ret["return_result"] == 15

    # Note: explicitly set with_ids=False to remove ids
    ret = storage_results.get_results(
        method="M2", program="P2", with_ids=False, projection={"return_result"})["data"][0]
    assert set(ret.keys()) == {"return_result"}


def test_results_query_driver(storage_results):
    ret = storage_results.get_results(driver="energy")
    assert ret["meta"]["n_found"] == 2


# ------ New Task Queue tests ------
# No hash index, tasks are unique by their base_result


def test_queue_submit(storage_results):

    result1 = storage_results.get_results()['data'][0]

    task1 = {
        # "hash_index": idx,  # not used anymore
        "spec": {
            "function": "qcengine.compute_procedure",
            "args": [{
                "json_blob": "data"
            }],
            "kwargs": {},
        },
        "hooks": [("service", "x")],
        "tag": None,
        "base_result": ('results', result1['id'])
    }

    # Submit a new task
    ret = storage_results.queue_submit([task1])
    assert len(ret["data"]) == 1
    assert ret['meta']['n_inserted'] == 1

    # submit a duplicate task with a hook
    task1['hooks'] = [('service', 'y')]
    ret = storage_results.queue_submit([task1])
    assert len(ret["data"]) == 1
    assert ret['meta']['n_inserted'] == 0
    assert len(ret["meta"]['duplicates']) == 1


# ----------------------------------------------------------

# Builds tests for the queue - Changed design


def test_storage_queue_roundtrip(storage_results):

    result1 = storage_results.get_results()['data'][1]
    task1 = {
        # "hash_index": idx,
        "spec": {
            "function": "qcengine.compute_procedure",
            "args": [{
                "json_blob": "data"
            }],
            "kwargs": {},
        },
        "hooks": [("service", "")],
        "tag": None,
        "base_result": ('results', result1['id'])
    }

    # Submit a task
    r = storage_results.queue_submit([task1])
    assert len(r["data"]) == 1

    # Query for next tasks
    r = storage_results.queue_get_next("test_manager")
    assert r[0]["spec"]["function"] == task1["spec"]["function"]
    queue_id = r[0]["id"]

    # Mark task as done
    r = storage_results.queue_mark_complete([queue_id])
    assert r == 1

    # Check results
    found = storage_results.queue_get_by_id([queue_id])
    assert len(found) == 1
    assert found[0]["status"] == "COMPLETE"
    res = storage_results.get_results_by_task_id(queue_id)['data'][0]
    assert res['status'] == 'COMPLETE'

    # Check queue is empty
    r = storage_results.queue_get_next("test_manager")
    assert len(r) == 0


def test_storage_queue_duplicate(storage_results):

    result1 = storage_results.get_results()['data'][2]
    task1 = {
        # "hash_index": idx,
        "spec": {},
        "hooks": [("service", "123")],
        "tag": None,
        "base_result": ('results', result1['id'])
    }
    r = storage_results.queue_submit([task1])
    assert len(r["data"]) == 1
    queue_id = r["data"][0]

    # Put the first task in a waiting state
    r = storage_results.queue_get_next("test_manager")
    assert len(r) == 1

    # Change hooks, only one submission due to hash_index conflict
    task1["hooks"] = [("service", "456")]
    r = storage_results.queue_submit([task1])
    assert r["meta"]["n_inserted"] == 0

    # Pull out the data and check the hooks
    r = storage_results.queue_get_by_id([queue_id])
    hooks = r[0]["hooks"]
    assert len(hooks) == 2
    assert hooks[0][0] == "service"
    assert hooks[1][0] == "service"
    assert {"123", "456"} == {hooks[0][1], hooks[1][1]}

    # Cleanup
    r = storage_results.queue_mark_complete([queue_id])
    assert r == 1


def test_queue_submit_many_order(storage_results):

    results = storage_results.get_results()['data']

    task1 = {"base_result": ('results', results[3]['id'])}
    task2 = {"base_result": ('results', results[4]['id'])}
    task3 = {"base_result": ('results', results[5]['id'])}

    # Submit tasks
    ret = storage_results.queue_submit([task1, task2, task3])
    assert len(ret["data"]) == 3
    assert ret['meta']['n_inserted'] == 3

    # Get task
    r = storage_results.queue_get_next("test_manager", limit=1)
    assert len(r) == 1
    # will get the first submitted result first
    assert r[0]['base_result']['id'] == results[3]['id']

    # Todo: test more scenarios


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


def test_project_name(storage_socket):
    assert 'test' in storage_socket.get_project_name()
