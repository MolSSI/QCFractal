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
from qcfractal.services.services import TorsionDriveService

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

    ret = storage_socket.add_collection(db.copy())
    # print(ret["meta"]["error_description"])
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
    # Can't delete molecule when datasets reference it (no cascade)
    ret = storage_socket.molecule.delete(mol_insert)
    assert not ret.success
    assert ret.n_errors == 2
    for e in ret.errors:
        assert "Attempting to delete resulted in error" in e[1]

    # should cascade delete entries and records when dataset is deleted
    assert storage_socket.del_collection(collection=collection, name=name) == 1
    assert storage_socket.del_collection(collection=collection2, name=name2) == 1

    # Now okay to delete molecules
    ret = storage_socket.molecule.delete(mol_insert)
    assert ret.success


# ------ New Task Queue tests ------
# No hash index, tasks are unique by their base_result


# def test_queue_submit_sql(storage_results):
#
#    result1 = storage_results.get_results()["data"][0]
#
#    task1 = ptl.models.TaskRecord(
#        **{
#            # "hash_index": idx,  # not used anymore
#            "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
#            "tag": None,
#            "program": "p1",
#            "parser": "",
#            "base_result": result1["id"],
#        }
#    )
#
#    # Submit a new task
#    ret = storage_results.task_queue.add([task1])
#    assert len(ret["data"]) == 1
#    assert ret["meta"]["n_inserted"] == 1
#
#    # submit a duplicate task with a hook
#    ret = storage_results.task_queue.add([task1])
#    assert len(ret["data"]) == 1
#    assert ret["meta"]["n_inserted"] == 0
#    assert len(ret["meta"]["duplicates"]) == 1
#
#    result2 = storage_results.get_results()["data"][1]
#
#    task2 = ptl.models.TaskRecord(
#        **{
#            "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
#            "tag": None,
#            "program": "p1",
#            "parser": "",
#            "base_result": result2["id"],
#        }
#    )
#
#    # submit repeated tasks
#    ret = storage_results.task_queue.add([task2, task2])
#    assert len(ret["data"]) == 2
#    assert ret["meta"]["n_inserted"] == 1
#    assert ret["data"][0] == ret["data"][1]
#
#
## ----------------------------------------------------------
#
## Builds tests for the queue - Changed design
#
#
# @pytest.mark.parametrize("status", ["COMPLETE", "ERROR"])
# def test_storage_queue_roundtrip(storage_results, status):
#
#    results = storage_results.get_results()["data"]
#
#    task_template = {
#        "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
#        "tag": None,
#        "program": "P1",
#        "procedure": "P1",
#        "parser": "",
#        "base_result": None,
#    }
#
#    task_template["base_result"] = results[0]["id"]
#    task1 = ptl.models.TaskRecord(**task_template)
#    task_template["base_result"] = results[1]["id"]
#    task2 = ptl.models.TaskRecord(**task_template)
#
#    # Submit a task
#    r = storage_results.task_queue.add([task1, task2])
#    assert len(r["data"]) == 2
#
#    # Add manager 'test_manager'
#    storage_results.manager.update("test_manager", status="ACTIVE")
#    storage_results.manager.update("test_manager2", status="ACTIVE")
#    # Query for next tasks
#    r = storage_results.task_queue.claim("test_manager", ["p1"], ["p1"], limit=1)
#    assert r[0].spec.function == task1.spec.function
#    queue_id = r[0].id
#
#    queue_id2 = storage_results.task_queue.claim("test_manager2", ["p1"], ["p1"], limit=1)[0].id
#
#    if status == "ERROR":
#        r = storage_results.task_queue.mark_error([queue_id, queue_id2])
#    elif status == "COMPLETE":
#        r = storage_results.task_queue.mark_complete([queue_id2, queue_id])
#        # Check queue is empty
#        tasks = storage_results.task_queue.claim("test_manager", ["p1"], ["p1"])
#        assert len(tasks) == 0
#
#        # completed task should be deleted
#        found = storage_results.task_queue.get([queue_id, queue_id2], missing_ok=True)
#        assert len(found) == 0
#
#    assert r == 2
#
#    # Check results
#    # TODO: We no longer change base results through queue_mark_*. So we should remove this?
#    # res = storage_results.get_results(id=results[0]["id"])["data"][0]
#    # assert res["status"] == status
#
#
# def test_queue_submit_many_order(storage_results):
#
#    results = storage_results.get_results()["data"]
#
#    task_template = {
#        # "hash_index": idx,
#        "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
#        "tag": None,
#        "program": "P1",
#        "procedure": "P1",
#        "parser": "",
#    }
#
#    task1 = ptl.models.TaskRecord(**task_template, base_result=results[3]["id"])
#    task2 = ptl.models.TaskRecord(**task_template, base_result=results[4]["id"])
#    task3 = ptl.models.TaskRecord(**task_template, base_result=results[5]["id"])
#
#    # Submit tasks
#    ret = storage_results.task_queue.add([task1, task2, task3])
#    assert len(ret["data"]) == 3
#    assert ret["meta"]["n_inserted"] == 3
#
#    # Add a manager
#    storage_results.manager.update("test_manager")
#
#    # Get tasks for manager 'test_manager'
#    r = storage_results.task_queue.claim("test_manager", ["p1"], ["p1"], limit=1)
#    assert len(r) == 1
#    # will get the first submitted result first
#    assert r[0].base_result == results[3]["id"]
#
#    # Todo: test more scenarios
#
#
# def test_manager(storage_socket):
#
#    assert storage_socket.manager.update(name="first_manager")
#    assert storage_socket.manager.update(name="first_manager", submitted=100)
#    assert storage_socket.manager.update(name="first_manager", submitted=50)
#
#    ret = storage_socket.manager.get(name=["first_manager"])
#    assert ret[0]["submitted"] == 150
#
#    meta, ret2 = storage_socket.manager.query(name=["first_manager"], modified_before=datetime.utcnow())
#    assert meta.n_returned == 1
#    assert len(ret2) == 1
#
#    assert ret2[0] == ret[0]


# def test_procedure_sql(storage_socket):
#
#    _, mols = storage_socket.molecule.query()
#    mol_ids = [mol["id"] for mol in mols]
#    results = storage_socket.get_results()["data"]
#
#    assert len(storage_socket.get_procedures(procedure="optimization", status=None)["data"]) == 0
#
#    proc_template = {
#        "procedure": "optimization",
#        "initial_molecule": mol_ids[0],
#        "program": "something",
#        "hash_index": 123,
#        # "trajectory": None,
#        "trajectory": [results[0]["id"], results[1]["id"]],
#        "qc_spec": {
#            "driver": "gradient",
#            "method": "HF",
#            "basis": "sto-3g",
#            # "keywords": None,
#            "program": "psi4",
#        },
#    }
#
#    # Optimization
#    inserted = storage_socket.add_procedures([ptl.models.OptimizationRecord(**proc_template)])
#    assert inserted["meta"]["n_inserted"] == 1
#
#    ret = storage_socket.get_procedures(procedure="optimization", status=None)
#    assert len(ret["data"]) == 1
#    # assert ret['data'][0]['trajectory'] == [str(i) for i in proc_template['trajectory']]
#    assert ret["data"][0]["trajectory"] == proc_template["trajectory"]
#
#    new_proc = ret["data"][0]
#
#    test_traj = [
#        [results[0]["id"], results[1]["id"], results[2]["id"]],  # add
#        # [results[0]['id']],  # remove
#        # [results[0]['id']],  # no change
#        # None  # empty
#    ]
#    # update relations
#    for trajectory in test_traj:
#        new_proc["trajectory"] = trajectory
#        ret_count = storage_socket.update_procedures([ptl.models.OptimizationRecord(**new_proc)])
#        assert ret_count == 1
#
#        ret = storage_socket.get_procedures(procedure="optimization", status=None)
#        assert len(ret["data"]) == 1
#        assert ret["data"][0]["trajectory"] == trajectory
#
#        opt_proc = ret["data"][0]
#
#    # Torsiondrive procedures
#    assert len(storage_socket.get_procedures(procedure="torsiondrive", status=None)["data"]) == 0
#
#    torsion_proc = {
#        "procedure": "torsiondrive",
#        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10]},
#        "hash_index": 456,
#        "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
#        "qc_spec": {
#            "driver": "gradient",
#            "method": "HF",
#            "basis": "sto-3g",
#            # "keywords": None,
#            "program": "psi4",
#        },
#        "initial_molecule": [mol_ids[0], mol_ids[1]],
#        "final_energy_dict": {},
#        "optimization_history": {},
#        "minimum_positions": {},
#        "provenance": {"creator": ""},
#    }
#
#    # Torsiondrive init molecule many to many
#    inserted2 = storage_socket.add_procedures([ptl.models.TorsionDriveRecord(**torsion_proc)])
#    assert inserted2["meta"]["n_inserted"] == 1
#
#    ret = storage_socket.get_procedures(procedure="torsiondrive", status=None)
#    assert len(ret["data"]) == 1
#    torsion = ret["data"][0]
#
#    init_mol_tests = [[mol_ids[0]], [mol_ids[0], mol_ids[2], mol_ids[3]]]  # del one
#
#    for init_mol in init_mol_tests:
#        torsion["initial_molecule"] = init_mol
#        ret = storage_socket.update_procedures([ptl.models.TorsionDriveRecord(**torsion)])
#        assert ret == 1
#        ret = storage_socket.get_procedures(procedure="torsiondrive", status=None)
#        assert set(ret["data"][0]["initial_molecule"]) == set([str(i) for i in init_mol])
#
#    # optimization history
#    opt_hist_tests = [
#        {"90": [opt_proc["id"]]},  # add one
#        {"90": [opt_proc["id"]], "44": [opt_proc["id"]]},
#        {"5": [opt_proc["id"]]},
#    ]
#
#    for opt_hist in opt_hist_tests:
#        torsion["optimization_history"] = opt_hist
#        ret = storage_socket.update_procedures([ptl.models.TorsionDriveRecord(**torsion)])
#        assert ret == 1
#        ret = storage_socket.get_procedures(procedure="torsiondrive", status=None)
#        assert ret["data"][0]["optimization_history"] == opt_hist
#
#    # clean up
#    storage_socket.del_procedures(inserted["data"])
#    storage_socket.del_procedures(inserted2["data"])
#
#
# def test_services_sql(storage_socket):
#
#    _, mols = storage_socket.molecule.query()
#    mol_ids = [mol["id"] for mol in mols]
#
#    torsion_proc = {
#        "procedure": "torsiondrive",
#        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10]},
#        "hash_index": 456,
#        "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
#        "qc_spec": {
#            "driver": "gradient",
#            "method": "HF",
#            "basis": "sto-3g",
#            # "keywords": None,
#            "program": "psi4",
#        },
#        "initial_molecule": [mol_ids[0], mol_ids[1]],
#        "final_energy_dict": {},
#        "optimization_history": {},
#        "minimum_positions": {},
#        "provenance": {"creator": ""},
#    }
#
#    # Procedure
#    proc_pydantic = ptl.models.TorsionDriveRecord(**torsion_proc)
#
#    service_data = {
#        "tag": "tag1 tag2",
#        "hash_index": "123",
#        "status": RecordStatusEnum.waiting,
#        "optimization_program": "gaussian",
#        # extra fields
#        "torsiondrive_state": {},
#        "dihedral_template": "1",
#        "optimization_template": "2",
#        "molecule_template": "",
#        "storage_socket": storage_socket,
#        "task_priority": 0,
#        "output": proc_pydantic,
#    }
#
#    service = TorsionDriveService(**service_data)
#    ret = storage_socket.add_services([service])
#    assert len(ret["data"]) == 1
#
#    ret = storage_socket.get_services(procedure_id=ret["data"][0], status=RecordStatusEnum.waiting)
#    assert ret["data"][0]["hash_index"] == service_data["hash_index"]
#
#    # attributes in extra fields
#    assert ret["data"][0]["dihedral_template"] == service_data["dihedral_template"]
#
#    # Create Pydantic object from DB returned object
#    py_obj = TorsionDriveService(**ret["data"][0], storage_socket=storage_socket)
#    assert py_obj
#
#    # Test update
#    py_obj.task_priority = 3
#    ret_count = storage_socket.update_services([py_obj])
#    assert ret_count == 1
#
#    ret = storage_socket.get_services(procedure_id=ret["data"][0]["procedure_id"], status=RecordStatusEnum.waiting)
#    assert ret["data"][0]["task_priority"] == py_obj.task_priority


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
