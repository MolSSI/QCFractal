"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import pytest

import qcfractal.interface as ptl

from .test_procedure import load_procedure_data


# @pytest.fixture(scope="function")
# def storage_single_results(storage_socket):
#
#    proc_inputs = [
#        "single_psi4_energy_1",
#        "single_psi4_energy_2",
#        "single_rdkit_energy_1",
#        "single_rdkit_error_1",
#    ]
#
#    ids = []
#    for proc in proc_inputs:
#        inp, mol, _ = load_procedure_data(proc)
#        _, i = storage_socket.procedure.create([mol], inp)
#        ids.extend(i)
#
#    yield ids, storage_socket
#
#
# def test_results_get(storage_single_results):
#
#    ids, storage_socket = storage_single_results
#    print(ids)
#    results = storage_socket.procedure.single.get(ids)
#    print(results)
#    return
#
#    # Add two waters
#    water = ptl.data.get_molecule("water_dimer_minima.psimol")
#    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")
#    _, mol_insert = storage_socket.molecule.add([water, water2])
#
#    kw1 = ptl.models.KeywordSet(**{"comments": "a", "values": {}})
#    kwid1 = storage_socket.keywords.add([kw1])[1][0]
#
#    page1 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[0],
#            "method": "M1",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P1",
#            "driver": "energy",
#            # "extras": {
#            #     "other_data": 5
#            # },
#            "hash_index": 0,
#        }
#    )
#
#    page2 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[1],
#            "method": "M1",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P1",
#            "driver": "energy",
#            # "extras": {
#            #     "other_data": 10
#            # },
#            "hash_index": 1,
#        }
#    )
#
#    page3 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[1],
#            "method": "M22",
#            "basis": "B1",
#            "keywords": None,
#            "program": "P1",
#            "driver": "energy",
#            # "extras": {
#            #     "other_data": 10
#            # },
#            "hash_index": 2,
#        }
#    )
#
#    ids = []
#    ret = storage_socket.add_results([page1, page2])
#    assert ret["meta"]["n_inserted"] == 2
#    ids.extend(ret["data"])
#
#    # add with duplicates:
#    ret = storage_socket.add_results([page1, page2, page3])
#
#    assert ret["meta"]["n_inserted"] == 1
#    assert len(ret["data"]) == 3  # first 2 found are None
#    assert len(ret["meta"]["duplicates"]) == 2
#
#    for res_id in ret["data"]:
#        if res_id is not None:
#            ids.append(res_id)
#
#    ret = storage_socket.del_results(ids)
#    assert ret == 3
#    ret = storage_socket.molecule.delete(mol_insert)
#    assert ret.n_deleted == 2


#### Build out a set of query tests
#
#
# @pytest.fixture(scope="function")
# def storage_results(storage_socket):
#    # Add two waters
#
#    mol_names = [
#        "water_dimer_minima.psimol",
#        "water_dimer_stretch.psimol",
#        "water_dimer_stretch2.psimol",
#        "neon_tetramer.psimol",
#    ]
#
#    molecules = []
#    for mol_name in mol_names:
#        mol = ptl.data.get_molecule(mol_name)
#        molecules.append(mol)
#
#    meta, mol_insert = storage_socket.molecule.add(molecules)
#    assert meta.success
#
#    kw1 = ptl.models.KeywordSet(**{"values": {}})
#    _, kwids = storage_socket.keywords.add([kw1])
#    kwid1 = kwids[0]
#
#    page1 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[0],
#            "method": "M1",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P1",
#            "driver": "energy",
#            "return_result": 5,
#            "hash_index": 0,
#            "status": "COMPLETE",
#        }
#    )
#
#    page2 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[1],
#            "method": "M1",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P1",
#            "driver": "energy",
#            "return_result": 10,
#            "hash_index": 1,
#            "status": "COMPLETE",
#        }
#    )
#
#    page3 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[0],
#            "method": "M1",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P2",
#            "driver": "gradient",
#            "return_result": 15,
#            "hash_index": 2,
#            "status": "COMPLETE",
#        }
#    )
#
#    page4 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[0],
#            "method": "M2",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P2",
#            "driver": "gradient",
#            "return_result": 15,
#            "hash_index": 3,
#            "status": "COMPLETE",
#        }
#    )
#
#    page5 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[1],
#            "method": "M2",
#            "basis": "B1",
#            "keywords": kwid1,
#            "program": "P1",
#            "driver": "gradient",
#            "return_result": 20,
#            "hash_index": 4,
#            "status": "COMPLETE",
#        }
#    )
#
#    page6 = ptl.models.ResultRecord(
#        **{
#            "molecule": mol_insert[1],
#            "method": "M3",
#            "basis": "B1",
#            "keywords": None,
#            "program": "P1",
#            "driver": "gradient",
#            "return_result": 20,
#            "hash_index": 5,
#            "status": "COMPLETE",
#        }
#    )
#
#    results_insert = storage_socket.add_results([page1, page2, page3, page4, page5, page6])
#    assert results_insert["meta"]["n_inserted"] == 6
#
#    yield storage_socket
#
#    # Cleanup
#    all_tasks = storage_socket.get_queue()["data"]
#    storage_socket.del_tasks(id=[task.id for task in all_tasks])
#
#    result_ids = [x for x in results_insert["data"]]
#    ret = storage_socket.del_results(result_ids)
#    assert ret == results_insert["meta"]["n_inserted"]
#
#    ret = storage_socket.molecule.delete(mol_insert)
#    assert ret.n_deleted == len(mol_insert)
#
#
# def test_empty_get(storage_results):
#
#    assert 0 == len(storage_results.molecule.query(id=[])[1])
#    assert 0 == len(storage_results.molecule.query(id=[bad_id1])[1])
#    assert 4 == len(storage_results.molecule.query()[1])
#
#    assert 6 == len(storage_results.get_results()["data"])
#    assert 1 == len(storage_results.get_results(keywords="null")["data"])
#    assert 0 == len(storage_results.get_results(program="null")["data"])
#
#
# def test_results_get_total(storage_results):
#
#    assert 6 == len(storage_results.get_results()["data"])
#
#
# def test_results_get_0(storage_results):
#    assert 0 == len(storage_results.get_results(limit=0)["data"])
#
#
# def test_get_results_by_ids(storage_results):
#    results = storage_results.get_results()["data"]
#    ids = [x["id"] for x in results]
#
#    ret = storage_results.get_results(id=ids, return_json=False)
#    assert ret["meta"]["n_found"] == 6
#    assert len(ret["data"]) == 6
#
#    ret = storage_results.get_results(id=ids, include=["status", "id"])
#    assert ret["data"][0].keys() == {"id", "status"}
#
#
# def test_results_get_method(storage_results):
#
#    ret = storage_results.get_results(method=["M2", "M1"])
#    assert ret["meta"]["n_found"] == 5
#
#    ret = storage_results.get_results(method=["M2"])
#    assert ret["meta"]["n_found"] == 2
#
#    ret = storage_results.get_results(method="M2")
#    assert ret["meta"]["n_found"] == 2
#
#
# def test_results_get_dual(storage_results):
#
#    ret = storage_results.get_results(method=["M2", "M1"], program=["P1", "P2"])
#    assert ret["meta"]["n_found"] == 5
#
#    ret = storage_results.get_results(method=["M2"], program="P2")
#    assert ret["meta"]["n_found"] == 1
#
#    ret = storage_results.get_results(method="M2", program="P2")
#    assert ret["meta"]["n_found"] == 1
#
#
# def test_results_get_project(storage_results):
#    """See new changes in design here"""
#
#    ret_true = storage_results.get_results(method="M2", program="P2", include=["return_result", "id"])["data"][0]
#    assert set(ret_true.keys()) == {"id", "return_result"}
#    assert ret_true["return_result"] == 15
#
#    # Note: explicitly set with_ids=False to remove ids
#    ret = storage_results.get_results(method="M2", program="P2", with_ids=False, include=["return_result"])["data"][0]
#    assert set(ret.keys()) == {"return_result"}
#
#
# def test_results_get_driver(storage_results):
#    ret = storage_results.get_results(driver="energy")
#    assert ret["meta"]["n_found"] == 2
