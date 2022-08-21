"""
Tests the tasks socket (claiming & returning data)
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from qcfractal.components.records.optimization.testing_helpers import load_test_data as load_opt_test_data
from qcfractal.components.records.singlepoint.testing_helpers import load_test_data as load_sp_test_data
from qcportal.managers import ManagerName
from qcportal.records import PriorityEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

input_spec_1, molecule_1, result_data_1 = load_sp_test_data("sp_psi4_water_energy")
input_spec_2, molecule_2, result_data_2 = load_sp_test_data("sp_psi4_water_gradient")
input_spec_3, molecule_3, result_data_3 = load_sp_test_data("sp_psi4_water_hessian")
input_spec_4, molecule_4, result_data_4 = load_opt_test_data("opt_psi4_benzene")
input_spec_5, molecule_5, result_data_5 = load_sp_test_data("sp_psi4_benzene_energy_1")
input_spec_6, molecule_6, result_data_6 = load_sp_test_data("sp_psi4_benzene_energy_2")
input_spec_7, molecule_7, result_data_7 = load_sp_test_data("sp_rdkit_benzene_energy")


def test_task_socket_claim_mixed(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host2", uuid="2234-5678-1234-5678")
    mname3 = ManagerName(cluster="test_cluster", hostname="a_host3", uuid="3234-5678-1234-5678")
    mname4 = ManagerName(cluster="test_cluster", hostname="a_host4", uuid="4234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0"},
        tags=["tag1"],
    )
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0"},
        tags=["*"],
    )
    storage_socket.managers.activate(
        name_data=mname3,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0"},
        tags=["tag3", "tag1"],
    )
    storage_socket.managers.activate(
        name_data=mname4,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0", "rdkit": "v1.0"},
        tags=["tag3", "*"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.low)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.normal)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "tag1", PriorityEnum.high)
    meta, id_4 = storage_socket.records.optimization.add([molecule_4], input_spec_4, "tag3", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag1", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag6", PriorityEnum.high)
    meta, id_7 = storage_socket.records.singlepoint.add([molecule_7], input_spec_7, "tag1", PriorityEnum.high)

    all_id = id_1 + id_2 + id_3 + id_4 + id_5 + id_6 + id_7
    recs = storage_socket.records.get(all_id, include=["*", "task"])

    # claim up to two tasks
    # should find the high and normal priority one, but not the one
    # requiring rdkit
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[2]["task"]["id"]
    assert tasks[1]["id"] == recs[4]["task"]["id"]

    # manager 4 should find tag3, and then #6 (highest priority left)
    tasks = storage_socket.tasks.claim_tasks(mname4.fullname, 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[3]["task"]["id"]
    assert tasks[1]["id"] == recs[5]["task"]["id"]

    # manager3 should find the only remaining tag1 that isn't rdkit
    tasks = storage_socket.tasks.claim_tasks(mname3.fullname, 2)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[0]["task"]["id"]

    # manager 2 only finds #2 - doesn't have rdkit
    tasks = storage_socket.tasks.claim_tasks(mname2.fullname, 20)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[1]["task"]["id"]

    # manager 4 can finally get the last one
    tasks = storage_socket.tasks.claim_tasks(mname4.fullname, 20)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[6]["task"]["id"]

    # Check assignments
    recs = storage_socket.records.get(all_id, include=["*", "task"])
    assert recs[0]["manager_name"] == mname3.fullname
    assert recs[1]["manager_name"] == mname2.fullname
    assert recs[2]["manager_name"] == mname1.fullname
    assert recs[3]["manager_name"] == mname4.fullname
    assert recs[4]["manager_name"] == mname1.fullname
    assert recs[5]["manager_name"] == mname4.fullname
    assert recs[6]["manager_name"] == mname4.fullname

    managers = storage_socket.managers.get([mname1.fullname, mname2.fullname, mname3.fullname, mname4.fullname])
    assert managers[0]["claimed"] == 2
    assert managers[1]["claimed"] == 1
    assert managers[2]["claimed"] == 1
    assert managers[3]["claimed"] == 3


def test_task_socket_claim_priority(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "Psi4": None, "geometric": "v3.0"},
        tags=["tag1"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.low)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag1", PriorityEnum.normal)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "tag1", PriorityEnum.high)
    meta, id_4 = storage_socket.records.optimization.add([molecule_4], input_spec_4, "tag1", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag1", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag1", PriorityEnum.high)

    all_id = id_1 + id_2 + id_3 + id_4 + id_5 + id_6
    recs = storage_socket.records.get(all_id, include=["*", "task"])

    # highest priority should be first, then by modified date
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 3)
    assert len(tasks) == 3
    assert tasks[0]["id"] == recs[2]["task"]["id"]
    assert tasks[1]["id"] == recs[5]["task"]["id"]
    assert tasks[2]["id"] == recs[1]["task"]["id"]

    # Now normal then low
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 3)
    assert len(tasks) == 3
    assert tasks[0]["id"] == recs[3]["task"]["id"]
    assert tasks[1]["id"] == recs[4]["task"]["id"]
    assert tasks[2]["id"] == recs[0]["task"]["id"]


def test_task_socket_claim_tag(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0"},
        tags=["tag3", "tag1"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "TAg1", PriorityEnum.normal)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.normal)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "*", PriorityEnum.normal)
    meta, id_4 = storage_socket.records.optimization.add([molecule_4], input_spec_4, "tag3", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag1", PriorityEnum.normal)

    all_id = id_1 + id_2 + id_3 + id_4 + id_5
    recs = storage_socket.records.get(all_id, include=["*", "task"])

    # tag3 should be first, then tag1
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[3]["task"]["id"]
    assert tasks[1]["id"] == recs[0]["task"]["id"]

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 3)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[4]["task"]["id"]


def test_task_socket_claim_tag_wildcard(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0"},
        tags=["TAG3", "*"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.normal)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.normal)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "*", PriorityEnum.normal)
    meta, id_4 = storage_socket.records.optimization.add([molecule_4], input_spec_4, "taG3", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag1", PriorityEnum.normal)

    all_id = id_1 + id_2 + id_3 + id_4 + id_5
    recs = storage_socket.records.get(all_id, include=["*", "task"])

    # tag3 should be first, then any task (in order)
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[3]["task"]["id"]
    assert tasks[1]["id"] == recs[0]["task"]["id"]

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 3)
    assert len(tasks) == 3
    assert tasks[0]["id"] == recs[1]["task"]["id"]
    assert tasks[1]["id"] == recs[2]["task"]["id"]
    assert tasks[2]["id"] == recs[4]["task"]["id"]


def test_task_socket_claim_program(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "geometric": "v3.0"},
        tags=["*"],
    )

    meta, id_7 = storage_socket.records.singlepoint.add([molecule_7], input_spec_7, "tag1", PriorityEnum.normal)
    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.normal)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag1", PriorityEnum.normal)

    all_id = id_7 + id_1 + id_2
    recs = storage_socket.records.get(all_id, include=["*", "task"])

    # claim all tasks. But it should claim #7
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, 100)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[1]["task"]["id"]
    assert tasks[1]["id"] == recs[2]["task"]["id"]
