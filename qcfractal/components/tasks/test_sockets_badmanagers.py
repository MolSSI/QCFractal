"""
Tests the tasks socket with respect to misbehaving managers
"""

import logging
from datetime import datetime

import pytest

from qcfractal.components.records.singlepoint.testing_helpers import load_test_data, submit_test_data
from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import caplog_handler_at_level
from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerName
from qcportal.records import RecordStatusEnum


def test_task_socket_claim_manager_noexist(storage_socket: SQLAlchemySocket):
    # Manager that doesn't exist tries to claim tasks

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    with pytest.raises(ComputeManagerError, match="does not exist") as err:
        storage_socket.tasks.claim_tasks(mname1.fullname)


def test_task_socket_claim_manager_inactive(storage_socket: SQLAlchemySocket):
    # Manager that is inactive tries to claim tasks

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    storage_socket.managers.deactivate([mname1.fullname])

    with pytest.raises(ComputeManagerError, match="is not active"):
        storage_socket.tasks.claim_tasks(mname1.fullname)


def test_task_socket_return_manager_noexist(storage_socket: SQLAlchemySocket):
    # Manager that returns data does not exist

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    with pytest.raises(ComputeManagerError, match="does not exist"):
        storage_socket.tasks.update_finished(
            "missing_manager",
            {tasks[0]["id"]: result_data},
        )

    # Task should still be running
    sp_records = storage_socket.records.get([record_id], include=["*", "task", "compute_history"])
    assert sp_records[0]["status"] == RecordStatusEnum.running
    assert sp_records[0]["manager_name"] == mname1.fullname
    assert sp_records[0]["task"] is not None
    assert sp_records[0]["compute_history"] == []


def test_task_socket_return_manager_inactive(storage_socket: SQLAlchemySocket):
    # Manager that returns data does not exist

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    storage_socket.managers.deactivate([mname1.fullname])

    with pytest.raises(ComputeManagerError, match="is not active"):
        storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data},
        )


def test_task_socket_return_wrongmanager(storage_socket: SQLAlchemySocket):
    # Manager returns data for a record that it hasn't claimed (or was stolen from it)

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="2345-6789-0123-4567")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")

    # Manager 1 claims tasks
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    # Manager 2 tries to return it
    rmeta = storage_socket.tasks.update_finished(
        mname2.fullname,
        {tasks[0]["id"]: result_data},
    )

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task is claimed by another manager"

    # But it didn't do anything
    # Task should still be running
    sp_records = storage_socket.records.get([record_id], include=["*", "task", "compute_history"])
    assert sp_records[0]["status"] == RecordStatusEnum.running
    assert sp_records[0]["manager_name"] == mname1.fullname
    assert sp_records[0]["task"] is not None
    assert sp_records[0]["compute_history"] == []

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname2.fullname])
    assert manager[0]["successes"] == 0
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 1


def test_task_socket_return_manager_badid(
    storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, caplog
):
    # Manager returns data for a record that doesn't exist

    _, _, result_data = load_test_data("sp_psi4_benzene_energy_1")

    # Should be logged
    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(activated_manager_name.fullname, {123: result_data})
        assert "does not exist in the task queue" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == 123
    assert rmeta.rejected_info[0][1] == "Task does not exist in the task queue"

    # Make sure manager info was updated
    manager = storage_socket.managers.get([activated_manager_name.fullname])
    assert manager[0]["successes"] == 0
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 1


def test_task_socket_return_manager_badstatus_1(storage_socket: SQLAlchemySocket, caplog):
    # Manager returns data for a record that is not running

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    storage_socket.records.reset([record_id])

    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data},
        )
        assert "not in a running state" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task is not in a running state"

    # Record should still be waiting
    sp_records = storage_socket.records.get([record_id], include=["*", "task", "compute_history"])
    assert sp_records[0]["status"] == RecordStatusEnum.waiting
    assert sp_records[0]["manager_name"] is None
    assert sp_records[0]["task"] is not None
    assert sp_records[0]["compute_history"] == []

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 0
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 1


def test_task_socket_return_manager_badstatus_2(storage_socket: SQLAlchemySocket, caplog):
    # Manager returns data for a record that completed (and therefore not in the task queue)

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    storage_socket.tasks.update_finished(
        mname1.fullname,
        {tasks[0]["id"]: result_data},
    )

    time_1 = datetime.utcnow()

    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data},
        )
        assert "does not exist in the task queue" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task does not exist in the task queue"

    # Record should be complete
    sp_records = storage_socket.records.get([record_id], include=["*", "task", "compute_history"])
    assert sp_records[0]["status"] == RecordStatusEnum.complete
    assert sp_records[0]["manager_name"] == mname1.fullname
    assert sp_records[0]["task"] is None
    assert len(sp_records[0]["compute_history"]) == 1
    assert sp_records[0]["modified_on"] < time_1

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 1  # from the first submission
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 1


def test_task_socket_return_manager_badstatus_3(storage_socket: SQLAlchemySocket, caplog):
    # Manager returns data for a record that is cancelled

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    storage_socket.records.cancel([record_id])

    time_1 = datetime.utcnow()

    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data},
        )
        assert "does not exist in the task queue" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task does not exist in the task queue"

    # Record should be cancelled
    sp_records = storage_socket.records.get([record_id], include=["*", "task", "compute_history"])
    assert sp_records[0]["status"] == RecordStatusEnum.cancelled
    assert sp_records[0]["manager_name"] is None
    assert sp_records[0]["task"] is None
    assert len(sp_records[0]["compute_history"]) == 0
    assert sp_records[0]["modified_on"] < time_1

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 0
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 1
