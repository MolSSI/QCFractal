"""
Tests the tasks socket with respect to misbehaving managers
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import caplog_handler_at_level
from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_test_data, submit_test_data
from qcfractalcompute.compress import compress_result
from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerName
from qcportal.record_models import RecordStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session

_manager_programs = {"qcengine": ["unknown"], "psi4": ["unknown"], "qchem": ["v3.0"]}


def test_task_socket_claim_manager_noexist(storage_socket: SQLAlchemySocket):
    # Manager that doesn't exist tries to claim tasks

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    with pytest.raises(ComputeManagerError, match="does not exist") as err:
        storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])


def test_task_socket_claim_manager_inactive(storage_socket: SQLAlchemySocket):
    # Manager that is inactive tries to claim tasks

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    storage_socket.managers.deactivate([mname1.fullname])

    with pytest.raises(ComputeManagerError, match="is not active"):
        storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])


def test_task_socket_return_manager_noexist(storage_socket: SQLAlchemySocket, session: Session):
    # Manager that returns data does not exist

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    result_data_compressed = compress_result(result_data.dict())

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])

    with pytest.raises(ComputeManagerError, match="does not exist"):
        storage_socket.tasks.update_finished(
            "missing_manager",
            {tasks[0]["id"]: result_data_compressed},
        )

    # Task should still be running
    sp_rec = session.get(BaseRecordORM, record_id)
    assert sp_rec.status == RecordStatusEnum.running
    assert sp_rec.manager_name == mname1.fullname
    assert sp_rec.task is not None
    assert sp_rec.compute_history == []


def test_task_socket_return_manager_inactive(storage_socket: SQLAlchemySocket):
    # Manager that returns data does not exist

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    result_data_compressed = compress_result(result_data.dict())

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])

    storage_socket.managers.deactivate([mname1.fullname])

    with pytest.raises(ComputeManagerError, match="is not active"):
        storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data_compressed},
        )


def test_task_socket_return_wrongmanager(storage_socket: SQLAlchemySocket, session: Session):
    # Manager returns data for a record that it hasn't claimed (or was stolen from it)

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="2345-6789-0123-4567")
    mid_1 = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "qchem": ["v3.0"]},
        compute_tags=["tag1"],
    )

    mid_2 = storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    result_data_compressed = compress_result(result_data.dict())

    # Manager 1 claims tasks
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])

    # Manager 2 tries to return it
    rmeta = storage_socket.tasks.update_finished(
        mname2.fullname,
        {tasks[0]["id"]: result_data_compressed},
    )

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task is claimed by another manager"

    # But it didn't do anything
    # Task should still be running
    sp_rec = session.get(BaseRecordORM, record_id)
    assert sp_rec.status == RecordStatusEnum.running
    assert sp_rec.manager_name == mname1.fullname
    assert sp_rec.task is not None
    assert sp_rec.compute_history == []

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid_2)
    assert manager.successes == 0
    assert manager.failures == 0
    assert manager.rejected == 1


def test_task_socket_return_manager_badid(snowflake: QCATestingSnowflake, caplog):
    storage_socket = snowflake.get_storage_socket()

    mname, mid = snowflake.activate_manager()

    # Manager returns data for a record that doesn't exist

    _, _, result_data = load_test_data("sp_psi4_benzene_energy_1")
    result_data_compressed = compress_result(result_data.dict())

    # Should be logged
    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(mname.fullname, {123: result_data_compressed})
        assert "does not exist in the task queue" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == 123
    assert rmeta.rejected_info[0][1] == "Task does not exist in the task queue"

    # Make sure manager info was updated
    with storage_socket.session_scope() as session:
        manager = session.get(ComputeManagerORM, mid)
        assert manager.successes == 0
        assert manager.failures == 0
        assert manager.rejected == 1


def test_task_socket_return_manager_badstatus_1(storage_socket: SQLAlchemySocket, session: Session, caplog):
    # Manager returns data for a record that is not running

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mid = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    result_data_compressed = compress_result(result_data.dict())

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])

    storage_socket.records.reset_running([record_id])

    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: result_data_compressed})
        assert "not in a running state" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task is not in a running state"

    # Record should still be waiting
    sp_rec = session.get(BaseRecordORM, record_id)
    assert sp_rec.status == RecordStatusEnum.waiting
    assert sp_rec.manager_name is None
    assert sp_rec.task is not None
    assert sp_rec.compute_history == []

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid)
    assert manager.successes == 0
    assert manager.failures == 0
    assert manager.rejected == 1


def test_task_socket_return_manager_badstatus_2(storage_socket: SQLAlchemySocket, session: Session, caplog):
    # Manager returns data for a record that completed (and therefore not in the task queue)

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mid = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    result_data_compressed = compress_result(result_data.dict())

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])

    storage_socket.tasks.update_finished(
        mname1.fullname,
        {tasks[0]["id"]: result_data_compressed},
    )

    time_1 = now_at_utc()

    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data_compressed},
        )
        assert "does not exist in the task queue" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task does not exist in the task queue"

    # Record should be complete
    sp_rec = session.get(BaseRecordORM, record_id)
    assert sp_rec.status == RecordStatusEnum.complete
    assert sp_rec.manager_name == mname1.fullname
    assert sp_rec.task is None
    assert len(sp_rec.compute_history) == 1
    assert sp_rec.modified_on < time_1

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid)
    assert manager.successes == 1  # from the first submission
    assert manager.failures == 0
    assert manager.rejected == 1


def test_task_socket_return_manager_badstatus_3(storage_socket: SQLAlchemySocket, session: Session, caplog):
    # Manager returns data for a record that is cancelled

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mid = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=_manager_programs,
        compute_tags=["tag1"],
    )

    record_id, result_data = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    result_data_compressed = compress_result(result_data.dict())

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, _manager_programs, ["tag1"])

    storage_socket.records.cancel([record_id])

    time_1 = now_at_utc()

    with caplog_handler_at_level(caplog, logging.WARNING):
        rmeta = storage_socket.tasks.update_finished(
            mname1.fullname,
            {tasks[0]["id"]: result_data_compressed},
        )
        assert "does not exist in the task queue" in caplog.text

    assert rmeta.n_accepted == 0
    assert rmeta.n_rejected == 1
    assert rmeta.rejected_info[0][0] == tasks[0]["id"]
    assert rmeta.rejected_info[0][1] == "Task does not exist in the task queue"

    # Record should be cancelled
    sp_rec = session.get(BaseRecordORM, record_id)
    assert sp_rec.status == RecordStatusEnum.cancelled
    assert sp_rec.manager_name is None
    assert sp_rec.task is None
    assert len(sp_rec.compute_history) == 0
    assert sp_rec.modified_on < time_1

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid)
    assert manager.successes == 0
    assert manager.failures == 0
    assert manager.rejected == 1
