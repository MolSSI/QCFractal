"""
Tests the tasks socket (claiming & returning data)
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Tuple

from qcelemental.models import ComputeError, FailedOperation

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_test_data, submit_test_data
from qcportal.managers import ManagerName
from qcportal.outputstore import OutputTypeEnum
from qcportal.record_models import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


def test_task_socket_fullworkflow_success(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager: Tuple[ManagerName, int]
):
    id1, result_data1 = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1", PriorityEnum.normal)
    id2, result_data2 = submit_test_data(storage_socket, "sp_psi4_fluoroethane_wfn", "tag1", PriorityEnum.normal)

    mname, mid = activated_manager

    time_1 = datetime.utcnow()

    result_map = {id1: result_data1, id2: result_data2}

    tasks = storage_socket.tasks.claim_tasks(mname.fullname)

    # Should be claimed in the manager table
    manager = session.get(ComputeManagerORM, mid)
    assert manager.claimed == 2

    # Status should be updated
    for rec_id in [id1, id2]:
        rec = session.get(BaseRecordORM, rec_id)
        assert rec.status == RecordStatusEnum.running
        assert rec.manager_name == mname.fullname
        assert rec.task is not None
        assert rec.compute_history == []
        assert rec.modified_on > time_1
        assert rec.created_on < time_1

    rmeta = storage_socket.tasks.update_finished(
        mname.fullname,
        {tasks[0]["id"]: result_map[tasks[0]["record_id"]], tasks[1]["id"]: result_map[tasks[1]["record_id"]]},
    )

    assert rmeta.n_accepted == 2
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == sorted([id1, id2])

    session.expire_all()
    for rec_id in [id1, id2]:
        rec = session.get(BaseRecordORM, rec_id)
        # Status should be complete
        assert rec.status == RecordStatusEnum.complete

        # Tasks should be deleted
        assert rec.task is None

        # Manager names should have been assigned
        assert rec.manager_name == mname.fullname

        # Modified_on should be updated, but not created_on
        assert rec.created_on < time_1
        assert rec.modified_on > time_1

        # History should have been saved
        assert len(rec.compute_history) == 1
        assert rec.compute_history[0].manager_name == mname.fullname
        assert rec.compute_history[0].modified_on > time_1
        assert rec.compute_history[0].status == RecordStatusEnum.complete

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid)
    assert manager.successes == 2
    assert manager.failures == 0
    assert manager.rejected == 0
    assert manager.claimed == 2


def test_task_socket_fullworkflow_error(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager: Tuple[ManagerName, int]
):
    id1, _ = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_test_data(storage_socket, "sp_psi4_fluoroethane_wfn")

    mname, mid = activated_manager

    time_1 = datetime.utcnow()

    tasks = storage_socket.tasks.claim_tasks(mname.fullname)

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))

    rmeta = storage_socket.tasks.update_finished(
        mname.fullname,
        {tasks[0]["id"]: fop, tasks[1]["id"]: fop},
    )

    assert rmeta.n_accepted == 2
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == sorted([id1, id2])

    for rec_id in [id1, id2]:
        rec = session.get(BaseRecordORM, rec_id)

        # Status should be error
        assert rec.status == RecordStatusEnum.error

        # Tasks should not be deleted
        assert rec.task is not None

        # Manager names should have been assigned
        assert rec.manager_name == mname.fullname

        # Modified_on should be updated, but not created_on
        assert rec.created_on < time_1
        assert rec.modified_on > time_1

        # History should have been saved
        assert len(rec.compute_history) == 1
        assert rec.compute_history[0].manager_name == mname.fullname
        assert rec.compute_history[0].modified_on > time_1
        assert rec.compute_history[0].status == RecordStatusEnum.error

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid)
    assert manager.successes == 0
    assert manager.failures == 2
    assert manager.rejected == 0
    assert manager.claimed == 2


def test_task_socket_fullworkflow_error_retry(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager: Tuple[ManagerName, int]
):
    input_spec1, molecule1, result_data1 = load_test_data("sp_psi4_benzene_energy_1")

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule1], input_spec1, "tag1", PriorityEnum.normal, None, None
    )

    mname, mid = activated_manager

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))

    # Sends back an error. Do it a few times
    time_0 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname)
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: fop})
    storage_socket.records.reset(id1)

    time_1 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname)
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: fop})
    storage_socket.records.reset(id1)

    time_2 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname)
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: fop})
    storage_socket.records.reset(id1)

    # Now succeed
    time_3 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname)
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: result_data1})
    time_4 = datetime.utcnow()

    rec = session.get(BaseRecordORM, id1)
    hist = rec.compute_history
    assert len(hist) == 4

    for h in hist:
        assert h.manager_name == mname.fullname
        assert len(h.outputs) == 1

    assert time_0 < hist[0].modified_on < time_1
    assert time_1 < hist[1].modified_on < time_2
    assert time_2 < hist[2].modified_on < time_3
    assert time_3 < hist[3].modified_on < time_4

    assert hist[0].status == RecordStatusEnum.error
    assert hist[1].status == RecordStatusEnum.error
    assert hist[2].status == RecordStatusEnum.error
    assert hist[3].status == RecordStatusEnum.complete

    assert list(hist[3].outputs.keys()) == [OutputTypeEnum.stdout]
    assert list(hist[2].outputs.keys()) == [OutputTypeEnum.error]
    assert list(hist[1].outputs.keys()) == [OutputTypeEnum.error]
    assert list(hist[0].outputs.keys()) == [OutputTypeEnum.error]

    # Make sure manager info was updated
    manager = session.get(ComputeManagerORM, mid)
    assert manager.successes == 1
    assert manager.failures == 3
    assert manager.rejected == 0
    assert manager.claimed == 4


def test_task_socket_fullworkflow_error_autoreset(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName
):
    # Change the socket config
    storage_socket.qcf_config.auto_reset.enabled = True
    storage_socket.qcf_config.auto_reset.unknown_error = 1
    storage_socket.qcf_config.auto_reset.random_error = 2

    input_spec1, molecule1, result_data1 = load_test_data("sp_psi4_benzene_energy_1")

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule1], input_spec1, "tag1", PriorityEnum.normal, None, None
    )

    fop_u = FailedOperation(error=ComputeError(error_type="unknown_error", error_message="this is a test error"))
    fop_r = FailedOperation(error=ComputeError(error_type="random_error", error_message="this is a test error"))

    # Sends back an error
    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname)
    storage_socket.tasks.update_finished(activated_manager_name.fullname, {tasks[0]["id"]: fop_r})

    # task should be waiting
    rec = session.get(BaseRecordORM, id1)
    assert rec.status == RecordStatusEnum.waiting
    assert len(rec.compute_history) == 1

    # Claim again, and return a different error
    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname)
    storage_socket.tasks.update_finished(activated_manager_name.fullname, {tasks[0]["id"]: fop_u})

    # waiting again...
    session.expire(rec)
    assert rec.status == RecordStatusEnum.waiting
    assert len(rec.compute_history) == 2

    # Claim again, and return an unknown error. Should stay in errored state now
    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname)
    storage_socket.tasks.update_finished(activated_manager_name.fullname, {tasks[0]["id"]: fop_u})

    session.expire(rec)
    assert rec.status == RecordStatusEnum.error
    assert len(rec.compute_history) == 3
