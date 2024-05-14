"""
Tests the tasks socket (claiming & returning data)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from qcelemental.models import ComputeError, FailedOperation

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_test_data, submit_test_data
from qcfractalcompute.compress import compress_result
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_task_socket_fullworkflow_success(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    mname, mid = snowflake.activate_manager()
    activated_manager_programs = snowflake.activated_manager_programs()

    id1, result_data1 = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1", PriorityEnum.normal)
    id2, result_data2 = submit_test_data(storage_socket, "sp_psi4_fluoroethane_wfn", "tag1", PriorityEnum.normal)

    time_1 = now_at_utc()

    result_map = {id1: result_data1, id2: result_data2}

    tasks = storage_socket.tasks.claim_tasks(mname.fullname, activated_manager_programs, ["*"])

    with storage_socket.session_scope() as session:
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
            {
                tasks[0]["id"]: compress_result(result_map[tasks[0]["record_id"]].dict()),
                tasks[1]["id"]: compress_result(result_map[tasks[1]["record_id"]].dict()),
            },
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


def test_task_socket_fullworkflow_error(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    mname, mid = snowflake.activate_manager()
    activated_manager_programs = snowflake.activated_manager_programs()

    id1, _ = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_test_data(storage_socket, "sp_psi4_fluoroethane_wfn")

    time_1 = now_at_utc()

    tasks = storage_socket.tasks.claim_tasks(mname.fullname, activated_manager_programs, ["*"])

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))

    rmeta = storage_socket.tasks.update_finished(
        mname.fullname,
        {tasks[0]["id"]: compress_result(fop.dict()), tasks[1]["id"]: compress_result(fop.dict())},
    )

    assert rmeta.n_accepted == 2
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == sorted([id1, id2])

    with storage_socket.session_scope() as session:
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


def test_task_socket_fullworkflow_error_retry(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    mname, mid = snowflake.activate_manager()
    activated_manager_programs = snowflake.activated_manager_programs()

    input_spec1, molecule1, result_data1 = load_test_data("sp_psi4_benzene_energy_1")
    result_data1_compressed = compress_result(result_data1.dict())

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule1], input_spec1, "tag1", PriorityEnum.normal, None, None, True
    )

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))
    fop_compressed = compress_result(fop.dict())

    # Sends back an error. Do it a few times
    time_0 = now_at_utc()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname, activated_manager_programs, ["*"])
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: fop_compressed})
    storage_socket.records.reset(id1)

    time_1 = now_at_utc()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname, activated_manager_programs, ["*"])
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: fop_compressed})
    storage_socket.records.reset(id1)

    time_2 = now_at_utc()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname, activated_manager_programs, ["*"])
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: fop_compressed})
    storage_socket.records.reset(id1)

    # Now succeed
    time_3 = now_at_utc()
    tasks = storage_socket.tasks.claim_tasks(mname.fullname, activated_manager_programs, ["*"])
    storage_socket.tasks.update_finished(mname.fullname, {tasks[0]["id"]: result_data1_compressed})
    time_4 = now_at_utc()

    with storage_socket.session_scope() as session:
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


def test_task_socket_fullworkflow_error_autoreset(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    activated_manager_programs = snowflake.activated_manager_programs()

    # Change the socket config
    storage_socket.qcf_config.auto_reset.enabled = True
    storage_socket.qcf_config.auto_reset.unknown_error = 1
    storage_socket.qcf_config.auto_reset.random_error = 2

    input_spec1, molecule1, result_data1 = load_test_data("sp_psi4_benzene_energy_1")

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule1], input_spec1, "tag1", PriorityEnum.normal, None, None, True
    )

    fop_u = FailedOperation(error=ComputeError(error_type="unknown_error", error_message="this is a test error"))
    fop_u_compressed = compress_result(fop_u.dict())
    fop_r = FailedOperation(error=ComputeError(error_type="random_error", error_message="this is a test error"))
    fop_r_compressed = compress_result(fop_r.dict())

    # Sends back an error
    with storage_socket.session_scope() as session:
        tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname, activated_manager_programs, ["*"])
        storage_socket.tasks.update_finished(activated_manager_name.fullname, {tasks[0]["id"]: fop_r_compressed})

        # task should be waiting
        rec = session.get(BaseRecordORM, id1)
        assert rec.status == RecordStatusEnum.waiting
        assert len(rec.compute_history) == 1

        # Claim again, and return a different error
        tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname, activated_manager_programs, ["*"])
        storage_socket.tasks.update_finished(activated_manager_name.fullname, {tasks[0]["id"]: fop_u_compressed})

        # waiting again...
        session.expire(rec)
        assert rec.status == RecordStatusEnum.waiting
        assert len(rec.compute_history) == 2

        # Claim again, and return an unknown error. Should stay in errored state now
        tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname, activated_manager_programs, ["*"])
        storage_socket.tasks.update_finished(activated_manager_name.fullname, {tasks[0]["id"]: fop_u_compressed})

        session.expire(rec)
        assert rec.status == RecordStatusEnum.error
        assert len(rec.compute_history) == 3
