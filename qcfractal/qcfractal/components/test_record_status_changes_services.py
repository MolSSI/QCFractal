from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from qcelemental.models import FailedOperation, ComputeError

from qcfractal.components.gridoptimization.testing_helpers import (
    submit_test_data as submit_go_test_data,
    generate_task_key as generate_go_task_key,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.torsiondrive.testing_helpers import (
    submit_test_data as submit_td_test_data,
    generate_task_key as generate_td_task_key,
)
from qcfractal.testing_helpers import run_service
from qcportal.record_models import RecordStatusEnum, PriorityEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcfractal.db_socket import SQLAlchemySocket

# The slow ones are a bit more thorough - ie, some iterations are mixed complete/incomplete
test_files = [
    pytest.param("go_C4H4N2OS_mopac_pm6", marks=pytest.mark.slow),
    "td_H2O2_mopac_pm6",
    "go_H3NS_psi4_pbe",  # preopt = True
    "go_H2O2_psi4_pbe",  # preopt = False
]


def _submit_test_data(storage_socket, name: str, tag="*", priority=PriorityEnum.normal):
    if name.startswith("go"):
        return submit_go_test_data(storage_socket, name, tag, priority)
    else:
        return submit_td_test_data(storage_socket, name, tag, priority)


def _get_task_key_generator(name: str):
    if name.startswith("go"):
        return generate_go_task_key
    else:
        return generate_td_task_key


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_reset_error_service(snowflake: QCATestingSnowflake, procedure_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    # create an alternative result dict where everything has errored
    failed_op = FailedOperation(
        error=ComputeError(error_type="test_error", error_message="this is just a test error"),
    )

    failed_data = {x: failed_op for x in result_data.keys()}

    run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

    # Make initial check for waiting service work
    storage_socket.records.reset_running([svc_id])

    while True:
        snowflake_client.reset_records([svc_id])

        with storage_socket.session_scope() as session:
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status == RecordStatusEnum.waiting

            # We should have also reset all the dependencies
            statuses = [x.record.status for x in rec.service.dependencies]
            assert all(x in [RecordStatusEnum.waiting, RecordStatusEnum.complete] for x in statuses)

            # Move the service to running. This will also use result_data to populate some of the
            # previously-errored (now waiting) dependencies
            run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

            session.expire(rec)
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status == RecordStatusEnum.running

            # Inject an error
            # needs multiple iterations - first generates tasks and submits results
            # second returns all tasks as errored
            # iteration will only happen when all tasks are completed or errored
            run_service(storage_socket, activated_manager_name, svc_id, keygen, failed_data, 200)

            session.expire(rec)
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status in [RecordStatusEnum.error, RecordStatusEnum.complete]

            # Should have errored dependencies
            if rec.status != RecordStatusEnum.complete:
                statuses = [x.record.status for x in rec.service.dependencies]
                assert RecordStatusEnum.error in statuses
            else:
                break


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_waiting_service(snowflake: QCATestingSnowflake, procedure_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file, "test_tag", PriorityEnum.low)
    keygen = _get_task_key_generator(procedure_file)

    snowflake_client.cancel_records([svc_id])

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.cancelled
        assert rec.service is not None

        # Uncancel - will run?
        snowflake_client.uncancel_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.waiting
        assert rec.service is not None
        assert rec.service.compute_tag == "test_tag"
        assert rec.service.compute_priority == PriorityEnum.low

        finished, n_optimizations = run_service(
            storage_socket, activated_manager_name, svc_id, keygen, result_data, 200
        )

        assert finished

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete
        assert len(rec.compute_history) == 1
        assert len(rec.compute_history[0].outputs) == 1


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_waiting_service_child(snowflake: QCATestingSnowflake, procedure_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

    # Cancel a child
    with storage_socket.session_scope() as s:
        ch_ids = storage_socket.records.get_children_ids(s, [svc_id])

    snowflake_client.cancel_records(ch_ids[0])

    # service is cancelled
    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.cancelled

        # Uncancel & continue
        snowflake_client.uncancel_records([svc_id])

        finished, n_optimizations = run_service(
            storage_socket, activated_manager_name, svc_id, keygen, result_data, 200
        )
        assert finished

        session.expire(rec)
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_running_service(snowflake: QCATestingSnowflake, procedure_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    # Get it running
    finished, n_optimizations = run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.running

        while not finished:
            meta = snowflake_client.cancel_records([svc_id])
            assert meta.n_updated == 1

            session.expire(rec)
            rec = session.get(BaseRecordORM, svc_id)

            assert rec.status == RecordStatusEnum.cancelled
            assert rec.service is not None  # service queue data left in place
            statuses = [x.record.status for x in rec.service.dependencies]
            assert all(x in [RecordStatusEnum.complete, RecordStatusEnum.cancelled] for x in statuses)
            changed_count = statuses.count(RecordStatusEnum.cancelled)
            assert meta.n_children_updated == changed_count

            # will it run after uncancel?
            meta = snowflake_client.uncancel_records([svc_id])
            assert meta.n_updated == 1
            assert meta.n_children_updated == changed_count

            session.expire(rec)
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status == RecordStatusEnum.waiting

            statuses = [x.record.status for x in rec.service.dependencies]
            assert all(x in [RecordStatusEnum.complete, RecordStatusEnum.waiting] for x in statuses)

            # we need two iterations. The first will move the service to running,
            # the second will actually iterate if necessary
            finished, n_optimizations = run_service(
                storage_socket, activated_manager_name, svc_id, keygen, result_data, 2
            )


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_error_service(snowflake: QCATestingSnowflake, procedure_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    # create an alternative result dict where everything has errored
    failed_op = FailedOperation(
        error=ComputeError(error_type="test_error", error_message="this is just a test error"),
    )

    failed_data = {x: failed_op for x in result_data.keys()}

    run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

    with storage_socket.session_scope() as session:
        while True:
            snowflake_client.cancel_records([svc_id])

            session.expire_all()
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status == RecordStatusEnum.cancelled
            assert rec.service is not None  # service queue data left in place
            statuses = [x.record.status for x in rec.service.dependencies]
            assert all(
                x in [RecordStatusEnum.complete, RecordStatusEnum.error, RecordStatusEnum.cancelled] for x in statuses
            )

            # will it run after uncanceling and resetting?
            snowflake_client.uncancel_records([svc_id])
            snowflake_client.reset_records([svc_id])

            session.expire_all()
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status == RecordStatusEnum.waiting

            # We should have also reset all the dependencies
            statuses = [x.record.status for x in rec.service.dependencies]
            assert all(x in [RecordStatusEnum.waiting, RecordStatusEnum.complete] for x in statuses)

            # Move the service to running. This will also use result_data to populate some of the
            # previously-errored (now waiting) dependencies
            run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

            session.expire_all()
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status == RecordStatusEnum.running

            # Inject an error
            # needs multiple iterations - first generates tasks and submits results
            # second returns all tasks as errored
            # iteration will only happen when all tasks are completed or errored
            run_service(storage_socket, activated_manager_name, svc_id, keygen, failed_data, 200)

            session.expire_all()
            rec = session.get(BaseRecordORM, svc_id)
            assert rec.status in [RecordStatusEnum.error, RecordStatusEnum.complete]

            # Should have errored dependencies
            if rec.status != RecordStatusEnum.complete:
                statuses = [x.record.status for x in rec.service.dependencies]
                assert RecordStatusEnum.error in statuses
            else:
                break


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_invalidate_completed_service(snowflake: QCATestingSnowflake, procedure_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    # Run it straight
    finished, n_optimizations = run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 200)

    assert finished

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete

        # Mark as invalid
        snowflake_client.invalidate_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.invalid

        # Uninvalidate
        snowflake_client.uninvalidate_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete

        # Invalidate a child
        with storage_socket.session_scope() as s:
            ch_ids = storage_socket.records.get_children_ids(s, [svc_id])

        snowflake_client.invalidate_records([ch_ids[0], ch_ids[1]])

        session.expire_all()
        rec = [session.get(BaseRecordORM, i) for i in [svc_id, ch_ids[0], ch_ids[1]]]
        assert rec[0].status == RecordStatusEnum.invalid
        assert rec[1].status == RecordStatusEnum.invalid
        assert rec[2].status == RecordStatusEnum.invalid

        # Uninvalidate one child - shouldn't uninvalidate service
        snowflake_client.uninvalidate_records([ch_ids[0]])

        session.expire_all()
        rec = [session.get(BaseRecordORM, i) for i in [svc_id, ch_ids[0], ch_ids[1]]]
        assert rec[0].status == RecordStatusEnum.invalid
        assert rec[1].status == RecordStatusEnum.complete
        assert rec[2].status == RecordStatusEnum.invalid

        # Uninvalidate service
        snowflake_client.uninvalidate_records([svc_id])

        session.expire_all()
        rec = [session.get(BaseRecordORM, i) for i in [svc_id, ch_ids[0], ch_ids[1]]]
        assert rec[0].status == RecordStatusEnum.complete
        assert rec[1].status == RecordStatusEnum.complete
        assert rec[2].status == RecordStatusEnum.complete


def get_children_ids(storage_socket: SQLAlchemySocket, svc_ids):
    with storage_socket.session_scope() as session:
        return storage_socket.records.get_children_ids(session, svc_ids)


@pytest.mark.parametrize("procedure_file", test_files)
@pytest.mark.parametrize("delete_children", [True, False])
def test_record_client_softdelete_service(snowflake: QCATestingSnowflake, procedure_file: str, delete_children: bool):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    with storage_socket.session_scope() as session:

        def check_children_deleted():
            ch_ids = get_children_ids(storage_socket, [svc_id])

            ch = [session.get(BaseRecordORM, i) for i in ch_ids]
            if delete_children:
                assert all(x.status == RecordStatusEnum.deleted for x in ch)
            else:
                assert all(x.status != RecordStatusEnum.deleted for x in ch)

        def check_children_undeleted():
            ch_ids = get_children_ids(storage_socket, [svc_id])

            ch = [session.get(BaseRecordORM, i) for i in ch_ids]
            assert all(x.status != RecordStatusEnum.deleted for x in ch)

        # 1. Service is waiting
        snowflake_client.delete_records([svc_id], soft_delete=True, delete_children=delete_children)

        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted

        snowflake_client.undelete_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.waiting

        # 2. running
        run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.running

        snowflake_client.delete_records([svc_id], soft_delete=True, delete_children=delete_children)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted
        assert rec.service is not None

        check_children_deleted()

        snowflake_client.undelete_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.waiting  # gets undeleted to "waiting"

        check_children_undeleted()

        # 3. error
        failed_op = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )
        failed_data = {x: failed_op for x in result_data.keys()}
        run_service(storage_socket, activated_manager_name, svc_id, keygen, failed_data, 3)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.error

        snowflake_client.delete_records([svc_id], soft_delete=True, delete_children=delete_children)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted
        assert rec.service is not None

        check_children_deleted()

        snowflake_client.undelete_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.error

        check_children_undeleted()

        # 4. cancelled
        snowflake_client.cancel_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.cancelled

        snowflake_client.delete_records([svc_id], soft_delete=True, delete_children=delete_children)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted
        assert rec.service is not None

        check_children_deleted()

        snowflake_client.undelete_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.cancelled

        check_children_undeleted()

        # 5. completed
        # reset and finish
        snowflake_client.uncancel_records([svc_id])
        snowflake_client.reset_records([svc_id])  # was error
        finished, n_optimizations = run_service(
            storage_socket, activated_manager_name, svc_id, keygen, result_data, 200
        )
        assert finished

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete
        assert rec.service is None

        # Now delete
        snowflake_client.delete_records([svc_id], soft_delete=True, delete_children=delete_children)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted

        check_children_deleted()

        snowflake_client.undelete_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete

        check_children_undeleted()

        # 6. Invalid
        snowflake_client.invalidate_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.invalid

        snowflake_client.delete_records([svc_id], soft_delete=True, delete_children=delete_children)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted

        check_children_deleted()

        snowflake_client.undelete_records([svc_id])

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.invalid

        check_children_undeleted()


@pytest.mark.parametrize("procedure_file", test_files)
@pytest.mark.parametrize("delete_children", [True, False])
def test_record_client_softdelete_service_child(
    snowflake: QCATestingSnowflake, procedure_file: str, delete_children: bool
):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 3)

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.running

        ch_ids = get_children_ids(storage_socket, [svc_id])
        snowflake_client.delete_records(ch_ids[0], soft_delete=True, delete_children=delete_children)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.deleted

        ch = [session.get(BaseRecordORM, i) for i in ch_ids]

        # which children were deleted
        if delete_children:
            assert all(x.status == RecordStatusEnum.deleted for x in ch)
        else:
            for c in ch:
                if c.id == ch_ids[0]:
                    assert c.status == RecordStatusEnum.deleted
                else:
                    assert c.status != RecordStatusEnum.deleted


@pytest.mark.parametrize("procedure_file", test_files)
@pytest.mark.parametrize("status", [RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.complete])
@pytest.mark.parametrize("delete_children", [True, False])
def test_record_client_harddelete_service(
    snowflake: QCATestingSnowflake, procedure_file: str, status: RecordStatusEnum, delete_children: bool
):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    svc_id, result_data = _submit_test_data(storage_socket, procedure_file)
    keygen = _get_task_key_generator(procedure_file)

    with storage_socket.session_scope() as session:
        if status == RecordStatusEnum.waiting:
            ch_ids = get_children_ids(storage_socket, [svc_id])
            snowflake_client.delete_records([svc_id], soft_delete=False, delete_children=delete_children)

            rec = session.get(BaseRecordORM, svc_id)
            assert rec is None

            ch = [session.get(BaseRecordORM, i) for i in ch_ids]
            if delete_children:
                assert all(x is None for x in ch)
            else:
                assert all(x is not None for x in ch)
            return

        run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 1)

        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.running

        if status == RecordStatusEnum.running:
            ch_ids = get_children_ids(storage_socket, [svc_id])
            snowflake_client.delete_records(svc_id, soft_delete=False, delete_children=delete_children)

            session.expire_all()
            rec = session.get(BaseRecordORM, svc_id)
            assert rec is None

            ch = [session.get(BaseRecordORM, i) for i in ch_ids]
            if delete_children:
                assert all(x is None for x in ch)
            else:
                assert all(x is not None for x in ch)

            return

        run_service(storage_socket, activated_manager_name, svc_id, keygen, result_data, 200)

        session.expire_all()
        rec = session.get(BaseRecordORM, svc_id)
        assert rec.status == RecordStatusEnum.complete

        if status == RecordStatusEnum.complete:
            ch_ids = get_children_ids(storage_socket, [svc_id])
            snowflake_client.delete_records([svc_id], soft_delete=False, delete_children=delete_children)

            session.expire_all()
            rec = session.get(BaseRecordORM, svc_id)
            assert rec is None

            ch = [session.get(BaseRecordORM, i) for i in ch_ids]
            if delete_children:
                assert all(x is None for x in ch)
            else:
                assert all(x is not None for x in ch)

            return

        raise RuntimeError("Unhandled status in test")
