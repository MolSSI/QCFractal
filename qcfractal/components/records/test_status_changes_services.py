from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting import load_procedure_data, submit_service
from qcportal.records import FailedOperation, RecordStatusEnum, PriorityEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

# The slow ones are a bit more thorough - ie, some iterations are mixed complete/incomplete
test_files = [
    pytest.param("go_C4H4N2OS_psi4_b3lyp-d3bj", marks=pytest.mark.slow),
    "td_H2O2_psi4_hf",
    "go_H3NS_psi4_pbe0",  # preopt = True
    "go_H2O2_psi4_b3lyp-d3bj",  # preopt = False
]


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_reset_running_service(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.high)

    finished, n_optimizations = run_service_constropt(svc_id[0], result_data, storage_socket, 1)
    while not finished:
        snowflake_client.reset_records(svc_id)

        rec = storage_socket.records.get(svc_id, include=["*", "service.dependencies.record"])
        assert rec[0]["status"] == RecordStatusEnum.waiting

        # We should have also reset all the dependencies
        statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
        assert all(x in [RecordStatusEnum.waiting, RecordStatusEnum.complete] for x in statuses)

        # we need two iterations. The first will move the service to running,
        # the second will actually iterate if necessary
        finished, n_optimizations = run_service_constropt(
            svc_id[0], result_data, storage_socket, 2, activate_manager=False
        )

        rec = storage_socket.records.get(svc_id)
        assert rec[0]["status"] in [RecordStatusEnum.running, RecordStatusEnum.complete]


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_reset_error(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.high)

    # create an alternative result dict where everything has errored
    failed_op = FailedOperation(
        error={"error_type": "test_error", "error_message": "this is just a test error"},
    )

    failed_data = {x: failed_op for x in result_data.keys()}

    run_service_constropt(svc_id[0], result_data, storage_socket, 1)

    while True:
        snowflake_client.reset_records(svc_id)

        rec = storage_socket.records.get(svc_id, include=["*", "service.dependencies.record"])
        assert rec[0]["status"] == RecordStatusEnum.waiting

        # We should have also reset all the dependencies
        statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
        assert all(x in [RecordStatusEnum.waiting, RecordStatusEnum.complete] for x in statuses)

        # Move the service to running. This will also use result_data to populate some of the
        # previously-errored (now waiting) dependencies
        run_service_constropt(svc_id[0], result_data, storage_socket, 1, activate_manager=False)

        rec = storage_socket.records.get(svc_id)
        assert rec[0]["status"] == RecordStatusEnum.running

        # Inject an error
        # needs multiple iterations - first generates tasks and submits results
        # second returns all tasks as errored
        # iteration will only happen when all tasks are completed or errored
        run_service_constropt(svc_id[0], failed_data, storage_socket, 200, activate_manager=False)

        rec = storage_socket.records.get(svc_id, include=["*", "service.dependencies.record"])
        assert rec[0]["status"] in [RecordStatusEnum.error, RecordStatusEnum.complete]

        # Should have errored dependencies
        if rec[0]["status"] != RecordStatusEnum.complete:
            statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
            assert RecordStatusEnum.error in statuses
        else:
            break


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_waiting_service(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)

    snowflake_client.cancel_records(svc_id)

    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.cancelled
    assert rec[0]["service"] is not None

    # Uncancel - will run?
    snowflake_client.uncancel_records(svc_id)

    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[0]["service"] is not None
    assert rec[0]["service"]["tag"] == "test_tag"
    assert rec[0]["service"]["priority"] == PriorityEnum.low

    finished, n_optimizations = run_service_constropt(
        svc_id[0], result_data, storage_socket, 200, activate_manager=True
    )

    assert finished
    rec = storage_socket.records.get(svc_id, include=["*", "compute_history.outputs"])
    assert rec[0]["status"] == RecordStatusEnum.complete
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][0]["outputs"]) == 1


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_waiting_child(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)
    run_service_constropt(svc_id[0], result_data, storage_socket, 1, activate_manager=True)

    # Cancel a child
    with storage_socket.session_scope() as session:
        ch_ids = storage_socket.records.get_children_ids(session, svc_id)

    snowflake_client.cancel_records(ch_ids[0])

    # service is cancelled
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.cancelled

    # Uncancel & continue
    snowflake_client.uncancel_records(svc_id)

    finished, n_optimizations = run_service_constropt(
        svc_id[0], result_data, storage_socket, 200, activate_manager=False
    )
    assert finished
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.complete


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_running_service(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)

    # Get it running
    finished, n_optimizations = run_service_constropt(svc_id[0], result_data, storage_socket, 1)

    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.running

    while not finished:
        meta = snowflake_client.cancel_records(svc_id)
        assert meta.n_updated == 1

        rec = storage_socket.records.get(svc_id, include=["*", "service.*", "service.dependencies.record"])
        assert rec[0]["status"] == RecordStatusEnum.cancelled
        assert rec[0]["service"] is not None  # service queue data left in place
        statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
        assert all(x in [RecordStatusEnum.complete, RecordStatusEnum.cancelled] for x in statuses)
        changed_count = statuses.count(RecordStatusEnum.cancelled)
        assert meta.n_children_updated == changed_count

        # will it run after uncancel?
        meta = snowflake_client.uncancel_records(svc_id)
        assert meta.n_updated == 1
        assert meta.n_children_updated == changed_count

        rec = storage_socket.records.get(svc_id)
        assert rec[0]["status"] == RecordStatusEnum.waiting

        rec = storage_socket.records.get(svc_id, include=["*", "service.*", "service.dependencies.record"])
        statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
        assert all(x in [RecordStatusEnum.complete, RecordStatusEnum.waiting] for x in statuses)

        # we need two iterations. The first will move the service to running,
        # the second will actually iterate if necessary
        finished, n_optimizations = run_service_constropt(
            svc_id[0], result_data, storage_socket, 2, activate_manager=False
        )


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_cancel_error_service(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.high)

    # create an alternative result dict where everything has errored
    failed_op = FailedOperation(
        error={"error_type": "test_error", "error_message": "this is just a test error"},
    )

    failed_data = {x: failed_op for x in result_data.keys()}

    run_service_constropt(svc_id[0], result_data, storage_socket, 1)

    while True:
        snowflake_client.cancel_records(svc_id)

        rec = storage_socket.records.get(svc_id, include=["*", "service.*", "service.dependencies.record"])
        assert rec[0]["status"] == RecordStatusEnum.cancelled
        assert rec[0]["service"] is not None  # service queue data left in place
        statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
        assert all(
            x in [RecordStatusEnum.complete, RecordStatusEnum.error, RecordStatusEnum.cancelled] for x in statuses
        )

        # will it run after uncanceling and resetting?
        snowflake_client.uncancel_records(svc_id)
        snowflake_client.reset_records(svc_id)

        rec = storage_socket.records.get(svc_id, include=["*", "service.dependencies.record"])
        assert rec[0]["status"] == RecordStatusEnum.waiting

        # We should have also reset all the dependencies
        statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
        assert all(x in [RecordStatusEnum.waiting, RecordStatusEnum.complete] for x in statuses)

        # Move the service to running. This will also use result_data to populate some of the
        # previously-errored (now waiting) dependencies
        run_service_constropt(svc_id[0], result_data, storage_socket, 1, activate_manager=False)

        rec = storage_socket.records.get(svc_id)
        assert rec[0]["status"] == RecordStatusEnum.running

        # Inject an error
        # needs multiple iterations - first generates tasks and submits results
        # second returns all tasks as errored
        # iteration will only happen when all tasks are completed or errored
        run_service_constropt(svc_id[0], failed_data, storage_socket, 200, activate_manager=False)

        rec = storage_socket.records.get(svc_id, include=["*", "service.dependencies.record"])
        assert rec[0]["status"] in [RecordStatusEnum.error, RecordStatusEnum.complete]

        # Should have errored dependencies
        if rec[0]["status"] != RecordStatusEnum.complete:
            statuses = [x["record"]["status"] for x in rec[0]["service"]["dependencies"]]
            assert RecordStatusEnum.error in statuses
        else:
            break


@pytest.mark.parametrize("procedure_file", test_files)
def test_record_client_invalidate_completed_service(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str
):
    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)

    # Run it straight
    finished, n_optimizations = run_service_constropt(
        svc_id[0], result_data, storage_socket, 200, activate_manager=True
    )

    assert finished
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.complete

    # Mark as invalid
    snowflake_client.invalidate_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.invalid

    # Uninvalidate
    snowflake_client.uninvalidate_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.complete

    # Invalidate a child
    with storage_socket.session_scope() as session:
        ch_ids = storage_socket.records.get_children_ids(session, svc_id)

    snowflake_client.invalidate_records([ch_ids[0], ch_ids[1]])
    rec = storage_socket.records.get([svc_id[0], ch_ids[0], ch_ids[1]])
    assert rec[0]["status"] == RecordStatusEnum.invalid
    assert rec[1]["status"] == RecordStatusEnum.invalid
    assert rec[2]["status"] == RecordStatusEnum.invalid

    # Uninvalidate one child - shouldn't uninvalidate service
    snowflake_client.uninvalidate_records([ch_ids[0]])
    rec = storage_socket.records.get([svc_id[0], ch_ids[0], ch_ids[1]])
    assert rec[0]["status"] == RecordStatusEnum.invalid
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.invalid

    # Uninvalidate service
    snowflake_client.uninvalidate_records(svc_id)
    rec = storage_socket.records.get([svc_id[0], ch_ids[0], ch_ids[1]])
    assert rec[0]["status"] == RecordStatusEnum.complete
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.complete


def get_children_ids(storage_socket: SQLAlchemySocket, svc_ids):
    with storage_socket.session_scope() as session:
        return storage_socket.records.get_children_ids(session, svc_ids)


@pytest.mark.parametrize("procedure_file", test_files)
@pytest.mark.parametrize("delete_children", [True, False])
def test_record_client_softdelete_service(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str, delete_children: bool
):

    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)

    def check_children_deleted():
        ch_ids = get_children_ids(storage_socket, svc_id)
        ch = storage_socket.records.get(ch_ids)
        if delete_children:
            assert all(x["status"] == RecordStatusEnum.deleted for x in ch)
        else:
            assert all(x["status"] != RecordStatusEnum.deleted for x in ch)

    def check_children_undeleted():
        ch_ids = get_children_ids(storage_socket, svc_id)
        ch = storage_socket.records.get(ch_ids)
        assert all(x["status"] != RecordStatusEnum.deleted for x in ch)

    # 1. Service is waiting
    snowflake_client.delete_records(svc_id, soft_delete=True, delete_children=delete_children)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.deleted

    snowflake_client.undelete_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.waiting

    # 2. running
    finished, n_optimizations = run_service_constropt(svc_id[0], result_data, storage_socket, 1, activate_manager=True)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.running

    snowflake_client.delete_records(svc_id, soft_delete=True, delete_children=delete_children)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.deleted
    assert rec[0]["service"] is not None

    check_children_deleted()

    snowflake_client.undelete_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.waiting  # gets undeleted to "waiting"

    check_children_undeleted()

    # 3. error
    failed_op = FailedOperation(
        error={"error_type": "test_error", "error_message": "this is just a test error"},
    )
    failed_data = {x: failed_op for x in result_data.keys()}
    finished, n_optimizations = run_service_constropt(svc_id[0], failed_data, storage_socket, 3, activate_manager=False)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.error

    snowflake_client.delete_records(svc_id, soft_delete=True, delete_children=delete_children)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.deleted
    assert rec[0]["service"] is not None

    check_children_deleted()

    snowflake_client.undelete_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.error

    check_children_undeleted()

    # 4. cancelled
    snowflake_client.cancel_records(svc_id)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.cancelled

    snowflake_client.delete_records(svc_id, soft_delete=True, delete_children=delete_children)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.deleted
    assert rec[0]["service"] is not None

    check_children_deleted()

    snowflake_client.undelete_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.cancelled

    check_children_undeleted()

    # 5. completed
    # reset and finish
    snowflake_client.uncancel_records(svc_id)
    snowflake_client.reset_records(svc_id)  # was error
    finished, n_optimizations = run_service_constropt(
        svc_id[0], result_data, storage_socket, 200, activate_manager=False
    )
    assert finished
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.complete
    assert rec[0]["service"] is None

    # Now delete
    snowflake_client.delete_records(svc_id, soft_delete=True, delete_children=delete_children)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.deleted

    check_children_deleted()

    snowflake_client.undelete_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.complete

    check_children_undeleted()

    # 6. Invalid
    snowflake_client.invalidate_records(svc_id)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.invalid

    snowflake_client.delete_records(svc_id, soft_delete=True, delete_children=delete_children)
    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.deleted

    check_children_deleted()

    snowflake_client.undelete_records(svc_id)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.invalid

    check_children_undeleted()


@pytest.mark.parametrize("procedure_file", test_files)
@pytest.mark.parametrize("delete_children", [True, False])
def test_record_client_softdelete_service_child(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, procedure_file: str, delete_children: bool
):

    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)

    finished, n_optimizations = run_service_constropt(svc_id[0], result_data, storage_socket, 3, activate_manager=True)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.running

    ch_ids = get_children_ids(storage_socket, svc_id)
    snowflake_client.delete_records(ch_ids, soft_delete=True, delete_children=delete_children)

    rec = storage_socket.records.get(svc_id, include=["*", "service"])
    assert rec[0]["status"] == RecordStatusEnum.deleted

    ch = storage_socket.records.get([ch_ids[0]])

    # which children were deleted
    if delete_children:
        assert all(x["status"] == RecordStatusEnum.deleted for x in ch)
    else:
        for c in ch:
            if c["id"] == ch_ids[0]:
                assert c["status"] == RecordStatusEnum.deleted
            else:
                assert c["status"] != RecordStatusEnum.deleted


@pytest.mark.parametrize("procedure_file", test_files)
@pytest.mark.parametrize("status", [RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.complete])
@pytest.mark.parametrize("delete_children", [True, False])
def test_record_client_harddelete_service(
    snowflake_client: PortalClient,
    storage_socket: SQLAlchemySocket,
    procedure_file: str,
    status: RecordStatusEnum,
    delete_children: bool,
):

    input_spec, molecules, result_data = load_procedure_data(procedure_file)
    meta, svc_id = submit_service(storage_socket, input_spec, molecules, tag="test_tag", priority=PriorityEnum.low)

    if status == RecordStatusEnum.waiting:
        ch_ids = get_children_ids(storage_socket, svc_id)
        snowflake_client.delete_records(svc_id, soft_delete=False, delete_children=delete_children)

        rec = storage_socket.records.get(svc_id, missing_ok=True)
        assert rec == [None]

        ch = storage_socket.records.get(ch_ids, missing_ok=True)
        if delete_children:
            assert all(x is None for x in ch)
        else:
            assert all(x is not None for x in ch)
        return

    finished, n_optimizations = run_service_constropt(svc_id[0], result_data, storage_socket, 1, activate_manager=True)
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.running

    if status == RecordStatusEnum.running:
        ch_ids = get_children_ids(storage_socket, svc_id)
        snowflake_client.delete_records(svc_id, soft_delete=False, delete_children=delete_children)

        rec = storage_socket.records.get(svc_id, missing_ok=True)
        assert rec == [None]

        ch = storage_socket.records.get(ch_ids, missing_ok=True)
        if delete_children:
            assert all(x is None for x in ch)
        else:
            assert all(x is not None for x in ch)

        return

    finished, n_optimizations = run_service_constropt(
        svc_id[0], result_data, storage_socket, 200, activate_manager=False
    )
    rec = storage_socket.records.get(svc_id)
    assert rec[0]["status"] == RecordStatusEnum.complete

    if status == RecordStatusEnum.complete:
        ch_ids = get_children_ids(storage_socket, svc_id)
        snowflake_client.delete_records(svc_id, soft_delete=False, delete_children=delete_children)

        rec = storage_socket.records.get(svc_id, missing_ok=True)
        assert rec == [None]

        ch = storage_socket.records.get(ch_ids, missing_ok=True)
        if delete_children:
            assert all(x is None for x in ch)
        else:
            assert all(x is not None for x in ch)

        return

    raise RuntimeError("Unhandled status in test")
