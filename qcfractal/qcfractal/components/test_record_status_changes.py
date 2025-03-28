from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.optimization.testing_helpers import (
    run_test_data as run_opt_test_data,
    submit_test_data as submit_opt_test_data,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import submit_test_data as submit_sp_test_data
from qcfractal.components.testing_helpers import populate_records_status
from qcportal.managers import ManagerName
from qcportal.record_models import RecordStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


def test_record_socket_reset_assigned_manager(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="9876-5432-1098-7654")

    manager_programs = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=manager_programs,
        compute_tags=["tag1"],
    )
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs=manager_programs,
        compute_tags=["tag2"],
    )

    id_1, result_data_1 = submit_sp_test_data(storage_socket, "sp_psi4_water_energy", "tag1")
    id_2, result_data_2 = submit_sp_test_data(storage_socket, "sp_psi4_water_gradient", "tag2")
    id_3, result_data_3 = submit_sp_test_data(storage_socket, "sp_psi4_water_hessian", "tag1")
    id_4, result_data_4 = submit_opt_test_data(storage_socket, "opt_psi4_benzene", "tag2")
    id_5, result_data_5 = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1", "tag1")
    id_6, result_data_6 = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_2", "tag1")
    all_id = [id_1, id_2, id_3, id_4, id_5, id_6]

    tasks_1 = storage_socket.tasks.claim_tasks(mname1.fullname, manager_programs, ["tag1"])
    tasks_2 = storage_socket.tasks.claim_tasks(mname2.fullname, manager_programs, ["tag2"])

    rec = [session.get(BaseRecordORM, i) for i in all_id]
    assert all(r.status == RecordStatusEnum.running for r in rec)
    assert all(r.task.available is False for r in rec)

    assert len(tasks_1) == 4
    assert len(tasks_2) == 2

    time_0 = now_at_utc()
    ids = storage_socket.records.reset_assigned(manager_name=[mname1.fullname])
    time_1 = now_at_utc()
    assert set(ids) == {id_1, id_3, id_5, id_6}

    session.expire_all()
    rec = [session.get(BaseRecordORM, i) for i in all_id]
    assert rec[0].status == RecordStatusEnum.waiting
    assert rec[1].status == RecordStatusEnum.running
    assert rec[2].status == RecordStatusEnum.waiting
    assert rec[3].status == RecordStatusEnum.running
    assert rec[4].status == RecordStatusEnum.waiting
    assert rec[5].status == RecordStatusEnum.waiting

    assert rec[0].manager_name is None
    assert rec[1].manager_name == mname2.fullname
    assert rec[2].manager_name is None
    assert rec[3].manager_name == mname2.fullname
    assert rec[4].manager_name is None
    assert rec[5].manager_name is None

    assert time_0 < rec[0].modified_on < time_1
    assert rec[1].modified_on < time_0
    assert time_0 < rec[2].modified_on < time_1
    assert rec[3].modified_on < time_0
    assert time_0 < rec[4].modified_on < time_1
    assert time_0 < rec[5].modified_on < time_1

    assert rec[0].task.available is True
    assert rec[1].task.available is False
    assert rec[2].task.available is True
    assert rec[3].task.available is False
    assert rec[4].task.available is True
    assert rec[5].task.available is True


def test_record_socket_reset_assigned_manager_none(storage_socket: SQLAlchemySocket):
    populate_records_status(storage_socket)
    ids = storage_socket.records.reset_assigned(manager_name=[])
    assert ids == []


def test_record_client_reset(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)

    # Can reset only error
    time_0 = now_at_utc()
    meta = snowflake_client.reset_records(all_id)
    time_1 = now_at_utc()
    assert meta.n_updated == 1

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]

        # created_on shouldn't change
        for r in rec:
            assert r.created_on < time_0

        assert rec[0].status == RecordStatusEnum.waiting
        assert rec[1].status == RecordStatusEnum.complete
        assert rec[2].status == RecordStatusEnum.running
        assert rec[3].status == RecordStatusEnum.waiting
        assert rec[4].status == RecordStatusEnum.cancelled
        assert rec[5].status == RecordStatusEnum.deleted
        assert rec[6].status == RecordStatusEnum.invalid

        assert rec[0].task is not None
        assert rec[1].task is None
        assert rec[2].task is not None
        assert rec[3].task is not None
        assert rec[4].task is None
        assert rec[5].task is None
        assert rec[6].task is None

        assert rec[0].manager_name is None
        assert rec[1].manager_name is not None
        assert rec[2].manager_name is not None
        assert rec[3].manager_name is None
        assert rec[4].manager_name is None
        assert rec[5].manager_name is None
        assert rec[6].manager_name is not None

        assert rec[0].modified_on < time_0
        assert rec[1].modified_on < time_0
        assert rec[2].modified_on < time_0
        assert time_0 < rec[3].modified_on < time_1
        assert rec[4].modified_on < time_0
        assert rec[5].modified_on < time_0
        assert rec[6].modified_on < time_0


def test_record_socket_reset_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_records_status(storage_socket)
    meta = storage_socket.records.reset([])
    assert meta.n_updated == 0


def test_record_client_reset_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.reset_records([])
    assert meta.n_updated == 0


def test_record_client_reset_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)
    meta = snowflake_client.reset_records([all_id[3], 9999])
    assert meta.success is False
    assert meta.n_updated == 1


def test_record_client_cancel(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)

    # waiting, running, error can be cancelled
    time_0 = now_at_utc()
    meta = snowflake_client.cancel_records(all_id)
    time_1 = now_at_utc()
    assert meta.n_updated == 3

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]

        # created_on hasn't changed
        for r in rec:
            assert r.created_on < time_0

        assert rec[0].status == RecordStatusEnum.cancelled
        assert rec[1].status == RecordStatusEnum.complete
        assert rec[2].status == RecordStatusEnum.cancelled
        assert rec[3].status == RecordStatusEnum.cancelled
        assert rec[4].status == RecordStatusEnum.cancelled
        assert rec[5].status == RecordStatusEnum.deleted
        assert rec[6].status == RecordStatusEnum.invalid

        assert rec[0].task is None
        assert rec[1].task is None
        assert rec[2].task is None
        assert rec[3].task is None
        assert rec[4].task is None
        assert rec[5].task is None
        assert rec[6].task is None

        assert rec[0].manager_name is None
        assert rec[1].manager_name is not None
        assert rec[2].manager_name is None
        assert rec[3].manager_name is not None  # manager left for errored
        assert rec[4].manager_name is None
        assert rec[5].manager_name is None
        assert rec[6].manager_name is not None

        assert time_0 < rec[0].modified_on < time_1
        assert rec[1].modified_on < time_0
        assert time_0 < rec[2].modified_on < time_1
        assert time_0 < rec[3].modified_on < time_1
        assert rec[4].modified_on < time_0
        assert rec[5].modified_on < time_0
        assert rec[6].modified_on < time_0


def test_record_socket_cancel_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_records_status(storage_socket)
    meta = storage_socket.records.cancel([])
    assert meta.n_updated == 0


def test_record_client_cancel_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.cancel_records([])
    assert meta.n_updated == 0


def test_record_client_cancel_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)
    meta = snowflake_client.cancel_records([all_id[0], 9999])
    assert meta.success is False
    assert meta.n_updated == 1


def test_record_client_invalidate(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)

    # only completed can be invalidated
    time_0 = now_at_utc()
    meta = snowflake_client.invalidate_records(all_id)
    time_1 = now_at_utc()
    assert meta.n_updated == 1

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]

        # created_on hasn't changed
        for r in rec:
            assert r.created_on < time_0

        assert rec[0].status == RecordStatusEnum.waiting
        assert rec[1].status == RecordStatusEnum.invalid
        assert rec[2].status == RecordStatusEnum.running
        assert rec[3].status == RecordStatusEnum.error
        assert rec[4].status == RecordStatusEnum.cancelled
        assert rec[5].status == RecordStatusEnum.deleted
        assert rec[6].status == RecordStatusEnum.invalid

        assert rec[0].task is not None
        assert rec[1].task is None
        assert rec[2].task is not None
        assert rec[3].task is not None
        assert rec[4].task is None
        assert rec[5].task is None
        assert rec[6].task is None

        assert rec[0].task.available is True
        assert rec[2].task.available is False
        assert rec[3].task.available is False

        assert rec[0].manager_name is None
        assert rec[1].manager_name is not None  # Manager left on
        assert rec[2].manager_name is not None
        assert rec[3].manager_name is not None
        assert rec[4].manager_name is None
        assert rec[5].manager_name is None
        assert rec[6].manager_name is not None

        assert rec[0].modified_on < time_0
        assert time_0 < rec[1].modified_on < time_1
        assert rec[2].modified_on < time_0
        assert rec[3].modified_on < time_0
        assert rec[4].modified_on < time_0
        assert rec[5].modified_on < time_0
        assert rec[6].modified_on < time_0


def test_record_socket_invalidate_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_records_status(storage_socket)
    meta = storage_socket.records.invalidate([])
    assert meta.n_updated == 0


def test_record_client_invalidate_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.invalidate_records([])
    assert meta.n_updated == 0


def test_record_client_invalidate_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)
    meta = snowflake_client.invalidate_records([all_id[1], 9999])
    assert meta.success is False
    assert meta.n_updated == 1


def test_record_client_softdelete(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)

    # only deleted can't be deleted
    time_0 = now_at_utc()
    meta = snowflake_client.delete_records(all_id, soft_delete=True)
    time_1 = now_at_utc()
    assert meta.n_deleted == 6
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 6]
    assert meta.error_idx == [5]  # deleted can't be deleted

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]

        # created_on hasn't changed
        for r in rec:
            assert r.created_on < time_0

            assert r.status == RecordStatusEnum.deleted
            assert r.task is None

        assert time_0 < rec[0].modified_on < time_1
        assert time_0 < rec[1].modified_on < time_1
        assert time_0 < rec[2].modified_on < time_1
        assert time_0 < rec[3].modified_on < time_1
        assert time_0 < rec[4].modified_on < time_1
        assert rec[5].modified_on < time_0
        assert time_0 < rec[6].modified_on < time_1

        # completed and errored records should keep their manager
        assert rec[0].manager_name is None
        assert rec[1].manager_name is not None
        assert rec[2].manager_name is None
        assert rec[3].manager_name is not None
        assert rec[4].manager_name is None
        assert rec[5].manager_name is None
        assert rec[6].manager_name is not None


def test_record_client_softdelete_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)
    meta = snowflake_client.delete_records(all_id + [99999], soft_delete=True)
    assert meta.success is False
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 6]
    assert meta.n_deleted == 6
    assert meta.error_idx == [5, 7]


def test_record_socket_softdelete_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_records_status(storage_socket)
    meta = storage_socket.records.delete([], soft_delete=True)
    assert meta.success is True
    assert meta.n_deleted == 0


def test_record_client_softdelete_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.delete_records([], soft_delete=True)
    assert meta.success is True
    assert meta.n_deleted == 0


def test_record_client_harddelete_1(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)

    # only deleted can't be deleted
    meta = snowflake_client.delete_records(all_id, soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 5, 6]
    assert meta.n_deleted == 7

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert all(x is None for x in rec)


def test_record_client_harddelete_2(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    # Delete only some records
    all_id = populate_records_status(storage_socket)

    # only deleted can't be deleted
    meta = snowflake_client.delete_records([all_id[0], all_id[4]], soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1]
    assert meta.n_deleted == 2

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert rec[0] is None
        assert rec[1] is not None
        assert rec[2] is not None
        assert rec[3] is not None
        assert rec[4] is None
        assert rec[5] is not None
        assert rec[6] is not None


def test_record_socket_harddelete_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_records_status(storage_socket)
    meta = storage_socket.records.delete([], soft_delete=False)
    assert meta.success is True
    assert meta.deleted_idx == []


def test_record_client_harddelete_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.delete_records([], soft_delete=False)
    assert meta.success is True
    assert meta.deleted_idx == []


def test_record_client_harddelete_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    # Deletion always succeeds even if record doesn't exist
    all_id = populate_records_status(storage_socket)
    meta = snowflake_client.delete_records(all_id + [99999], soft_delete=False)
    assert meta.success is True
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 5, 6, 7]
    assert meta.n_deleted == 8


def test_record_client_revert_chain(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    # Tests undelete, uninvalidate, uncancel
    all_id = populate_records_status(storage_socket)

    # cancel, invalidate, then delete all
    meta = snowflake_client.cancel_records(all_id)
    assert meta.n_updated == 3

    meta = snowflake_client.invalidate_records(all_id)
    assert meta.n_updated == 1

    meta = snowflake_client.delete_records(all_id)
    assert meta.n_deleted == 6

    with storage_socket.session_scope() as session:
        rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert len(rec[0].info_backup) == 2
        assert len(rec[1].info_backup) == 2
        assert len(rec[2].info_backup) == 2
        assert len(rec[3].info_backup) == 2
        assert len(rec[4].info_backup) == 2
        assert len(rec[5].info_backup) == 1  # deleted in populate_db
        assert len(rec[6].info_backup) == 2

        meta = snowflake_client.undelete_records(all_id)
        assert meta.n_updated == 7

        session.expire_all()
        rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert rec[0].status == RecordStatusEnum.cancelled
        assert rec[1].status == RecordStatusEnum.invalid
        assert rec[2].status == RecordStatusEnum.cancelled
        assert rec[3].status == RecordStatusEnum.cancelled
        assert rec[4].status == RecordStatusEnum.cancelled
        assert rec[5].status == RecordStatusEnum.waiting  # from populate_db
        assert rec[6].status == RecordStatusEnum.invalid

        assert rec[0].task is None
        assert rec[1].task is None
        assert rec[2].task is None
        assert rec[3].task is None
        assert rec[4].task is None
        assert rec[5].task is not None
        assert rec[6].task is None

        assert rec[5].task.available is True

        assert len(rec[0].info_backup) == 1
        assert len(rec[1].info_backup) == 1
        assert len(rec[2].info_backup) == 1
        assert len(rec[3].info_backup) == 1
        assert len(rec[4].info_backup) == 1
        assert len(rec[5].info_backup) == 0
        assert len(rec[6].info_backup) == 1

        meta = snowflake_client.uncancel_records(all_id)
        assert meta.n_updated == 4

        session.expire_all()
        rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert rec[0].status == RecordStatusEnum.waiting
        assert rec[1].status == RecordStatusEnum.invalid
        assert rec[2].status == RecordStatusEnum.waiting
        assert rec[3].status == RecordStatusEnum.error
        assert rec[4].status == RecordStatusEnum.waiting  # from populate_db
        assert rec[5].status == RecordStatusEnum.waiting  # from populate_db
        assert rec[6].status == RecordStatusEnum.invalid

        assert len(rec[0].info_backup) == 0
        assert len(rec[1].info_backup) == 1
        assert len(rec[2].info_backup) == 0
        assert len(rec[3].info_backup) == 0
        assert len(rec[4].info_backup) == 0
        assert len(rec[5].info_backup) == 0
        assert len(rec[6].info_backup) == 1

        assert rec[0].task is not None
        assert rec[1].task is None
        assert rec[2].task is not None
        assert rec[3].task is not None
        assert rec[4].task is not None
        assert rec[5].task is not None
        assert rec[6].task is None

        assert rec[0].task.available is True
        assert rec[2].task.available is True
        assert rec[3].task.available is False
        assert rec[4].task.available is True
        assert rec[5].task.available is True

        meta = snowflake_client.uninvalidate_records(all_id)
        assert meta.n_updated == 2

        session.expire_all()
        rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert rec[0].status == RecordStatusEnum.waiting
        assert rec[1].status == RecordStatusEnum.complete
        assert rec[2].status == RecordStatusEnum.waiting
        assert rec[3].status == RecordStatusEnum.error
        assert rec[4].status == RecordStatusEnum.waiting  # from populate_db
        assert rec[5].status == RecordStatusEnum.waiting  # from populate_db
        assert rec[6].status == RecordStatusEnum.complete

        assert len(rec[0].info_backup) == 0
        assert len(rec[1].info_backup) == 0
        assert len(rec[2].info_backup) == 0
        assert len(rec[3].info_backup) == 0
        assert len(rec[4].info_backup) == 0
        assert len(rec[5].info_backup) == 0
        assert len(rec[6].info_backup) == 0

        assert rec[0].task is not None
        assert rec[1].task is None
        assert rec[2].task is not None
        assert rec[3].task is not None
        assert rec[4].task is not None
        assert rec[5].task is not None
        assert rec[6].task is None

        assert rec[0].task.available is True
        assert rec[2].task.available is True
        assert rec[3].task.available is False
        assert rec[4].task.available is True
        assert rec[5].task.available is True


def test_record_socket_undelete_none(storage_socket: SQLAlchemySocket):
    populate_records_status(storage_socket)
    meta = storage_socket.records.undelete([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_undelete_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.undelete_records([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_undelete_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.undelete_records([99999])
    assert meta.success is False
    assert meta.updated_idx == []
    assert meta.n_updated == 0
    assert meta.error_idx == [0]


def test_record_socket_uncancel_none(storage_socket: SQLAlchemySocket):
    populate_records_status(storage_socket)
    meta = storage_socket.records.uncancel([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uncancel_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.uncancel_records([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uncancel_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.uncancel_records([99999])
    assert meta.success is False
    assert meta.updated_idx == []
    assert meta.n_updated == 0
    assert meta.error_idx == [0]


def test_record_socket_uninvalidate_none(storage_socket: SQLAlchemySocket):
    populate_records_status(storage_socket)
    meta = storage_socket.records.uninvalidate([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uninvalidate_none(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.uninvalidate_records([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uninvalidate_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    populate_records_status(storage_socket)
    meta = snowflake_client.uninvalidate_records([99999])
    assert meta.success is False
    assert meta.updated_idx == []
    assert meta.n_updated == 0
    assert meta.error_idx == [0]


@pytest.mark.parametrize("opt_file", ["opt_psi4_benzene", "opt_psi4_fluoroethane_notraj"])
def test_record_client_delete_children(snowflake: QCATestingSnowflake, opt_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    # Deleting with deleting children
    id1 = run_opt_test_data(storage_socket, activated_manager_name, opt_file)

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, id1)
        child_ids = [x.singlepoint_id for x in rec.trajectory]

        meta = snowflake_client.delete_records([id1], soft_delete=True, delete_children=True)
        assert meta.success
        assert meta.deleted_idx == [0]
        assert meta.n_children_deleted == len(child_ids)

        session.expire_all()
        child_recs = [session.get(BaseRecordORM, i) for i in child_ids]
        assert all(x.status == RecordStatusEnum.deleted for x in child_recs)

        meta = snowflake_client.delete_records([id1], soft_delete=False, delete_children=True)
        assert meta.success
        assert meta.deleted_idx == [0]
        assert meta.n_children_deleted == len(child_ids)

        session.expire_all()
        rec = session.get(BaseRecordORM, id1)
        assert rec is None

        child_recs = [session.get(BaseRecordORM, i) for i in child_ids]
        assert all(x is None for x in child_recs)


@pytest.mark.parametrize("opt_file", ["opt_psi4_benzene", "opt_psi4_fluoroethane_notraj"])
def test_record_client_delete_nochildren(snowflake: QCATestingSnowflake, opt_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    # Deleting without deleting children
    id1 = run_opt_test_data(storage_socket, activated_manager_name, opt_file)

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, id1)
        child_ids = [x.singlepoint_id for x in rec.trajectory]

        meta = snowflake_client.delete_records([id1], soft_delete=True, delete_children=False)
        assert meta.success
        assert meta.deleted_idx == [0]
        assert meta.n_children_deleted == 0

        session.expire_all()
        child_recs = [session.get(BaseRecordORM, i) for i in child_ids]
        assert all(x.status == RecordStatusEnum.complete for x in child_recs)

        meta = snowflake_client.delete_records([id1], soft_delete=False, delete_children=False)
        assert meta.success
        assert meta.deleted_idx == [0]
        assert meta.n_children_deleted == 0

        session.expire_all()
        rec = session.get(BaseRecordORM, id1)
        assert rec is None

        child_recs = [session.get(BaseRecordORM, i) for i in child_ids]
        assert all(x.status == RecordStatusEnum.complete for x in child_recs)


@pytest.mark.parametrize("opt_file", ["opt_psi4_benzene", "opt_psi4_fluoroethane_notraj"])
def test_record_client_undelete_children(snowflake: QCATestingSnowflake, opt_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    # Deleting with deleting children, then undeleting
    id1 = run_opt_test_data(storage_socket, activated_manager_name, opt_file)

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, id1)
        child_ids = [x.singlepoint_id for x in rec.trajectory]

        meta = snowflake_client.delete_records([id1], soft_delete=True, delete_children=True)
        assert meta.success
        assert meta.deleted_idx == [0]
        assert meta.n_children_deleted == len(child_ids)

        meta = snowflake_client.undelete_records([id1])
        assert meta.success
        assert meta.updated_idx == [0]

        child_recs = [session.get(BaseRecordORM, i) for i in child_ids]
        assert all(x.status == RecordStatusEnum.complete for x in child_recs)
