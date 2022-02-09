from datetime import datetime

import pytest

from qcfractal.components.records.optimization.db_models import OptimizationRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import populate_db
from qcfractaltesting import load_procedure_data
from qcportal import PortalClient
from qcportal.managers import ManagerName
from qcportal.records import RecordStatusEnum, PriorityEnum


def test_record_socket_reset_assigned_manager(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="9876-5432-1098-7654")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag2"],
    )

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_water_energy")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_water_gradient")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_water_hessian")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_6, molecule_6, result_data_6 = load_procedure_data("psi4_benzene_energy_2")

    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.normal)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.normal)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "tag1", PriorityEnum.normal)
    meta, id_4 = storage_socket.records.singlepoint.add([molecule_4], input_spec_4, "tag2", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag1", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag1", PriorityEnum.normal)
    all_id = id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    tasks_1 = storage_socket.tasks.claim_tasks(mname1.fullname)
    tasks_2 = storage_socket.tasks.claim_tasks(mname2.fullname)

    assert len(tasks_1) == 4
    assert len(tasks_2) == 2

    time_0 = datetime.utcnow()
    ids = storage_socket.records.reset_assigned(manager_name=[mname1.fullname])
    time_1 = datetime.utcnow()
    assert set(ids) == set(id_1 + id_3 + id_5 + id_6)

    rec = storage_socket.records.get(all_id, include=["*", "task"])
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.running
    assert rec[2]["status"] == RecordStatusEnum.waiting
    assert rec[3]["status"] == RecordStatusEnum.running
    assert rec[4]["status"] == RecordStatusEnum.waiting
    assert rec[5]["status"] == RecordStatusEnum.waiting

    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] == mname2.fullname
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] == mname2.fullname
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None

    assert time_0 < rec[0]["modified_on"] < time_1
    assert rec[1]["modified_on"] < time_0
    assert time_0 < rec[2]["modified_on"] < time_1
    assert rec[3]["modified_on"] < time_0
    assert time_0 < rec[4]["modified_on"] < time_1
    assert time_0 < rec[5]["modified_on"] < time_1


def test_record_socket_reset_assigned_manager_none(storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    ids = storage_socket.records.reset_assigned(manager_name=[])
    assert ids == []


def test_record_client_reset(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # Can reset only running, error
    time_0 = datetime.utcnow()
    meta = snowflake_client.reset_records(all_id)
    time_1 = datetime.utcnow()
    assert meta.n_updated == 2

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on shouldn't change
    for r in rec:
        assert r["created_on"] < time_0

    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.waiting
    assert rec[3]["status"] == RecordStatusEnum.waiting
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.deleted
    assert rec[6]["status"] == RecordStatusEnum.invalid

    assert rec[0]["task"] is not None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is not None
    assert rec[3]["task"] is not None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None
    assert rec[6]["task"] is None

    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] is not None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is None
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None
    assert rec[6]["manager_name"] is not None

    assert rec[0]["modified_on"] < time_0
    assert rec[1]["modified_on"] < time_0
    assert time_0 < rec[2]["modified_on"] < time_1
    assert time_0 < rec[3]["modified_on"] < time_1
    assert rec[4]["modified_on"] < time_0
    assert rec[5]["modified_on"] < time_0
    assert rec[6]["modified_on"] < time_0


def test_record_socket_reset_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_db(storage_socket)
    meta = storage_socket.records.reset([])
    assert meta.n_updated == 0


def test_record_client_reset_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.reset_records([])
    assert meta.n_updated == 0


def test_record_client_reset_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)
    meta = snowflake_client.reset_records([all_id[2], 9999])
    assert meta.success is False
    assert meta.n_updated == 1


def test_record_client_cancel(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # waiting, running, error can be cancelled
    time_0 = datetime.utcnow()
    meta = snowflake_client.cancel_records(all_id)
    time_1 = datetime.utcnow()
    assert meta.n_updated == 3

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0

    assert rec[0]["status"] == RecordStatusEnum.cancelled
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.cancelled
    assert rec[3]["status"] == RecordStatusEnum.cancelled
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.deleted
    assert rec[6]["status"] == RecordStatusEnum.invalid

    assert rec[0]["task"] is None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is None
    assert rec[3]["task"] is None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None
    assert rec[6]["task"] is None

    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] is not None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is not None  # manager left for errored
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None
    assert rec[6]["manager_name"] is not None

    assert time_0 < rec[0]["modified_on"] < time_1
    assert rec[1]["modified_on"] < time_0
    assert time_0 < rec[2]["modified_on"] < time_1
    assert time_0 < rec[3]["modified_on"] < time_1
    assert rec[4]["modified_on"] < time_0
    assert rec[5]["modified_on"] < time_0
    assert rec[6]["modified_on"] < time_0


def test_record_socket_cancel_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_db(storage_socket)
    meta = storage_socket.records.cancel([])
    assert meta.n_updated == 0


def test_record_client_cancel_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.cancel_records([])
    assert meta.n_updated == 0


def test_record_client_cancel_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)
    meta = snowflake_client.cancel_records([all_id[0], 9999])
    assert meta.success is False
    assert meta.n_updated == 1


def test_record_client_invalidate(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only completed can be invalidated
    time_0 = datetime.utcnow()
    meta = snowflake_client.invalidate_records(all_id)
    time_1 = datetime.utcnow()
    assert meta.n_updated == 1

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0

    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.invalid
    assert rec[2]["status"] == RecordStatusEnum.running
    assert rec[3]["status"] == RecordStatusEnum.error
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.deleted
    assert rec[6]["status"] == RecordStatusEnum.invalid

    assert rec[0]["task"] is not None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is not None
    assert rec[3]["task"] is not None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None
    assert rec[6]["task"] is None

    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] is not None  # Manager left on
    assert rec[2]["manager_name"] is not None
    assert rec[3]["manager_name"] is not None
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None
    assert rec[6]["manager_name"] is not None

    assert rec[0]["modified_on"] < time_0
    assert time_0 < rec[1]["modified_on"] < time_1
    assert rec[2]["modified_on"] < time_0
    assert rec[3]["modified_on"] < time_0
    assert rec[4]["modified_on"] < time_0
    assert rec[5]["modified_on"] < time_0
    assert rec[6]["modified_on"] < time_0


def test_record_socket_invalidate_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_db(storage_socket)
    meta = storage_socket.records.invalidate([])
    assert meta.n_updated == 0


def test_record_client_invalidate_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.invalidate_records([])
    assert meta.n_updated == 0


def test_record_client_invalidate_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)
    meta = snowflake_client.invalidate_records([all_id[1], 9999])
    assert meta.success is False
    assert meta.n_updated == 1


def test_record_client_softdelete(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    time_0 = datetime.utcnow()
    meta = snowflake_client.delete_records(all_id, soft_delete=True)
    time_1 = datetime.utcnow()
    assert meta.n_deleted == 6
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 6]
    assert meta.error_idx == [5]  # deleted can't be deleted

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0

        assert r["status"] == RecordStatusEnum.deleted
        assert r["task"] is None

    assert time_0 < rec[0]["modified_on"] < time_1
    assert time_0 < rec[1]["modified_on"] < time_1
    assert time_0 < rec[2]["modified_on"] < time_1
    assert time_0 < rec[3]["modified_on"] < time_1
    assert time_0 < rec[4]["modified_on"] < time_1
    assert rec[5]["modified_on"] < time_0
    assert time_0 < rec[6]["modified_on"] < time_1

    # completed and errored records should keep their manager
    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] is not None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is not None
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None
    assert rec[6]["manager_name"] is not None


def test_record_client_softdelete_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)
    meta = snowflake_client.delete_records(all_id + [99999], soft_delete=True)
    assert meta.success is False
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 6]
    assert meta.n_deleted == 6
    assert meta.error_idx == [5, 7]


def test_record_socket_softdelete_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_db(storage_socket)
    meta = storage_socket.records.delete([], soft_delete=True)
    assert meta.success is True
    assert meta.n_deleted == 0


def test_record_client_softdelete_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.delete_records([], soft_delete=True)
    assert meta.success is True
    assert meta.n_deleted == 0


def test_record_client_harddelete_1(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    meta = snowflake_client.delete_records(all_id, soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 5, 6]
    assert meta.n_deleted == 7

    rec = storage_socket.records.get(all_id, include=["*", "task"], missing_ok=True)
    assert all(x is None for x in rec)


def test_record_client_harddelete_2(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    # Delete only some records
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    meta = snowflake_client.delete_records([all_id[0], all_id[4]], soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1]
    assert meta.n_deleted == 2

    rec = storage_socket.records.get(all_id, missing_ok=True)
    assert rec[0] is None
    assert rec[1] is not None
    assert rec[2] is not None
    assert rec[3] is not None
    assert rec[4] is None
    assert rec[5] is not None
    assert rec[6] is not None


def test_record_socket_harddelete_none(storage_socket: SQLAlchemySocket):
    # The client may short-circuit with empty lists, so we should also test the socket separately
    populate_db(storage_socket)
    meta = storage_socket.records.delete([], soft_delete=False)
    assert meta.success is True
    assert meta.deleted_idx == []


def test_record_client_harddelete_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.delete_records([], soft_delete=False)
    assert meta.success is True
    assert meta.deleted_idx == []


def test_record_client_harddelete_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)
    meta = snowflake_client.delete_records(all_id + [99999], soft_delete=False)
    assert meta.success is False
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 5, 6]
    assert meta.n_deleted == 7
    assert meta.error_idx == [7]


def test_record_client_revert_chain(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    # Tests undelete, uninvalidate, uncancel
    all_id = populate_db(storage_socket)

    # cancel, invalidate, then delete all
    meta = snowflake_client.cancel_records(all_id)
    assert meta.n_updated == 3

    meta = snowflake_client.invalidate_records(all_id)
    assert meta.n_updated == 1

    meta = snowflake_client.delete_records(all_id)
    assert meta.n_deleted == 6

    rec = storage_socket.records.get(all_id, include=["*", "task", "info_backup"])
    assert len(rec[0]["info_backup"]) == 2
    assert len(rec[1]["info_backup"]) == 2
    assert len(rec[2]["info_backup"]) == 2
    assert len(rec[3]["info_backup"]) == 2
    assert len(rec[4]["info_backup"]) == 2
    assert len(rec[5]["info_backup"]) == 1  # deleted in populate_db
    assert len(rec[6]["info_backup"]) == 2

    meta = snowflake_client.undelete_records(all_id)
    assert meta.n_updated == 7

    rec = storage_socket.records.get(all_id, include=["*", "task", "info_backup"])
    assert rec[0]["status"] == RecordStatusEnum.cancelled
    assert rec[1]["status"] == RecordStatusEnum.invalid
    assert rec[2]["status"] == RecordStatusEnum.cancelled
    assert rec[3]["status"] == RecordStatusEnum.cancelled
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.waiting  # from populate_db
    assert rec[6]["status"] == RecordStatusEnum.invalid

    assert rec[0]["task"] is None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is None
    assert rec[3]["task"] is None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is not None
    assert rec[6]["task"] is None

    assert len(rec[0]["info_backup"]) == 1
    assert len(rec[1]["info_backup"]) == 1
    assert len(rec[2]["info_backup"]) == 1
    assert len(rec[3]["info_backup"]) == 1
    assert len(rec[4]["info_backup"]) == 1
    assert len(rec[5]["info_backup"]) == 0
    assert len(rec[6]["info_backup"]) == 1

    meta = snowflake_client.uncancel_records(all_id)
    assert meta.n_updated == 4

    rec = storage_socket.records.get(all_id, include=["*", "task", "info_backup"])
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.invalid
    assert rec[2]["status"] == RecordStatusEnum.waiting
    assert rec[3]["status"] == RecordStatusEnum.error
    assert rec[4]["status"] == RecordStatusEnum.waiting  # from populate_db
    assert rec[5]["status"] == RecordStatusEnum.waiting  # from populate_db
    assert rec[6]["status"] == RecordStatusEnum.invalid

    assert len(rec[0]["info_backup"]) == 0
    assert len(rec[1]["info_backup"]) == 1
    assert len(rec[2]["info_backup"]) == 0
    assert len(rec[3]["info_backup"]) == 0
    assert len(rec[4]["info_backup"]) == 0
    assert len(rec[5]["info_backup"]) == 0
    assert len(rec[6]["info_backup"]) == 1

    assert rec[0]["task"] is not None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is not None
    assert rec[3]["task"] is not None
    assert rec[4]["task"] is not None
    assert rec[5]["task"] is not None
    assert rec[6]["task"] is None

    meta = snowflake_client.uninvalidate_records(all_id)
    assert meta.n_updated == 2

    rec = storage_socket.records.get(all_id, include=["*", "task", "info_backup"])
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.waiting
    assert rec[3]["status"] == RecordStatusEnum.error
    assert rec[4]["status"] == RecordStatusEnum.waiting  # from populate_db
    assert rec[5]["status"] == RecordStatusEnum.waiting  # from populate_db
    assert rec[6]["status"] == RecordStatusEnum.complete

    assert len(rec[0]["info_backup"]) == 0
    assert len(rec[1]["info_backup"]) == 0
    assert len(rec[2]["info_backup"]) == 0
    assert len(rec[3]["info_backup"]) == 0
    assert len(rec[4]["info_backup"]) == 0
    assert len(rec[5]["info_backup"]) == 0
    assert len(rec[6]["info_backup"]) == 0

    assert rec[0]["task"] is not None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is not None
    assert rec[3]["task"] is not None
    assert rec[4]["task"] is not None
    assert rec[5]["task"] is not None
    assert rec[6]["task"] is None


def test_record_socket_undelete_none(storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = storage_socket.records.undelete([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_undelete_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.undelete_records([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_undelete_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.undelete_records([99999])
    assert meta.success is False
    assert meta.updated_idx == []
    assert meta.n_updated == 0
    assert meta.error_idx == [0]


def test_record_socket_uncancel_none(storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = storage_socket.records.uncancel([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uncancel_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.uncancel_records([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uncancel_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.uncancel_records([99999])
    assert meta.success is False
    assert meta.updated_idx == []
    assert meta.n_updated == 0
    assert meta.error_idx == [0]


def test_record_socket_uninvalidate_none(storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = storage_socket.records.uninvalidate([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uninvalidate_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.uninvalidate_records([])
    assert meta.success is True
    assert meta.updated_idx == []
    assert meta.n_updated == 0


def test_record_client_uninvalidate_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.uninvalidate_records([99999])
    assert meta.success is False
    assert meta.updated_idx == []
    assert meta.n_updated == 0
    assert meta.error_idx == [0]


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
def test_record_client_delete_children(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, opt_file: str):
    # Deleting with deleting children
    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
    meta1, id1 = storage_socket.records.optimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )

    with storage_socket.session_scope() as session:
        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed_task(session, rec_orm, result_data_1, None)

    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]

    meta = snowflake_client.delete_records(id1, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = storage_socket.records.get(child_ids)
    assert all(x["status"] == RecordStatusEnum.deleted for x in child_recs)

    meta = snowflake_client.delete_records(id1, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = storage_socket.records.get(id1, missing_ok=True)
    assert recs == [None]

    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
def test_record_client_delete_nochildren(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, opt_file: str
):
    # Deleting without deleting children
    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
    meta1, id1 = storage_socket.records.optimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )

    with storage_socket.session_scope() as session:
        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed_task(session, rec_orm, result_data_1, None)

    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]

    meta = snowflake_client.delete_records(id1, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = storage_socket.records.get(child_ids)
    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)

    meta = snowflake_client.delete_records(id1, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = storage_socket.records.get(id1, missing_ok=True)
    assert recs == [None]

    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
def test_record_client_undelete_children(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, opt_file: str
):
    # Deleting with deleting children, then undeleting
    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
    meta1, id1 = storage_socket.records.optimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )

    with storage_socket.session_scope() as session:
        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed_task(session, rec_orm, result_data_1, None)

    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]

    meta = snowflake_client.delete_records(id1, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    meta = snowflake_client.undelete_records(id1)
    assert meta.success
    assert meta.updated_idx == [0]

    child_recs = storage_socket.records.get(child_ids)
    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)
