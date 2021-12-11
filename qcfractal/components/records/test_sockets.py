"""
Tests the general record socket
"""
from __future__ import annotations

import random
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.exceptions import MissingDataError
from qcfractal.portal.managers import ManagerName
from qcfractal.portal.records import PriorityEnum, RecordStatusEnum, RecordQueryBody
from qcfractal.testing import load_procedure_data, populate_db, mname1, mname2

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_record_socket_get(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    random.shuffle(all_id)
    r = storage_socket.records.get(all_id)
    assert all_id == [x["id"] for x in r]

    r = storage_socket.records.get(all_id, include={"id", "status"})
    assert all_id == [x["id"] for x in r]
    assert all(set(x.keys()) == {"id", "status", "record_type"} for x in r)


def test_record_socket_get_history(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    r = storage_socket.records.get_history(all_id[3])
    assert len(r) == 5

    assert r[0]["manager_name"] == mname1.fullname
    assert r[1]["manager_name"] == mname2.fullname
    assert r[2]["manager_name"] == mname2.fullname
    assert r[3]["manager_name"] == mname2.fullname
    assert r[4]["manager_name"] == mname2.fullname

    assert r[0]["modified_on"] < r[1]["modified_on"]
    assert r[1]["modified_on"] < r[2]["modified_on"]
    assert r[2]["modified_on"] < r[3]["modified_on"]
    assert r[3]["modified_on"] < r[4]["modified_on"]

    assert all("outputs" not in x for x in r)

    r = storage_socket.records.get_history(all_id[3], include=["*", "outputs"])
    assert len(r) == 5
    assert all(x["outputs"] is not None for x in r)


def test_record_socket_get_missing(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    with pytest.raises(MissingDataError, match=r"Could not find all requested"):
        storage_socket.records.get([all_id[0], 9999, all_id[1]])

    r = storage_socket.records.get([all_id[0], 9999, all_id[1]], missing_ok=True)
    assert r[1] is None
    assert r[0]["id"] == all_id[0]
    assert r[2]["id"] == all_id[1]


def test_record_socket_query(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    all_records = storage_socket.records.get(all_id)

    # Try created before/after
    all_records_sorted = sorted(all_records, key=lambda x: x["created_on"])
    meta, data = storage_socket.records.query(RecordQueryBody(created_before=all_records_sorted[3]["created_on"]))
    assert meta.n_found == 3

    meta, data = storage_socket.records.query(RecordQueryBody(created_after=all_records_sorted[3]["created_on"]))
    assert meta.n_found == 2

    # modified before/after
    all_records_sorted = sorted(all_records, key=lambda x: x["modified_on"])
    meta, data = storage_socket.records.query(RecordQueryBody(modified_before=all_records_sorted[3]["modified_on"]))
    assert meta.n_found == 3

    meta, data = storage_socket.records.query(RecordQueryBody(modified_after=all_records_sorted[3]["modified_on"]))
    assert meta.n_found == 2

    # Record type
    meta, data = storage_socket.records.query(RecordQueryBody(record_type=["singlepoint"]))
    assert meta.n_found == 6

    meta, data = storage_socket.records.query(RecordQueryBody(record_type=["optimization"]))
    assert meta.n_found == 0

    meta, data = storage_socket.records.query(RecordQueryBody(record_type=["singlepoint", "optimization"]))
    assert meta.n_found == 6

    # Status
    meta, data = storage_socket.records.query(RecordQueryBody(status=[RecordStatusEnum.error]))
    assert meta.n_found == 1

    meta, data = storage_socket.records.query(
        RecordQueryBody(status=[RecordStatusEnum.error, RecordStatusEnum.waiting, RecordStatusEnum.deleted])
    )
    assert meta.n_found == 3

    # Some combinations
    meta, data = storage_socket.records.query(
        RecordQueryBody(record_type=["singlepoint"], status=[RecordStatusEnum.waiting])
    )
    assert meta.n_found == 1

    meta, data = storage_socket.records.query(
        RecordQueryBody(created_before=all_records[0]["created_on"], status=[RecordStatusEnum.waiting])
    )
    assert meta.n_found == 0

    meta, data = storage_socket.records.query(
        RecordQueryBody(status=[RecordStatusEnum.waiting], include=["id", "status"])
    )
    assert meta.n_found == 1
    assert set(data[0]) == {"id", "status", "record_type"}

    meta, data = storage_socket.records.query(RecordQueryBody(status=[RecordStatusEnum.waiting], exclude=["status"]))
    assert meta.n_found == 1
    assert "status" not in data[0].keys()

    # Empty query returns everything
    meta, data = storage_socket.records.query(RecordQueryBody())
    assert len(data) == len(all_id)
    assert meta.success
    assert meta.n_found == len(all_id)


def test_record_socket_get_empty(storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    r = storage_socket.records.get([])
    assert r == []


def test_record_socket_reset_id(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # waiting, deleted, completed cannot be reset
    time_0 = datetime.utcnow()
    meta = storage_socket.records.reset(all_id)
    time_1 = datetime.utcnow()
    assert meta.n_updated == 3

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on shouldn't change
    for r in rec:
        assert r["created_on"] < time_0

    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.waiting
    assert rec[3]["status"] == RecordStatusEnum.waiting
    assert rec[4]["status"] == RecordStatusEnum.waiting
    assert rec[5]["status"] == RecordStatusEnum.deleted

    assert rec[0]["task"] is not None
    assert rec[2]["task"] is not None
    assert rec[3]["task"] is not None
    assert rec[4]["task"] is not None
    assert rec[5]["task"] is None

    assert rec[0]["manager_name"] is None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is None
    assert rec[4]["manager_name"] is None

    # None because it was deleted while waiting
    assert rec[5]["manager_name"] is None

    assert rec[0]["modified_on"] < time_0
    assert rec[1]["modified_on"] < time_0
    assert time_0 < rec[2]["modified_on"] < time_1
    assert time_0 < rec[3]["modified_on"] < time_1
    assert time_0 < rec[4]["modified_on"] < time_1
    assert rec[5]["modified_on"] < time_0

    # Regenerated tasks have a new created_on
    assert rec[0]["task"]["created_on"] < time_0
    assert rec[2]["task"]["created_on"] < time_0
    assert rec[3]["task"]["created_on"] < time_0
    assert time_0 < rec[4]["task"]["created_on"] < time_1


def test_record_socket_reset_id_none(storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = storage_socket.records.reset([])
    assert meta.n_updated == 0


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

    meta, id_1 = storage_socket.records.singlepoint.add(input_spec_1, [molecule_1], "tag1", PriorityEnum.normal)
    meta, id_2 = storage_socket.records.singlepoint.add(input_spec_2, [molecule_2], "tag2", PriorityEnum.normal)
    meta, id_3 = storage_socket.records.singlepoint.add(input_spec_3, [molecule_3], "tag1", PriorityEnum.normal)
    meta, id_4 = storage_socket.records.singlepoint.add(input_spec_4, [molecule_4], "tag2", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add(input_spec_5, [molecule_5], "tag1", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add(input_spec_6, [molecule_6], "tag1", PriorityEnum.normal)
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
    all_id = populate_db(storage_socket)
    ids = storage_socket.records.reset_assigned(manager_name=[])
    assert ids == []


def test_record_socket_cancel(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # completed, cancelled, deleted cannot be cancelled
    time_0 = datetime.utcnow()
    meta = storage_socket.records.cancel(all_id)
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

    assert rec[0]["task"] is None
    assert rec[2]["task"] is None
    assert rec[3]["task"] is None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None

    assert rec[0]["manager_name"] is None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is None
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None

    assert time_0 < rec[0]["modified_on"] < time_1
    assert rec[1]["modified_on"] < time_0
    assert time_0 < rec[2]["modified_on"] < time_1
    assert time_0 < rec[3]["modified_on"] < time_1
    assert rec[4]["modified_on"] < time_0
    assert rec[5]["modified_on"] < time_0


def test_record_socket_cancel_none(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)
    meta = storage_socket.records.cancel([])
    assert meta.n_updated == 0


def test_record_socket_softdelete(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    time_0 = datetime.utcnow()
    meta = storage_socket.records.delete(all_id, soft_delete=True)
    time_1 = datetime.utcnow()
    assert meta.n_deleted == 5
    assert meta.deleted_idx == [0, 1, 2, 3, 4]

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

    # completed and errored records should keep their manager
    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] is not None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is not None
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None


def test_record_socket_undelete(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    time_0 = datetime.utcnow()
    meta = storage_socket.records.delete(all_id, soft_delete=True)
    assert meta.n_deleted == 5
    assert meta.deleted_idx == [0, 1, 2, 3, 4]

    time_1 = datetime.utcnow()
    meta = storage_socket.records.undelete(all_id)
    time_2 = datetime.utcnow()

    assert meta.success
    assert meta.n_undeleted == 6
    assert meta.undeleted_idx == [0, 1, 2, 3, 4, 5]

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    for r in rec:
        assert r["created_on"] < time_0
        assert time_1 < r["modified_on"] < time_2

    # 1 = waiting   2 = complete   3 = running
    # 4 = error     5 = cancelled  6 = deleted
    assert rec[0]["manager_name"] is None
    assert rec[1]["manager_name"] is not None
    assert rec[2]["manager_name"] is None
    assert rec[3]["manager_name"] is not None
    assert rec[4]["manager_name"] is None
    assert rec[5]["manager_name"] is None

    # rec[5] was deleted in populate_db. Will now be waiting
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.waiting
    assert rec[3]["status"] == RecordStatusEnum.error
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.waiting

    assert rec[0]["task"] is not None
    assert rec[1]["task"] is None
    assert rec[2]["task"] is not None
    assert rec[3]["task"] is not None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is not None


def test_record_socket_undelete_missing(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    meta = storage_socket.records.undelete([99999])
    assert meta.success
    assert meta.undeleted_idx == []
    assert meta.n_undeleted == 0
    assert meta.missing_idx == [0]


def test_record_socket_delete_1(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    meta = storage_socket.records.delete(all_id, soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 5]
    assert meta.n_deleted == 6

    rec = storage_socket.records.get(all_id, include=["*", "task"], missing_ok=True)
    assert all(x is None for x in rec)


def test_record_socket_delete_2(storage_socket: SQLAlchemySocket):
    # Delete only some records
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    meta = storage_socket.records.delete([all_id[0], all_id[4]], soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1]
    assert meta.n_deleted == 2

    rec = storage_socket.records.get(all_id, include=["*", "task"], missing_ok=True)
    assert rec[0] is None
    assert rec[1] is not None
    assert rec[2] is not None
    assert rec[3] is not None
    assert rec[4] is None
    assert rec[5] is not None


def test_record_socket_delete_missing(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    meta = storage_socket.records.delete(all_id + [99999], soft_delete=True)
    assert meta.success
    assert meta.deleted_idx == [0, 1, 2, 3, 4]
    assert meta.n_deleted == 5
    assert meta.missing_idx == [5, 6]


def test_record_socket_modify(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    time_0 = datetime.utcnow()

    # record 2 is complete - can't change
    meta = storage_socket.records.modify([all_id[0], all_id[1]], new_tag="new_tag")
    assert meta.n_updated == 1

    # one of these records in cancelled
    meta = storage_socket.records.modify([all_id[3], all_id[4]], new_priority=PriorityEnum.high)
    assert meta.n_updated == 1

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0
        assert r["modified_on"] < time_0

    # Waiting
    assert rec[0]["task"]["tag"] == "new_tag"
    assert rec[0]["task"]["priority"] == PriorityEnum.normal

    # completed
    assert rec[1]["task"] is None

    # running - not changed
    assert rec[2]["task"]["tag"] == "tag3"
    assert rec[2]["task"]["priority"] == PriorityEnum.normal

    # error
    assert rec[3]["task"]["tag"] == "tag4"
    assert rec[3]["task"]["priority"] == PriorityEnum.high

    # cancelled/deleted
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None

    # Delete tag
    meta = storage_socket.records.modify(all_id, delete_tag=True)
    assert meta.n_updated == 2

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0
        assert r["modified_on"] < time_0

    assert rec[0]["task"]["tag"] is None
    assert rec[1]["task"] is None
    assert rec[2]["task"]["tag"] == "tag3"
    assert rec[2]["task"]["priority"] == PriorityEnum.normal
    assert rec[3]["task"]["tag"] is None
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None
