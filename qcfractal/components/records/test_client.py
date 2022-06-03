"""
Tests the general record socket
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.testing_helpers import TestingSnowflake, mname1
from qcfractal.components.records.test_sockets import populate_db
from qcfractaltesting import test_users, load_record_data, submit_record_data
from qcportal import PortalRequestError, ManagerClient
from qcportal.managers import ManagerName
from qcportal.records import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


def test_record_client_get(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    r = snowflake_client.get_records(all_id)
    assert all_id == [x.raw_data.id for x in r]
    assert [x.raw_data.task is None for x in r]
    assert r[0].raw_data.compute_history == []

    assert r[1].raw_data.compute_history[0].outputs is None
    assert r[3].raw_data.compute_history[0].outputs is None
    assert [x.raw_data.task is None for x in r]

    r = snowflake_client.get_records(all_id, include=["outputs", "task"])
    assert r[1].raw_data.compute_history[0].outputs is not None
    assert r[3].raw_data.compute_history[0].outputs is not None
    assert r[0].raw_data.task is not None
    assert r[1].raw_data.task is None
    assert r[2].raw_data.task is not None
    assert r[3].raw_data.task is not None


def test_record_client_get_missing(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    with pytest.raises(PortalRequestError, match=r"Could not find all requested"):
        snowflake_client.get_records([all_id[0], 9999, all_id[1]])

    r = snowflake_client.get_records([all_id[0], 9999, all_id[1]], missing_ok=True)
    assert r[1] is None
    assert r[0].raw_data.id == all_id[0]
    assert r[2].raw_data.id == all_id[1]


def test_record_client_get_empty(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)

    r = snowflake_client.get_records([])
    assert r == []


def test_record_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    all_records = snowflake_client.get_records(all_id)

    # Try created before/after
    all_records_sorted = sorted(all_records, key=lambda x: x.raw_data.created_on)
    meta, data = snowflake_client.query_records(created_before=all_records_sorted[3].raw_data.created_on)
    assert meta.n_found == 3

    meta, data = snowflake_client.query_records(created_after=all_records_sorted[3].raw_data.created_on)
    assert meta.n_found == 3

    # modified before/after
    all_records_sorted = sorted(all_records, key=lambda x: x.raw_data.modified_on)
    meta, data = snowflake_client.query_records(modified_before=all_records_sorted[3].raw_data.modified_on)
    assert meta.n_found == 3

    meta, data = snowflake_client.query_records(modified_after=all_records_sorted[3].raw_data.modified_on)
    assert meta.n_found == 3

    # Record type
    meta, data = snowflake_client.query_records(record_type=["singlepoint"])
    assert meta.n_found == 6

    meta, data = snowflake_client.query_records(record_type=["optimization"])
    assert meta.n_found == 1

    meta, data = snowflake_client.query_records(record_type=["singlepoint", "optimization"])
    assert meta.n_found == 7

    # Status
    meta, data = snowflake_client.query_records(status=[RecordStatusEnum.error])
    assert meta.n_found == 1

    meta, data = snowflake_client.query_records(status=RecordStatusEnum.cancelled)
    assert meta.n_found == 1

    meta, data = snowflake_client.query_records(
        status=[RecordStatusEnum.error, RecordStatusEnum.waiting, RecordStatusEnum.deleted]
    )
    assert meta.n_found == 3

    # Some combinations
    meta, data = snowflake_client.query_records(record_type=["singlepoint"], status=[RecordStatusEnum.waiting])
    assert meta.n_found == 0

    meta, data = snowflake_client.query_records(record_type=["optimization"], status=[RecordStatusEnum.waiting])
    assert meta.n_found == 1

    meta, data = snowflake_client.query_records(
        created_before=all_records[0].raw_data.created_on, status=[RecordStatusEnum.waiting]
    )
    assert meta.n_found == 0

    # Including fields
    meta, data = snowflake_client.query_records(status=RecordStatusEnum.error)
    assert meta.n_found == 1
    assert data[0].raw_data.task is None
    assert data[0].raw_data.compute_history[0].outputs is None

    meta, data = snowflake_client.query_records(status=RecordStatusEnum.error, include=["outputs", "task"])
    assert meta.n_found == 1
    assert data[0].raw_data.task is not None
    assert data[0].raw_data.compute_history[0].outputs is not None

    # Empty query
    meta, data = snowflake_client.query_records()
    assert len(data) == len(all_id)
    assert meta.success
    assert meta.n_found == len(all_id)


def test_record_client_query_parents_children(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    id1, result_data_1 = submit_record_data(storage_socket, "psi4_benzene_opt")

    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname, limit=100)
    assert len(tasks) == 1

    rmeta = storage_socket.tasks.update_finished(
        activated_manager_name.fullname,
        {tasks[0]["id"]: result_data_1},
    )
    assert rmeta.n_accepted == 1

    opt_rec = snowflake_client.get_optimizations(id1, include=["trajectory"])
    assert opt_rec.status == RecordStatusEnum.complete

    traj_id = [x.singlepoint_id for x in opt_rec.raw_data.trajectory]
    assert len(traj_id) > 0

    # Query for records containing a record as a parent
    meta, recs = snowflake_client.query_records(parent_id=opt_rec.id)
    assert {x.id for x in recs} == set(traj_id)

    # Query for records containing a record as a child
    meta, recs = snowflake_client.query_records(child_id=traj_id[0])
    assert meta.n_found == 1
    assert recs[0].id == opt_rec.id


def test_record_client_add_comment(secure_snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    all_id = populate_db(storage_socket)

    # comments not retrieved by default
    rec = client.get_records(all_id)
    for r in rec:
        assert r.raw_data.comments is None

    rec = client.get_records(all_id, include=["comments"])
    for r in rec:
        assert r.raw_data.comments == []

    time_0 = datetime.utcnow()
    meta = client.add_comment([all_id[1], all_id[3]], comment="This is a test comment")
    time_1 = datetime.utcnow()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    meta = client.add_comment([all_id[2], all_id[3]], comment="This is another test comment")
    time_2 = datetime.utcnow()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    rec = client.get_records(all_id, include=["comments"])
    assert rec[0].raw_data.comments == []
    assert rec[4].raw_data.comments == []
    assert rec[5].raw_data.comments == []
    assert len(rec[1].raw_data.comments) == 1
    assert len(rec[2].raw_data.comments) == 1
    assert len(rec[3].raw_data.comments) == 2

    assert time_0 < rec[1].raw_data.comments[0].timestamp < time_1
    assert time_1 < rec[2].raw_data.comments[0].timestamp < time_2
    assert time_0 < rec[3].raw_data.comments[0].timestamp < time_1
    assert time_1 < rec[3].raw_data.comments[1].timestamp < time_2
    assert rec[1].raw_data.comments[0].username == "admin_user"
    assert rec[3].raw_data.comments[0].username == "admin_user"
    assert rec[2].raw_data.comments[0].username == "admin_user"
    assert rec[3].raw_data.comments[1].username == "admin_user"

    assert rec[1].raw_data.comments[0].comment == "This is a test comment"
    assert rec[3].raw_data.comments[0].comment == "This is a test comment"
    assert rec[2].raw_data.comments[0].comment == "This is another test comment"
    assert rec[3].raw_data.comments[1].comment == "This is another test comment"


def test_record_client_add_comment_nouser(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    time_0 = datetime.utcnow()
    meta = snowflake_client.add_comment([all_id[1], all_id[3]], comment="This is a test comment")
    time_1 = datetime.utcnow()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    rec = snowflake_client.get_records(all_id, include=["comments"])
    assert len(rec[1].raw_data.comments) == 1
    assert len(rec[3].raw_data.comments) == 1

    assert time_0 < rec[1].raw_data.comments[0].timestamp < time_1
    assert time_0 < rec[3].raw_data.comments[0].timestamp < time_1
    assert rec[1].raw_data.comments[0].username is None
    assert rec[3].raw_data.comments[0].username is None

    assert rec[1].raw_data.comments[0].comment == "This is a test comment"
    assert rec[3].raw_data.comments[0].comment == "This is a test comment"


def test_record_client_add_comment_badid(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    meta = snowflake_client.add_comment([all_id[1], 9999, all_id[3]], comment="test")
    assert not meta.success
    assert meta.n_updated == 2
    assert meta.n_errors == 1
    assert meta.updated_idx == [0, 2]
    assert meta.error_idx == [1]
    assert "does not exist" in meta.errors[0][1]


def test_record_client_modify(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    time_0 = datetime.utcnow()

    # record 2 is complete - can't change
    meta = snowflake_client.modify_records([all_id[0], all_id[1]], new_tag="new_tag")
    assert meta.n_updated == 1
    assert meta.updated_idx == [0]
    assert meta.error_idx == [1]

    # one of these records in cancelled
    meta = snowflake_client.modify_records([all_id[3], all_id[4]], new_priority=PriorityEnum.low)
    assert meta.n_updated == 1

    rec = snowflake_client.get_records(all_id, include=["task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r.raw_data.created_on < time_0
        assert r.raw_data.modified_on < time_0

    # 0 - waiting
    assert rec[0].raw_data.task.tag == "new_tag"
    assert rec[0].raw_data.task.priority == PriorityEnum.normal

    # 1 - completed
    assert rec[1].raw_data.task is None

    # 2 - running - not changed
    assert rec[2].raw_data.task.tag == "tag2"
    assert rec[2].raw_data.task.priority == PriorityEnum.high

    # 3 - error
    assert rec[3].raw_data.task.tag == "tag3"
    assert rec[3].raw_data.task.priority == PriorityEnum.low

    # 4/5/6 - cancelled/deleted/invalid
    assert rec[4].raw_data.task is None
    assert rec[5].raw_data.task is None
    assert rec[6].raw_data.task is None

    rec = snowflake_client.get_records(all_id, include=["task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r.raw_data.created_on < time_0
        assert r.raw_data.modified_on < time_0

    assert rec[1].raw_data.task is None
    assert rec[2].raw_data.task.priority == PriorityEnum.high
    assert rec[4].raw_data.task is None
    assert rec[5].raw_data.task is None
    assert rec[6].raw_data.task is None


def test_record_client_modify_service(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):

    svc_id, _ = submit_record_data(storage_socket, "td_H2O2_psi4_hf", "test_tag", PriorityEnum.high)

    storage_socket.services.iterate_services()

    rec = storage_socket.records.get([svc_id], include=["*", "service", "service.dependencies.record.task"])
    tasks = [x["record"]["task"] for x in rec[0]["service"]["dependencies"]]
    assert all(x["tag"] == "test_tag" for x in tasks)
    assert all(x["priority"] == PriorityEnum.high for x in tasks)

    # Modify service priority and tag
    meta = snowflake_client.modify_records(svc_id, new_tag="new_tag", new_priority=PriorityEnum.low)
    assert meta.n_updated == 1
    assert meta.n_children_updated > 0

    rec = snowflake_client.get_records(svc_id, include=["service"])
    assert rec.service.tag == "new_tag"
    assert rec.service.priority == PriorityEnum.low

    # Also changed all the dependencies
    assert len(rec.service.dependencies) > 0
    for opt in rec.service.dependencies:
        r = snowflake_client.get_records(opt.record_id, include=["task"])
        assert r.task.tag == "new_tag"
        assert r.task.priority == PriorityEnum.low
