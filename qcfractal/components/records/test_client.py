"""
Tests the general record socket
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.portal.client import PortalRequestError
from qcfractal.portal.records import PriorityEnum, RecordStatusEnum
from qcfractal.testing import populate_db

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcfractal.portal import PortalClient


def test_record_client_get(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    r = snowflake_client.get_records(all_id)
    assert all_id == [x.raw_data.id for x in r]
    assert [x.raw_data.task is None for x in r]
    assert r[0].raw_data.compute_history == []

    assert r[1].raw_data.compute_history[0].outputs is None
    assert r[3].raw_data.compute_history[0].outputs is None
    assert [x.raw_data.task is None for x in r]

    r = snowflake_client.get_records(all_id, include_outputs=True, include_task=True)
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


def test_record_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    all_records = snowflake_client.get_records(all_id)

    # Try created before/after
    all_records_sorted = sorted(all_records, key=lambda x: x.raw_data.created_on)
    meta, data = snowflake_client.query_records(created_before=all_records_sorted[3].raw_data.created_on)
    assert meta.n_found == 3

    meta, data = snowflake_client.query_records(created_after=all_records_sorted[3].raw_data.created_on)
    assert meta.n_found == 2

    # modified before/after
    all_records_sorted = sorted(all_records, key=lambda x: x.raw_data.modified_on)
    meta, data = snowflake_client.query_records(modified_before=all_records_sorted[3].raw_data.modified_on)
    assert meta.n_found == 3

    meta, data = snowflake_client.query_records(modified_after=all_records_sorted[3].raw_data.modified_on)
    assert meta.n_found == 2

    # Record type
    meta, data = snowflake_client.query_records(record_type=["singlepoint"])
    assert meta.n_found == 6

    meta, data = snowflake_client.query_records(record_type=["optimization"])
    assert meta.n_found == 0

    meta, data = snowflake_client.query_records(record_type=["singlepoint", "optimization"])
    assert meta.n_found == 6

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

    meta, data = snowflake_client.query_records(status=RecordStatusEnum.error, include_outputs=True, include_task=True)
    assert meta.n_found == 1
    assert data[0].raw_data.task is not None
    assert data[0].raw_data.compute_history[0].outputs is not None

    # Empty query
    meta, data = snowflake_client.query_records()
    assert len(data) == len(all_id)
    assert meta.success
    assert meta.n_found == len(all_id)


def test_record_client_get_empty(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)

    r = snowflake_client.get_records([])
    assert r == []


def test_record_client_reset_id(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # waiting, deleted, completed cannot be reset
    time_0 = datetime.utcnow()
    meta = snowflake_client.reset_records(all_id)
    time_1 = datetime.utcnow()
    assert meta.n_updated == 3

    rec = snowflake_client.get_records(all_id, include_task=True)

    # created_on shouldn't change
    for r in rec:
        assert r.raw_data.created_on < time_0

    assert rec[0].raw_data.status == RecordStatusEnum.waiting
    assert rec[1].raw_data.status == RecordStatusEnum.complete
    assert rec[2].raw_data.status == RecordStatusEnum.waiting
    assert rec[3].raw_data.status == RecordStatusEnum.waiting
    assert rec[4].raw_data.status == RecordStatusEnum.waiting
    assert rec[5].raw_data.status == RecordStatusEnum.deleted

    assert rec[0].raw_data.task is not None
    assert rec[2].raw_data.task is not None
    assert rec[3].raw_data.task is not None
    assert rec[4].raw_data.task is not None
    assert rec[5].raw_data.task is None

    assert rec[0].raw_data.manager_name is None
    assert rec[2].raw_data.manager_name is None
    assert rec[3].raw_data.manager_name is None
    assert rec[4].raw_data.manager_name is None

    # None because it was deleted while waiting
    assert rec[5].raw_data.manager_name is None

    assert rec[0].raw_data.modified_on < time_0
    assert rec[1].raw_data.modified_on < time_0
    assert time_0 < rec[2].raw_data.modified_on < time_1
    assert time_0 < rec[3].raw_data.modified_on < time_1
    assert time_0 < rec[4].raw_data.modified_on < time_1
    assert rec[5].raw_data.modified_on < time_0

    # Regenerated tasks have a new created_on
    assert rec[0].raw_data.task.created_on < time_0
    assert rec[2].raw_data.task.created_on < time_0
    assert rec[3].raw_data.task.created_on < time_0
    assert time_0 < rec[4].raw_data.task.created_on < time_1


def test_record_client_reset_id_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    populate_db(storage_socket)
    meta = snowflake_client.reset_records([])
    assert meta.n_updated == 0
    assert meta.n_errors == 0


def test_record_client_cancel(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # completed, cancelled, deleted cannot be cancelled
    time_0 = datetime.utcnow()
    meta = snowflake_client.cancel_records(all_id)
    time_1 = datetime.utcnow()
    assert meta.n_updated == 3

    rec = snowflake_client.get_records(all_id, include_task=True)

    # created_on hasn't changed
    for r in rec:
        assert r.raw_data.created_on < time_0

    assert rec[0].raw_data.status == RecordStatusEnum.cancelled
    assert rec[1].raw_data.status == RecordStatusEnum.complete
    assert rec[2].raw_data.status == RecordStatusEnum.cancelled
    assert rec[3].raw_data.status == RecordStatusEnum.cancelled
    assert rec[4].raw_data.status == RecordStatusEnum.cancelled
    assert rec[5].raw_data.status == RecordStatusEnum.deleted

    assert rec[0].raw_data.task is None
    assert rec[2].raw_data.task is None
    assert rec[3].raw_data.task is None
    assert rec[4].raw_data.task is None
    assert rec[5].raw_data.task is None

    assert rec[0].raw_data.manager_name is None
    assert rec[2].raw_data.manager_name is None
    assert rec[3].raw_data.manager_name is None
    assert rec[4].raw_data.manager_name is None
    assert rec[5].raw_data.manager_name is None

    assert time_0 < rec[0].raw_data.modified_on < time_1
    assert rec[1].raw_data.modified_on < time_0
    assert time_0 < rec[2].raw_data.modified_on < time_1
    assert time_0 < rec[3].raw_data.modified_on < time_1
    assert rec[4].raw_data.modified_on < time_0
    assert rec[5].raw_data.modified_on < time_0


def test_record_client_cancel_none(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # completed, cancelled, deleted cannot be cancelled
    meta = snowflake_client.cancel_records([])
    assert meta.n_updated == 0


def test_record_client_softdelete(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    time_0 = datetime.utcnow()
    meta = snowflake_client.delete_records(all_id, soft_delete=True)
    time_1 = datetime.utcnow()
    assert meta.n_deleted == 5
    assert meta.deleted_idx == [0, 1, 2, 3, 4]

    rec = snowflake_client.get_records(all_id, include_task=True)

    for r in rec:
        # created_on hasn't changed
        assert r.raw_data.created_on < time_0

        assert r.raw_data.status == RecordStatusEnum.deleted
        assert r.raw_data.task is None

    assert time_0 < rec[0].raw_data.modified_on < time_1
    assert time_0 < rec[1].raw_data.modified_on < time_1
    assert time_0 < rec[2].raw_data.modified_on < time_1
    assert time_0 < rec[3].raw_data.modified_on < time_1
    assert time_0 < rec[4].raw_data.modified_on < time_1
    assert rec[5].raw_data.modified_on < time_0

    # completed and errored records should keep their manager
    assert rec[0].raw_data.manager_name is None
    assert rec[1].raw_data.manager_name is not None
    assert rec[2].raw_data.manager_name is None
    assert rec[3].raw_data.manager_name is not None
    assert rec[4].raw_data.manager_name is None
    assert rec[5].raw_data.manager_name is None


def test_record_socket_undelete(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # only deleted can't be deleted
    time_0 = datetime.utcnow()
    meta = snowflake_client.delete_records(all_id, soft_delete=True)
    assert meta.n_deleted == 5
    assert meta.deleted_idx == [0, 1, 2, 3, 4]

    time_1 = datetime.utcnow()
    meta = snowflake_client.undelete_records(all_id)
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


def test_record_client_delete_1(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    meta = snowflake_client.delete_records(all_id, soft_delete=False)
    assert meta.success
    assert meta.deleted_idx == [0, 1, 2, 3, 4, 5]
    assert meta.n_deleted == 6

    rec = snowflake_client.get_records(all_id, missing_ok=True)
    assert all(x is None for x in rec)


def test_record_client_delete_2(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    # Delete only some records
    all_id = populate_db(storage_socket)

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


def test_record_client_modify(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    time_0 = datetime.utcnow()

    # record 2 is complete - can't change
    meta = snowflake_client.modify_records([all_id[0], all_id[1]], new_tag="new_tag")
    assert meta.n_updated == 1
    assert meta.updated_idx == [0]
    assert meta.error_idx == [1]

    # one of these records in cancelled
    meta = snowflake_client.modify_records([all_id[3], all_id[4]], new_priority=PriorityEnum.high)
    assert meta.n_updated == 1

    rec = snowflake_client.get_records(all_id, include_task=True)

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r.raw_data.created_on < time_0
        assert r.raw_data.modified_on < time_0

    # Waiting
    assert rec[0].raw_data.task.tag == "new_tag"
    assert rec[0].raw_data.task.priority == PriorityEnum.normal

    # completed
    assert rec[1].raw_data.task is None

    # running - not changed
    assert rec[2].raw_data.task.tag == "tag3"
    assert rec[2].raw_data.task.priority == PriorityEnum.normal

    # error
    assert rec[3].raw_data.task.tag == "tag4"
    assert rec[3].raw_data.task.priority == PriorityEnum.high

    # cancelled/deleted
    assert rec[4].raw_data.task is None
    assert rec[5].raw_data.task is None

    # Delete tag
    meta = snowflake_client.modify_records(all_id, delete_tag=True)
    assert meta.n_updated == 2

    rec = snowflake_client.get_records(all_id, include_task=True)

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r.raw_data.created_on < time_0
        assert r.raw_data.modified_on < time_0

    assert rec[0].raw_data.task.tag is None
    assert rec[1].raw_data.task is None
    assert rec[2].raw_data.task.tag == "tag3"
    assert rec[2].raw_data.task.priority == PriorityEnum.normal
    assert rec[3].raw_data.task.tag is None
    assert rec[4].raw_data.task is None
    assert rec[5].raw_data.task is None
