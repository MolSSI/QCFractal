"""
Tests the general record socket
"""
from __future__ import annotations

import random
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.testing_helpers import populate_db, mname1
from qcportal.exceptions import MissingDataError
from qcportal.records import PriorityEnum, RecordStatusEnum, RecordQueryBody

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

    r = storage_socket.records.get([all_id[3]], include=["*", "compute_history"])
    ch = r[0]["compute_history"]
    assert len(ch) == 5

    assert ch[0]["manager_name"] == mname1.fullname
    assert ch[1]["manager_name"] == mname1.fullname
    assert ch[2]["manager_name"] == mname1.fullname
    assert ch[3]["manager_name"] == mname1.fullname
    assert ch[4]["manager_name"] == mname1.fullname

    assert ch[0]["modified_on"] < ch[1]["modified_on"]
    assert ch[1]["modified_on"] < ch[2]["modified_on"]
    assert ch[2]["modified_on"] < ch[3]["modified_on"]
    assert ch[3]["modified_on"] < ch[4]["modified_on"]

    assert all("outputs" not in x for x in ch)

    r = storage_socket.records.get([all_id[3]], include=["compute_history.*", "compute_history.outputs"])
    ch = r[0]["compute_history"]
    assert len(ch) == 5
    assert all(x["outputs"] is not None for x in ch)


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
    assert meta.n_found == 3

    # modified before/after
    all_records_sorted = sorted(all_records, key=lambda x: x["modified_on"])
    meta, data = storage_socket.records.query(RecordQueryBody(modified_before=all_records_sorted[3]["modified_on"]))
    assert meta.n_found == 3

    meta, data = storage_socket.records.query(RecordQueryBody(modified_after=all_records_sorted[3]["modified_on"]))
    assert meta.n_found == 3

    # Record type
    meta, data = storage_socket.records.query(RecordQueryBody(record_type=["singlepoint"]))
    assert meta.n_found == 6

    meta, data = storage_socket.records.query(RecordQueryBody(record_type=["optimization"]))
    assert meta.n_found == 1

    meta, data = storage_socket.records.query(RecordQueryBody(record_type=["singlepoint", "optimization"]))
    assert meta.n_found == 7

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
    assert meta.n_found == 0

    meta, data = storage_socket.records.query(
        RecordQueryBody(record_type=["optimization"], status=[RecordStatusEnum.waiting])
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


def test_record_socket_add_comment(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    # comments not retrieved by default
    rec = storage_socket.records.get(all_id)
    for r in rec:
        assert "comments" not in r

    rec = storage_socket.records.get(all_id, include=["*", "comments"])
    for r in rec:
        assert r["comments"] == []

    time_0 = datetime.utcnow()
    meta = storage_socket.records.add_comment(
        [all_id[1], all_id[3]], username="test_user", comment="This is a test comment"
    )
    time_1 = datetime.utcnow()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    meta = storage_socket.records.add_comment(
        [all_id[2], all_id[3]], username=None, comment="This is a test comment without a user"
    )
    time_2 = datetime.utcnow()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    rec = storage_socket.records.get(all_id, include=["*", "comments"])
    assert rec[0]["comments"] == []
    assert rec[4]["comments"] == []
    assert rec[5]["comments"] == []
    assert len(rec[1]["comments"]) == 1
    assert len(rec[2]["comments"]) == 1
    assert len(rec[3]["comments"]) == 2

    assert time_0 < rec[1]["comments"][0]["timestamp"] < time_1
    assert time_1 < rec[2]["comments"][0]["timestamp"] < time_2
    assert time_0 < rec[3]["comments"][0]["timestamp"] < time_1
    assert time_1 < rec[3]["comments"][1]["timestamp"] < time_2
    assert rec[1]["comments"][0]["username"] == "test_user"
    assert rec[3]["comments"][0]["username"] == "test_user"
    assert rec[2]["comments"][0]["username"] is None
    assert rec[3]["comments"][1]["username"] is None

    assert rec[1]["comments"][0]["comment"] == "This is a test comment"
    assert rec[3]["comments"][0]["comment"] == "This is a test comment"
    assert rec[2]["comments"][0]["comment"] == "This is a test comment without a user"
    assert rec[3]["comments"][1]["comment"] == "This is a test comment without a user"


def test_record_socket_add_comment_badid(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    meta = storage_socket.records.add_comment([all_id[1], 9999, all_id[3]], username=None, comment="test")
    assert not meta.success
    assert meta.n_updated == 2
    assert meta.n_errors == 1
    assert meta.updated_idx == [0, 2]
    assert meta.error_idx == [1]
    assert "does not exist" in meta.errors[0][1]


def test_record_socket_modify(storage_socket: SQLAlchemySocket):
    all_id = populate_db(storage_socket)

    time_0 = datetime.utcnow()

    # record 1 is complete - can't change
    meta = storage_socket.records.modify([all_id[0], all_id[1]], new_tag="new_tag")
    assert meta.n_updated == 1

    # one of these records in cancelled
    meta = storage_socket.records.modify([all_id[3], all_id[4]], new_priority=PriorityEnum.low)
    assert meta.n_updated == 1

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0
        assert r["modified_on"] < time_0

    # 0 - waiting
    assert rec[0]["task"]["tag"] == "new_tag"
    assert rec[0]["task"]["priority"] == PriorityEnum.normal

    # 1 - completed
    assert rec[1]["task"] is None

    # 2 - running, but not changed
    assert rec[2]["task"]["tag"] == "tag2"
    assert rec[2]["task"]["priority"] == PriorityEnum.high

    # 3 - error
    assert rec[3]["task"]["tag"] == "tag3"
    assert rec[3]["task"]["priority"] == PriorityEnum.low

    # 4/5/6 - cancelled/deleted/invalid
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None
    assert rec[6]["task"] is None

    rec = storage_socket.records.get(all_id, include=["*", "task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r["created_on"] < time_0
        assert r["modified_on"] < time_0

    assert rec[1]["task"] is None
    assert rec[2]["task"]["priority"] == PriorityEnum.high
    assert rec[4]["task"] is None
    assert rec[5]["task"] is None
    assert rec[6]["task"] is None
