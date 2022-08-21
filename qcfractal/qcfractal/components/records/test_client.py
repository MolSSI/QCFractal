"""
Tests the general record socket
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.records.optimization.testing_helpers import (
    run_test_data as run_opt_test_data,
    submit_test_data as submit_opt_test_data,
)
from qcfractal.components.records.singlepoint.testing_helpers import (
    run_test_data as run_sp_test_data,
    submit_test_data as submit_sp_test_data,
)
from qcfractal.components.records.testing_helpers import populate_records_status
from qcfractal.components.records.torsiondrive.testing_helpers import submit_test_data as submit_td_test_data
from qcfractal.testing_helpers import TestingSnowflake
from qcfractaltesting import test_users
from qcportal import PortalRequestError
from qcportal.managers import ManagerName
from qcportal.records import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


def test_record_client_get(
    storage_socket: SQLAlchemySocket, snowflake_client: PortalClient, activated_manager_name: ManagerName
):
    id1 = run_sp_test_data(storage_socket, activated_manager_name, "sp_psi4_benzene_energy_1")
    id2, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")
    all_id = [id1, id2]

    r = snowflake_client.get_records(all_id)
    assert len(r)
    assert all_id == [x.raw_data.id for x in r]
    assert [x.raw_data.task is None for x in r]
    assert len(r[0].raw_data.compute_history) == 1
    assert len(r[1].raw_data.compute_history) == 0

    assert r[0].raw_data.compute_history[0].outputs is None
    assert [x.raw_data.task is None for x in r]

    r = snowflake_client.get_records(all_id, include=["outputs", "task"])
    assert r[0].raw_data.compute_history[0].outputs is not None
    assert r[0].raw_data.task is None
    assert r[1].raw_data.task is not None


def test_record_client_get_missing(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):

    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")

    all_id = [id1, id2]

    with pytest.raises(PortalRequestError, match=r"Could not find all requested"):
        snowflake_client.get_records([all_id[0], 9999, all_id[1]])

    r = snowflake_client.get_records([all_id[0], 9999, all_id[1]], missing_ok=True)
    assert r[1] is None
    assert r[0].raw_data.id == all_id[0]
    assert r[2].raw_data.id == all_id[1]


def test_record_client_get_empty(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    submit_opt_test_data(storage_socket, "opt_psi4_benzene")

    r = snowflake_client.get_records([])
    assert r == []


def test_record_client_query_parents_children(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    id1 = run_opt_test_data(storage_socket, activated_manager_name, "opt_psi4_benzene")

    opt_rec = snowflake_client.get_optimizations(id1, include=["trajectory"])
    assert opt_rec.status == RecordStatusEnum.complete

    traj_id = [x.singlepoint_id for x in opt_rec.raw_data.trajectory]
    assert len(traj_id) > 0

    # Query for records containing a record as a parent
    query_res = snowflake_client.query_records(parent_id=opt_rec.id)
    assert {x.id for x in query_res} == set(traj_id)

    # Query for records containing a record as a child
    query_res = snowflake_client.query_records(child_id=traj_id[0])
    assert query_res.current_meta.n_found == 1
    assert list(query_res)[0].id == opt_rec.id


def test_record_client_add_comment(secure_snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_2")
    id3, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_3")
    id4, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")
    all_id = [id1, id2, id3, id4]

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
    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_2")
    id3, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_3")
    id4, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")
    all_id = [id1, id2, id3, id4]

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
    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")

    meta = snowflake_client.add_comment([id1, 9999], comment="test")
    assert not meta.success
    assert meta.n_updated == 1
    assert meta.n_errors == 1
    assert meta.updated_idx == [0]
    assert meta.error_idx == [1]
    assert "does not exist" in meta.errors[0][1]


def test_record_client_modify(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    all_id = populate_records_status(storage_socket)

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

    svc_id, _ = submit_td_test_data(storage_socket, "td_H2O2_psi4_hf", "test_tag", PriorityEnum.high)

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
