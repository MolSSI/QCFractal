"""
Tests the general record socket
"""

from __future__ import annotations

import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.optimization.testing_helpers import (
    run_test_data as run_opt_test_data,
    submit_test_data as submit_opt_test_data,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import (
    run_test_data as run_sp_test_data,
    submit_test_data as submit_sp_test_data,
)
from qcfractal.components.testing_helpers import populate_records_status
from qcfractal.components.torsiondrive.testing_helpers import submit_test_data as submit_td_test_data
from qcportal import PortalRequestError
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.utils import now_at_utc


def test_record_client_get(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    id1 = run_sp_test_data(storage_socket, activated_manager_name, "sp_psi4_benzene_energy_1")
    id2, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")
    all_id = [id1, id2]

    r = snowflake_client.get_records(all_id)
    assert len(r)
    assert all_id == [x.id for x in r]
    assert [x.task is None for x in r]
    assert r[0].compute_history_ is None
    assert r[1].compute_history_ is None

    assert [x.task is None for x in r]

    r = snowflake_client.get_records(all_id, include=["compute_history", "task"])
    assert len(r[0].compute_history_) == 1
    assert len(r[1].compute_history_) == 0
    assert r[0].task is None
    assert r[1].task is not None


def test_record_client_get_missing(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")

    all_id = [id1, id2]

    with pytest.raises(PortalRequestError, match=r"Could not find all requested"):
        snowflake_client.get_records([all_id[0], 9999, all_id[1]])

    r = snowflake_client.get_records([all_id[0], 9999, all_id[1]], missing_ok=True)
    assert r[1] is None
    assert r[0].id == all_id[0]
    assert r[2].id == all_id[1]


def test_record_client_get_empty(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    submit_opt_test_data(storage_socket, "opt_psi4_benzene")

    r = snowflake_client.get_records([])
    assert r == []


def test_record_client_query_parents_children(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    id1 = run_opt_test_data(storage_socket, activated_manager_name, "opt_psi4_benzene")

    opt_rec = snowflake_client.get_optimizations(id1, include=["trajectory"])
    assert opt_rec.status == RecordStatusEnum.complete

    traj_id = [x.id for x in opt_rec.trajectory]
    assert len(traj_id) > 0

    # Query for records containing a record as a parent
    query_res = snowflake_client.query_records(parent_id=opt_rec.id)
    assert {x.id for x in query_res} == set(traj_id)

    # Query for records containing a record as a child
    query_res = snowflake_client.query_records(child_id=traj_id[0])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
    assert query_res_l[0].id == opt_rec.id


def test_record_client_add_comment(secure_snowflake: QCATestingSnowflake):
    storage_socket = secure_snowflake.get_storage_socket()

    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_2")
    id3, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_3")
    id4, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")
    all_id = [id1, id2, id3, id4]

    # comments not retrieved by default
    rec = client.get_records(all_id)
    for r in rec:
        assert r.comments_ is None

    rec = client.get_records(all_id, include=["comments"])
    for r in rec:
        assert r.comments_ == []

    time_0 = now_at_utc()
    meta = client.add_comment([all_id[1], all_id[3]], comment="This is a test comment")
    time_1 = now_at_utc()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    meta = client.add_comment([all_id[2], all_id[3]], comment="This is another test comment")
    time_2 = now_at_utc()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    rec = client.get_records(all_id, include=["comments"])
    assert rec[0].comments == []
    assert len(rec[1].comments) == 1
    assert len(rec[2].comments) == 1
    assert len(rec[3].comments) == 2

    assert time_0 < rec[1].comments[0].timestamp < time_1
    assert time_1 < rec[2].comments[0].timestamp < time_2
    assert time_0 < rec[3].comments[0].timestamp < time_1
    assert time_1 < rec[3].comments[1].timestamp < time_2
    assert rec[1].comments[0].username == "admin_user"
    assert rec[3].comments[0].username == "admin_user"
    assert rec[2].comments[0].username == "admin_user"
    assert rec[3].comments[1].username == "admin_user"

    assert rec[1].comments[0].comment == "This is a test comment"
    assert rec[3].comments[0].comment == "This is a test comment"
    assert rec[2].comments[0].comment == "This is another test comment"
    assert rec[3].comments[1].comment == "This is another test comment"


def test_record_client_add_comment_nouser(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id2, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_2")
    id3, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_3")
    id4, _ = submit_opt_test_data(storage_socket, "opt_psi4_benzene")
    all_id = [id1, id2, id3, id4]

    time_0 = now_at_utc()
    meta = snowflake_client.add_comment([all_id[1], all_id[3]], comment="This is a test comment")
    time_1 = now_at_utc()
    assert meta.success
    assert meta.n_updated == 2
    assert meta.updated_idx == [0, 1]

    rec = snowflake_client.get_records(all_id, include=["comments"])
    assert len(rec[1].comments) == 1
    assert len(rec[3].comments) == 1

    assert time_0 < rec[1].comments[0].timestamp < time_1
    assert time_0 < rec[3].comments[0].timestamp < time_1
    assert rec[1].comments[0].username is None
    assert rec[3].comments[0].username is None

    assert rec[1].comments[0].comment == "This is a test comment"
    assert rec[3].comments[0].comment == "This is a test comment"


def test_record_client_add_comment_badid(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id1, _ = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_1")

    meta = snowflake_client.add_comment([id1, 9999], comment="test")
    assert not meta.success
    assert meta.n_updated == 1
    assert meta.n_errors == 1
    assert meta.updated_idx == [0]
    assert meta.error_idx == [1]
    assert "does not exist" in meta.errors[0][1]


def test_record_client_modify(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    all_id = populate_records_status(storage_socket)

    time_0 = now_at_utc()

    # record 2 is complete - can't change
    meta = snowflake_client.modify_records([all_id[0], all_id[1]], new_compute_tag="new_tag")
    assert meta.n_updated == 1
    assert meta.updated_idx == [0]
    assert meta.error_idx == [1]

    # one of these records in cancelled
    meta = snowflake_client.modify_records([all_id[3], all_id[4]], new_compute_priority=PriorityEnum.low)
    assert meta.n_updated == 1

    rec = snowflake_client.get_records(all_id, include=["task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r.created_on < time_0
        assert r.modified_on < time_0

    # 0 - waiting
    assert rec[0].task.compute_tag == "new_tag"
    assert rec[0].task.compute_priority == PriorityEnum.normal

    # 1 - completed
    assert rec[1].task is None

    # 2 - running - not changed
    assert rec[2].task.compute_tag == "tag2"
    assert rec[2].task.compute_priority == PriorityEnum.high

    # 3 - error
    assert rec[3].task.compute_tag == "tag3"
    assert rec[3].task.compute_priority == PriorityEnum.low

    # 4/5/6 - cancelled/deleted/invalid
    assert rec[4].task is None
    assert rec[5].task is None
    assert rec[6].task is None

    rec = snowflake_client.get_records(all_id, include=["task"])

    # created_on and modified_on hasn't changed
    for r in rec:
        assert r.created_on < time_0
        assert r.modified_on < time_0

    assert rec[1].task is None
    assert rec[2].task.compute_priority == PriorityEnum.high
    assert rec[4].task is None
    assert rec[5].task is None
    assert rec[6].task is None


def test_record_client_modify_service(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    rec_id, _ = submit_td_test_data(storage_socket, "td_H2O2_mopac_pm6", "test_tag", PriorityEnum.high)

    with storage_socket.session_scope() as s:
        storage_socket.services.iterate_services(s)

        svc_id = s.get(BaseRecordORM, rec_id).service.id
        storage_socket.services._iterate_service(s, svc_id)

    with storage_socket.session_scope() as session:
        rec = session.get(BaseRecordORM, rec_id)
        tasks = [x.record.task for x in rec.service.dependencies]

        assert len(tasks) > 0
        assert all(x.compute_tag == "test_tag" for x in tasks)
        assert all(x.compute_priority == PriorityEnum.high for x in tasks)

    # Modify service priority and tag
    meta = snowflake_client.modify_records(rec_id, new_compute_tag="new_tag", new_compute_priority=PriorityEnum.low)
    assert meta.n_updated == 1
    assert meta.n_children_updated > 0

    rec = snowflake_client.get_records(rec_id)
    assert rec.service.compute_tag == "new_tag"
    assert rec.service.compute_priority == PriorityEnum.low

    # Also changed all the dependencies
    assert len(rec.service.dependencies) > 0
    for opt in rec.service.dependencies:
        r = snowflake_client.get_records(opt.record_id, include=["task"])
        assert r.task.compute_tag == "new_tag"
        assert r.task.compute_priority == PriorityEnum.low


def test_record_client_query_owner(secure_snowflake: QCATestingSnowflake):
    submit_client = secure_snowflake.client("submit_user", test_users["submit_user"]["pw"])
    admin_client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    submit_uid = submit_client.get_user().id

    m = Molecule(
        symbols=["h"],
        geometry=[0, 0, 0],
    )

    _, ids_1 = submit_client.add_singlepoints(m, "prog1", "energy", "b3lyp", "sto-3g", {}, owner_group=None)
    _, ids_2 = submit_client.add_singlepoints(m, "prog2", "energy", "b3lyp", "sto-3g", {}, owner_group="group1")

    _, ids_3 = admin_client.add_singlepoints(m, "prog3", "energy", "b3lyp", "sto-3g", {}, owner_group=None)
    _, ids_4 = admin_client.add_singlepoints(m, "prog4", "energy", "b3lyp", "sto-3g", {}, owner_group="group1")

    query_res = admin_client.query_records(owner_user="submit_user")
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = admin_client.query_records(owner_user=[submit_uid])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = admin_client.query_records(owner_group="group1")
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = admin_client.query_records(owner_user=["admin_user"], owner_group=["group1"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = admin_client.query_records(owner_user=["admin_user"], owner_group=["missing"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0
