"""
Tests the tasks socket (claiming & returning data)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.optimization.testing_helpers import load_test_data as load_opt_test_data
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_test_data as load_sp_test_data
from qcfractal.components.torsiondrive.testing_helpers import (
    load_test_data as load_td_test_data,
    generate_task_key as generate_td_task_key,
)
from qcfractal.testing_helpers import run_service
from qcfractalcompute.compress import compress_result
from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerName
from qcportal.record_models import PriorityEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session

input_spec_1, molecule_1, result_data_1 = load_sp_test_data("sp_psi4_water_energy")
input_spec_2, molecule_2, result_data_2 = load_sp_test_data("sp_psi4_water_gradient")
input_spec_3, molecule_3, result_data_3 = load_sp_test_data("sp_psi4_water_hessian")
input_spec_4, molecule_4, result_data_4 = load_opt_test_data("opt_psi4_benzene")
input_spec_5, molecule_5, result_data_5 = load_sp_test_data("sp_psi4_benzene_energy_1")
input_spec_6, molecule_6, result_data_6 = load_sp_test_data("sp_psi4_benzene_energy_2")
input_spec_7, molecule_7, result_data_7 = load_sp_test_data("sp_rdkit_benzene_energy")


def test_task_socket_update_manager_time(storage_socket: SQLAlchemySocket, session: Session):
    # Test that claiming & returning updates the modified_on time of the manager
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")

    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}

    mid_1 = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["tag1"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.low, None, None, True
    )

    # claim up to two tasks
    t0 = now_at_utc()
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag1"], 2)
    t1 = now_at_utc()
    assert len(tasks) == 1

    # Check assignments
    session.expire_all()

    m1 = session.get(ComputeManagerORM, mid_1)
    assert m1.claimed == 1
    assert t0 < m1.modified_on < t1

    # Claiming but getting nothing also updates the time
    t0 = now_at_utc()
    tasks0 = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag1"], 2)
    t1 = now_at_utc()
    assert len(tasks0) == 0

    session.expire_all()
    m1 = session.get(ComputeManagerORM, mid_1)
    assert m1.claimed == 1
    assert t0 < m1.modified_on < t1

    # Return the task data
    t0 = now_at_utc()
    rmeta = storage_socket.tasks.update_finished(
        mname1.fullname,
        {
            tasks[0]["id"]: compress_result(result_data_1.dict()),
        },
    )
    t1 = now_at_utc()

    assert rmeta.n_accepted == 1
    session.expire_all()
    m1 = session.get(ComputeManagerORM, mid_1)
    assert t0 < m1.modified_on < t1


def test_task_socket_claim_mixed(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host2", uuid="2234-5678-1234-5678")
    mname3 = ManagerName(cluster="test_cluster", hostname="a_host3", uuid="3234-5678-1234-5678")
    mname4 = ManagerName(cluster="test_cluster", hostname="a_host4", uuid="4234-5678-1234-5678")

    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    mprog2 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    mprog3 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    mprog4 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"], "rdkit": ["v1.0"]}

    mid_1 = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["tag1"],
    )
    mid_2 = storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs=mprog2,
        compute_tags=["*"],
    )
    mid_3 = storage_socket.managers.activate(
        name_data=mname3,
        manager_version="v2.0",
        username="bill",
        programs=mprog3,
        compute_tags=["tag3", "tag1"],
    )
    mid_4 = storage_socket.managers.activate(
        name_data=mname4,
        manager_version="v2.0",
        username="bill",
        programs=mprog4,
        compute_tags=["tag3", "*"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.low, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag2", PriorityEnum.normal, None, None, True
    )
    meta, id_3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, "tag1", PriorityEnum.high, None, None, True
    )
    meta, id_4 = storage_socket.records.optimization.add(
        [molecule_4], input_spec_4, "tag3", PriorityEnum.normal, None, None, True
    )
    meta, id_5 = storage_socket.records.singlepoint.add(
        [molecule_5], input_spec_5, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_6 = storage_socket.records.singlepoint.add(
        [molecule_6], input_spec_6, "tag6", PriorityEnum.high, None, None, True
    )
    meta, id_7 = storage_socket.records.singlepoint.add(
        [molecule_7], input_spec_7, "tag1", PriorityEnum.high, None, None, True
    )

    all_id = id_1 + id_2 + id_3 + id_4 + id_5 + id_6 + id_7
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    # claim up to two tasks
    # should find the high and normal priority one, but not the one
    # requiring rdkit
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag1"], 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[2].task.id
    assert tasks[1]["id"] == recs[4].task.id

    # manager 4 should find tag3, and then #6 (highest priority left)
    tasks = storage_socket.tasks.claim_tasks(mname4.fullname, mprog4, ["tag3", "*"], 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[3].task.id
    assert tasks[1]["id"] == recs[5].task.id

    # manager3 should find the only remaining tag1 that isn't rdkit
    tasks = storage_socket.tasks.claim_tasks(mname3.fullname, mprog3, ["tag3", "tag1"])
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[0].task.id

    # manager 2 only finds #2 - doesn't have rdkit
    tasks = storage_socket.tasks.claim_tasks(mname2.fullname, mprog2, ["*"], 20)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[1].task.id

    # manager 4 can finally get the last one
    tasks = storage_socket.tasks.claim_tasks(mname4.fullname, mprog4, ["tag3", "*"], 20)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[6].task.id

    # Check assignments
    session.expire_all()
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    assert recs[0].manager_name == mname3.fullname
    assert recs[1].manager_name == mname2.fullname
    assert recs[2].manager_name == mname1.fullname
    assert recs[3].manager_name == mname4.fullname
    assert recs[4].manager_name == mname1.fullname
    assert recs[5].manager_name == mname4.fullname
    assert recs[6].manager_name == mname4.fullname

    assert session.get(ComputeManagerORM, mid_1).claimed == 2
    assert session.get(ComputeManagerORM, mid_2).claimed == 1
    assert session.get(ComputeManagerORM, mid_3).claimed == 1
    assert session.get(ComputeManagerORM, mid_4).claimed == 3


def test_task_socket_claim_priority(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["tag1"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.low, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, "tag1", PriorityEnum.high, None, None, True
    )
    meta, id_4 = storage_socket.records.optimization.add(
        [molecule_4],
        input_spec_4,
        "tag1",
        PriorityEnum.normal,
        None,
        None,
        True,
    )
    meta, id_5 = storage_socket.records.singlepoint.add(
        [molecule_5], input_spec_5, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_6 = storage_socket.records.singlepoint.add(
        [molecule_6], input_spec_6, "tag1", PriorityEnum.high, None, None, True
    )

    all_id = id_1 + id_2 + id_3 + id_4 + id_5 + id_6
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    # highest priority should be first, then by modified date
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag1"], 3)
    assert len(tasks) == 3
    assert tasks[0]["id"] == recs[2].task.id
    assert tasks[1]["id"] == recs[5].task.id
    assert tasks[2]["id"] == recs[1].task.id

    # Now normal then low
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag1"], 3)
    assert len(tasks) == 3
    assert tasks[0]["id"] == recs[3].task.id
    assert tasks[1]["id"] == recs[4].task.id
    assert tasks[2]["id"] == recs[0].task.id


def test_task_socket_claim_tag(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["tag3", "tag1"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "TAg1", PriorityEnum.normal, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag2", PriorityEnum.normal, None, None, True
    )
    meta, id_3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, "*", PriorityEnum.normal, None, None, True
    )
    meta, id_4 = storage_socket.records.optimization.add(
        [molecule_4], input_spec_4, "tag3", PriorityEnum.normal, None, None, True
    )
    meta, id_5 = storage_socket.records.singlepoint.add(
        [molecule_5], input_spec_5, "tag1", PriorityEnum.normal, None, None, True
    )

    all_id = id_1 + id_2 + id_3 + id_4 + id_5
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    # tag3 should be first, then tag1
    # Change capitalization to test case insensitivity
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["Tag3", "Tag1"], 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[3].task.id
    assert tasks[1]["id"] == recs[0].task.id

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag3", "tag1"], 3)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[4].task.id


def test_task_socket_claim_tag_wildcard(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["TAG3", "*"],
    )

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag2", PriorityEnum.normal, None, None, True
    )
    meta, id_3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, "*", PriorityEnum.normal, None, None, True
    )
    meta, id_4 = storage_socket.records.optimization.add(
        [molecule_4], input_spec_4, "taG3", PriorityEnum.normal, None, None, True
    )
    meta, id_5 = storage_socket.records.singlepoint.add(
        [molecule_5], input_spec_5, "tag1", PriorityEnum.normal, None, None, True
    )

    all_id = id_1 + id_2 + id_3 + id_4 + id_5
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    # tag3 should be first, then any task (in order)
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag3", "*"], 2)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[3].task.id
    assert tasks[1]["id"] == recs[0].task.id

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag3", "*"], 3)
    assert len(tasks) == 3
    assert tasks[0]["id"] == recs[1].task.id
    assert tasks[1]["id"] == recs[2].task.id
    assert tasks[2]["id"] == recs[4].task.id


def test_task_socket_claim_tag_wildcard_strict(postgres_server, pytestconfig):

    pg_harness = postgres_server.get_new_harness("claim_tag_wildcard_strict")
    encoding = pytestconfig.getoption("--client-encoding")
    with QCATestingSnowflake(pg_harness, encoding=encoding, extra_config={"strict_queue_tags": True}) as snowflake:
        storage_socket = snowflake.get_storage_socket()

        mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
        mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
        storage_socket.managers.activate(
            name_data=mname1,
            manager_version="v2.0",
            username="bill",
            programs=mprog1,
            compute_tags=["TAG3", "*"],
        )

        meta, id_1 = storage_socket.records.singlepoint.add(
            [molecule_1], input_spec_1, "tag1", PriorityEnum.normal, None, None, True
        )
        meta, id_2 = storage_socket.records.singlepoint.add(
            [molecule_2], input_spec_2, "tag2", PriorityEnum.normal, None, None, True
        )
        meta, id_3 = storage_socket.records.singlepoint.add(
            [molecule_3], input_spec_3, "*", PriorityEnum.normal, None, None, True
        )
        meta, id_4 = storage_socket.records.optimization.add(
            [molecule_4], input_spec_4, "taG3", PriorityEnum.normal, None, None, True
        )
        meta, id_5 = storage_socket.records.singlepoint.add(
            [molecule_5], input_spec_5, "tag1", PriorityEnum.normal, None, None, True
        )

        all_id = id_1 + id_2 + id_3 + id_4 + id_5

        client = snowflake.client()
        recs = client.get_records(all_id, include=["task"])

        # tag3 should be first, then only the * task (in order)
        tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag3", "*"], 2)
        assert len(tasks) == 2
        assert tasks[0]["id"] == recs[3].task.id
        assert tasks[1]["id"] == recs[2].task.id

        tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["tag3", "*"], 3)
        assert len(tasks) == 0


def test_task_socket_claim_program(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")

    # Managers should not send programs with uppercase characters, but be forgiving
    mprog1 = {"QCengine": ["unknown"], "psi4": ["unknown"], "Geometric": ["V3.0"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["*"],
    )

    meta, id_7 = storage_socket.records.singlepoint.add(
        [molecule_7], input_spec_7, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag1", PriorityEnum.normal, None, None, True
    )

    all_id = id_7 + id_1 + id_2
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    # claim all tasks. But it shouldn't claim #7
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["*"], 100)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[1].task.id
    assert tasks[1]["id"] == recs[2].task.id


def test_task_socket_claim_program_subset(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"], "rdkit": ["unknown"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["*"],
    )

    meta, id_7 = storage_socket.records.singlepoint.add(
        [molecule_7], input_spec_7, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.normal, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag1", PriorityEnum.normal, None, None, True
    )

    all_id = id_7 + id_1 + id_2
    recs = []
    for rid in all_id:
        rec = session.get(BaseRecordORM, rid)
        recs.append(rec)

    # claim all tasks. Shouldn't claim #7 (rdkit not included)
    claim_prog = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, claim_prog, ["*"], 100)
    assert len(tasks) == 2
    assert tasks[0]["id"] == recs[1].task.id
    assert tasks[1]["id"] == recs[2].task.id

    # now claim the rdkit task
    claim_prog = {"qcengine": ["unknown"], "rdkit": ["unknown"]}
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, claim_prog, ["*"], 100)
    assert len(tasks) == 1
    assert tasks[0]["id"] == recs[0].task.id


def test_task_socket_claim_program_missing(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"], "rdkit": ["unknown"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["*"],
    )

    claim_prog = {}

    with pytest.raises(ComputeManagerError, match="did not send any valid programs to claim"):
        storage_socket.tasks.claim_tasks(mname1.fullname, claim_prog, ["*"], 100)


def test_task_socket_claim_tags_missing(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"], "rdkit": ["unknown"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["*"],
    )

    claim_prog = {"qcengine": ["unknown"], "rdkit": ["unknown"]}

    with pytest.raises(ComputeManagerError, match="did not send any valid queue tags to claim"):
        storage_socket.tasks.claim_tasks(mname1.fullname, claim_prog, [], 100)


def test_task_socket_claim_service_order(storage_socket: SQLAlchemySocket, session: Session):
    input_spec_1, molecules_1, result_data_1 = load_td_test_data("td_H2O2_psi4_pbe")
    input_spec_2, molecules_2, result_data_2 = load_td_test_data("td_H2O2_mopac_pm6")

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")

    mprog1 = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"], "mopac": ["unknown"]}

    mid_1 = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=mprog1,
        compute_tags=["*"],
    )

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, True, "tag1", PriorityEnum.low, None, None, True
    )
    meta_2, id_2 = storage_socket.records.torsiondrive.add(
        [molecules_2], input_spec_2, True, "tag1", PriorityEnum.low, None, None, True
    )

    id_1 = id_1[0]
    id_2 = id_2[0]

    finished, n_records = run_service(storage_socket, mname1, id_1, generate_td_task_key, result_data_1, 1)
    finished, n_records = run_service(storage_socket, mname1, id_2, generate_td_task_key, result_data_2, 1)
    assert finished is False

    # Iterate the second service first
    with storage_socket.session_scope() as session:
        service_orm = session.get(BaseRecordORM, id_2).service
        finished = storage_socket.records.iterate_service(session, service_orm)
        assert finished is False

    with storage_socket.session_scope() as session:
        service_orm = session.get(BaseRecordORM, id_1).service
        finished = storage_socket.records.iterate_service(session, service_orm)
        assert finished is False

    # The manager should claim tasks belonging to that first service, not the second
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, mprog1, ["*"], 1)
    assert len(tasks) == 1

    with storage_socket.session_scope() as session:
        service_orm = session.get(BaseRecordORM, id_1).service
        dep_ids = [d.record_id for d in service_orm.dependencies]
        assert tasks[0]["record_id"] in dep_ids

        service_orm_2 = session.get(BaseRecordORM, id_2).service
        dep_ids_2 = [d.record_id for d in service_orm_2.dependencies]
        assert tasks[0]["record_id"] not in dep_ids_2
