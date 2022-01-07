"""
Tests the tasks socket (claiming & returning data)
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from qcelemental.models import ComputeError

from qcportal.managers import ManagerName
from qcportal.outputstore import OutputTypeEnum, OutputStore, CompressionEnum
from qcportal.records import FailedOperation, PriorityEnum, RecordStatusEnum
from qcfractal.testing import load_procedure_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_task_socket_fullworkflow_success(storage_socket: SQLAlchemySocket):
    # Need a manager to claim the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    input_spec1, molecule1, result_data1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec2, molecule2, result_data2 = load_procedure_data("psi4_fluoroethane_wfn")

    meta1, id1 = storage_socket.records.singlepoint.add(input_spec1, [molecule1], "tag1", PriorityEnum.normal)
    meta2, id2 = storage_socket.records.singlepoint.add(input_spec2, [molecule2], "tag1", PriorityEnum.normal)

    time_1 = datetime.utcnow()

    result_map = {id1[0]: result_data1, id2[0]: result_data2}

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    # Should be claimed in the manager table
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["claimed"] == 2

    # Status should be updated
    records = storage_socket.records.get(id1 + id2, include=["*", "task"])
    for rec in records:
        assert rec["status"] == RecordStatusEnum.running
        assert rec["manager_name"] == mname1.fullname
        assert rec["task"] is not None
        assert rec["compute_history"] == []
        assert rec["modified_on"] > time_1
        assert rec["created_on"] < time_1

    rmeta = storage_socket.tasks.update_finished(
        mname1.fullname,
        {tasks[0]["id"]: result_map[tasks[0]["record_id"]], tasks[1]["id"]: result_map[tasks[1]["record_id"]]},
    )

    assert rmeta.n_accepted == 2
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == sorted(id1 + id2)

    records = storage_socket.records.get(id1 + id2, include=["*", "task"])

    for rec in records:
        # Status should be complete
        assert rec["status"] == RecordStatusEnum.complete

        # Tasks should be deleted
        assert rec["task"] is None

        # Manager names should have been assigned
        assert rec["manager_name"] == mname1.fullname

        # Modified_on should be updated, but not created_on
        assert rec["created_on"] < time_1
        assert rec["modified_on"] > time_1

        # History should have been saved
        assert len(rec["compute_history"]) == 1
        assert rec["compute_history"][0]["manager_name"] == mname1.fullname
        assert rec["compute_history"][0]["modified_on"] > time_1
        assert rec["compute_history"][0]["status"] == RecordStatusEnum.complete

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 2
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 0
    assert manager[0]["claimed"] == 2


def test_task_socket_fullworkflow_error(storage_socket: SQLAlchemySocket):
    # Need a manager to claim the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )
    input_spec1, molecule1, result_data1 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec2, molecule2, result_data2 = load_procedure_data("psi4_peroxide_energy_fail_basis")

    meta1, id1 = storage_socket.records.singlepoint.add(input_spec1, [molecule1], "tag1", PriorityEnum.normal)
    meta2, id2 = storage_socket.records.singlepoint.add(input_spec2, [molecule2], "tag1", PriorityEnum.normal)

    time_1 = datetime.utcnow()

    result_map = {id1[0]: result_data1, id2[0]: result_data2}

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    # Should be claimed in the manager table
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["claimed"] == 2

    # Status should be updated
    records = storage_socket.records.get(id1 + id2, include=["*", "task"])
    for rec in records:
        assert rec["status"] == RecordStatusEnum.running
        assert rec["manager_name"] == mname1.fullname
        assert rec["task"] is not None
        assert rec["compute_history"] == []
        assert rec["modified_on"] > time_1
        assert rec["created_on"] < time_1

    rmeta = storage_socket.tasks.update_finished(
        mname1.fullname,
        {tasks[0]["id"]: result_map[tasks[0]["record_id"]], tasks[1]["id"]: result_map[tasks[1]["record_id"]]},
    )

    assert rmeta.n_accepted == 2
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == sorted(id1 + id2)

    records = storage_socket.records.get(id1 + id2, include=["*", "task"])

    for rec in records:
        # Status should be error
        assert rec["status"] == RecordStatusEnum.error

        # Tasks should not be deleted
        assert rec["task"] is not None

        # Manager names should have been assigned
        assert rec["manager_name"] == mname1.fullname

        # Modified_on should be updated, but not created_on
        assert rec["created_on"] < time_1
        assert rec["modified_on"] > time_1

        # History should have been saved
        assert len(rec["compute_history"]) == 1
        assert rec["compute_history"][0]["manager_name"] == mname1.fullname
        assert rec["compute_history"][0]["modified_on"] > time_1
        assert rec["compute_history"][0]["status"] == RecordStatusEnum.error

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 0
    assert manager[0]["failures"] == 2
    assert manager[0]["rejected"] == 0
    assert manager[0]["claimed"] == 2


def test_task_socket_fullworkflow_error_retry(storage_socket: SQLAlchemySocket):
    # Need a manager to claim the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )
    input_spec1, molecule1, result_data1 = load_procedure_data("psi4_benzene_energy_1")

    meta1, id1 = storage_socket.records.singlepoint.add(input_spec1, [molecule1], "tag1", PriorityEnum.normal)

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))

    # Sends back an error. Do it a few times
    time_0 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)
    rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: fop})
    storage_socket.records.reset(id1)

    time_1 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)
    rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: fop})
    storage_socket.records.reset(id1)

    time_2 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)
    rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: fop})
    storage_socket.records.reset(id1)

    # Now succeed
    time_3 = datetime.utcnow()
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)
    rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: result_data1})
    time_4 = datetime.utcnow()

    records = storage_socket.records.get(id1, include=["*", "task", "compute_history.*", "compute_history.outputs"])
    hist = records[0]["compute_history"]
    assert len(hist) == 4

    for h in hist:
        assert h["manager_name"] == mname1.fullname
        assert len(h["outputs"]) == 1

    assert time_0 < hist[0]["modified_on"] < time_1
    assert time_1 < hist[1]["modified_on"] < time_2
    assert time_2 < hist[2]["modified_on"] < time_3
    assert time_3 < hist[3]["modified_on"] < time_4

    assert hist[0]["status"] == RecordStatusEnum.error
    assert hist[1]["status"] == RecordStatusEnum.error
    assert hist[2]["status"] == RecordStatusEnum.error
    assert hist[3]["status"] == RecordStatusEnum.complete

    assert hist[3]["outputs"][0]["output_type"] == OutputTypeEnum.stdout
    assert hist[2]["outputs"][0]["output_type"] == OutputTypeEnum.error
    assert hist[1]["outputs"][0]["output_type"] == OutputTypeEnum.error
    assert hist[0]["outputs"][0]["output_type"] == OutputTypeEnum.error

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 1
    assert manager[0]["failures"] == 3
    assert manager[0]["rejected"] == 0
    assert manager[0]["claimed"] == 4


def test_task_socket_compressed_outputs_success(storage_socket: SQLAlchemySocket):
    # Need a manager to claim the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    input_spec1, molecule1, result_data1 = load_procedure_data("psi4_benzene_energy_1")
    meta1, id1 = storage_socket.records.singlepoint.add(input_spec1, [molecule1], "tag1", PriorityEnum.normal)
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    # Compress the outputs
    compressed_output = OutputStore.compress(OutputTypeEnum.stdout, result_data1.stdout, CompressionEnum.lzma, 5)
    if result_data1.extras is None:
        result_data1.__dict__["extras"] = {}
    result_data1.extras["_qcfractal_compressed_outputs"] = [compressed_output.dict()]
    original_stdout = result_data1.__dict__.pop("stdout")

    rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: result_data1})

    assert rmeta.n_accepted == 1
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == id1

    records = storage_socket.records.get(id1, include=["*", "task", "compute_history.*", "compute_history.outputs"])
    out = OutputStore(**records[0]["compute_history"][0]["outputs"][0])
    assert out.as_string == original_stdout
