"""
Tests the tasks socket (claiming & returning data)
"""

from datetime import datetime
from qcfractal.components.records.singlepoint.db_models import ResultORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal.components.managers import ManagerName
from qcfractal.testing import load_procedure_data
from qcfractal.interface.models import RecordStatusEnum, PriorityEnum


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
        configuration={"key": "value"},
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
    sp_records = storage_socket.records.get(id1 + id2, include=["*", "task", "compute_history"])
    for spr in sp_records:
        assert spr["status"] == RecordStatusEnum.running
        assert spr["manager_name"] == mname1.fullname
        assert spr["task"] is not None
        assert spr["compute_history"] == []
        assert spr["modified_on"] > time_1
        assert spr["created_on"] < time_1

    rmeta = storage_socket.tasks.update_completed(
        mname1.fullname,
        {tasks[0]["id"]: result_map[tasks[0]["record_id"]], tasks[1]["id"]: result_map[tasks[1]["record_id"]]},
    )

    assert rmeta.n_accepted == 2
    assert rmeta.n_rejected == 0
    assert rmeta.accepted_ids == sorted(id1 + id2)

    sp_records = storage_socket.records.get(id1 + id2, include=["*", "task", "compute_history"])

    for spr in sp_records:
        # Status should be complete
        assert spr["status"] == RecordStatusEnum.complete

        # Tasks should be deleted
        assert spr["task"] is None

        # Manager names should have been assigned
        assert spr["manager_name"] == mname1.fullname

        # Modified_on should be updated, but not created_on
        assert spr["created_on"] < time_1
        assert spr["modified_on"] > time_1

        # History should have been saved
        assert len(spr["compute_history"]) == 1
        assert spr["compute_history"][0]["manager_name"] == mname1.fullname
        assert spr["compute_history"][0]["modified_on"] > time_1
        assert spr["compute_history"][0]["status"] == RecordStatusEnum.complete

        assert spr["compute_history_latest"]["manager_name"] == mname1.fullname
        assert spr["compute_history_latest"]["modified_on"] > time_1
        assert spr["compute_history_latest"]["status"] == RecordStatusEnum.complete
        assert spr["compute_history_latest"]["id"] == spr["compute_history_latest_id"]

    # Make sure manager info was updated
    manager = storage_socket.managers.get([mname1.fullname])
    assert manager[0]["successes"] == 2
    assert manager[0]["failures"] == 0
    assert manager[0]["rejected"] == 0
    assert manager[0]["claimed"] == 2
