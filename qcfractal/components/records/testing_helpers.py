"""
Tests the general record socket
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from qcelemental.models import FailedOperation, ComputeError

from qcfractal.components.records.optimization.testing_helpers import load_test_data as load_opt_test_data
from qcfractal.components.records.singlepoint.testing_helpers import load_test_data as load_sp_test_data
from qcfractal.testing_helpers import mname1
from qcportal.records import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def populate_records_status(storage_socket: SQLAlchemySocket):
    """
    Populates the db with tasks in all statuses
    """

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0", "rdkit": None, "geometric": None},
        tags=["tag1", "tag2", "tag3", "tag6"],
    )

    input_spec_0, molecule_0, result_data_0 = load_opt_test_data("psi4_methane_opt_sometraj")
    input_spec_1, molecule_1, result_data_1 = load_sp_test_data("psi4_water_gradient")
    input_spec_2, molecule_2, result_data_2 = load_sp_test_data("psi4_water_hessian")
    input_spec_3, molecule_3, result_data_3 = load_opt_test_data("psi4_benzene_opt")
    input_spec_4, molecule_4, result_data_4 = load_sp_test_data("rdkit_water_energy")
    input_spec_5, molecule_5, result_data_5 = load_sp_test_data("psi4_benzene_energy_2")
    input_spec_6, molecule_6, result_data_6 = load_sp_test_data("psi4_water_energy")

    meta, id_0 = storage_socket.records.optimization.add([molecule_0], input_spec_0, "tag0", PriorityEnum.normal)
    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.high)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.high)
    meta, id_3 = storage_socket.records.optimization.add([molecule_3], input_spec_3, "tag3", PriorityEnum.high)
    meta, id_4 = storage_socket.records.singlepoint.add([molecule_4], input_spec_4, "tag4", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag5", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag6", PriorityEnum.normal)
    all_id = id_0 + id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    # 0 = waiting   1 = complete   2 = running
    # 3 = error     4 = cancelled  5 = deleted
    # 6 = invalid

    # claim only the ones we want to be complete, running, or error (1, 2, 3, 6)
    # 6 needs to be complete to be invalidated
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=4)
    assert len(tasks) == 4

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))

    # we don't send back the one we want to be 'running' still (#2)
    storage_socket.tasks.update_finished(
        mname1.fullname,
        {
            # tasks[1] is left running (corresponds to record 2)
            tasks[0]["id"]: result_data_1,
            tasks[2]["id"]: fop,
            tasks[3]["id"]: result_data_6,
        },
    )

    # Add some more entries to the history of #3 (failing)
    for i in range(4):
        meta = storage_socket.records.reset(id_3)
        assert meta.success
        tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=1)
        assert len(tasks) == 1
        assert tasks[0]["tag"] == "tag3"

        storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: fop})

    meta = storage_socket.records.cancel(id_4)
    assert meta.n_updated == 1
    meta = storage_socket.records.delete(id_5)
    assert meta.n_deleted == 1
    meta = storage_socket.records.invalidate(id_6)
    assert meta.n_updated == 1

    rec = storage_socket.records.get(all_id, include=["status"])
    assert rec[0]["status"] == RecordStatusEnum.waiting
    assert rec[1]["status"] == RecordStatusEnum.complete
    assert rec[2]["status"] == RecordStatusEnum.running
    assert rec[3]["status"] == RecordStatusEnum.error
    assert rec[4]["status"] == RecordStatusEnum.cancelled
    assert rec[5]["status"] == RecordStatusEnum.deleted
    assert rec[6]["status"] == RecordStatusEnum.invalid

    return all_id
