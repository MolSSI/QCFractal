from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from qcelemental.models import FailedOperation

from qcfractal.components.records.gridoptimization.testing_helpers import submit_test_data as submit_go_test_data
from qcfractal.components.records.torsiondrive.testing_helpers import submit_test_data as submit_td_test_data
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_constropt
from qcportal.managers import ManagerName
from qcportal.outputstore import OutputStore, OutputTypeEnum
from qcportal.records import RecordStatusEnum, PriorityEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_service_socket_error(storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName):
    id_1, result_data_1 = submit_td_test_data(storage_socket, "td_H2O2_psi4_b3lyp", "test_tag", PriorityEnum.low)

    # Inject a failed computation
    failed_key = list(result_data_1.keys())[1]
    result_data_1[failed_key] = FailedOperation(
        error={"error_type": "test_error", "error_message": "this is just a test error"},
    )

    time_0 = datetime.utcnow()
    finished, n_optimizations = run_service_constropt(storage_socket, activated_manager_name, id_1, result_data_1, 20)
    time_1 = datetime.utcnow()

    assert finished is True

    rec = storage_socket.records.torsiondrive.get(
        [id_1], include=["*", "compute_history.*", "compute_history.outputs", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.error
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 2  # stdout and error
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.error
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is not None

    outs = rec[0]["compute_history"][-1]["outputs"]
    out_err = OutputStore(**outs[OutputTypeEnum.error])
    assert "did not complete successfully" in out_err.as_json["error_message"]


def test_service_socket_iterate_order(storage_socket: SQLAlchemySocket):

    storage_socket.services._max_active_services = 1

    id_1, _ = submit_td_test_data(storage_socket, "td_H2O2_psi4_b3lyp", "*", PriorityEnum.normal)
    id_2, _ = submit_go_test_data(storage_socket, "go_H3NS_psi4_pbe", "*", PriorityEnum.high)

    storage_socket.services.iterate_services()

    recs = storage_socket.records.get([id_1, id_2])
    assert recs[0]["status"] == RecordStatusEnum.waiting
    assert recs[1]["status"] == RecordStatusEnum.running
