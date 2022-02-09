"""
Tests the services socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting import load_procedure_data
from qcportal.outputstore import OutputStore, OutputTypeEnum
from qcportal.records import FailedOperation, RecordStatusEnum, PriorityEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_service_socket_error(storage_socket: SQLAlchemySocket):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("td_H2O2_psi4_b3lyp")

    # Inject a failed computation
    failed_key = list(result_data_1.keys())[1]
    result_data_1[failed_key] = FailedOperation(
        error={"error_type": "test_error", "error_message": "this is just a test error"},
    )

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low, as_service=True
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_optimizations = run_service_constropt(id_1[0], result_data_1, storage_socket, 20)
    time_1 = datetime.utcnow()

    assert finished is True

    rec = storage_socket.records.torsiondrive.get(
        id_1, include=["*", "compute_history.*", "compute_history.outputs", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.error
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 2  # stdout and error
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.error
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is not None

    outs = rec[0]["compute_history"][-1]["outputs"]
    out0 = OutputStore(**outs[0])
    out1 = OutputStore(**outs[1])

    out_err = out0 if out0.output_type == OutputTypeEnum.error else out1
    assert "did not complete successfully" in out_err.as_json["error_message"]


def test_service_socket_iterate_order(storage_socket: SQLAlchemySocket):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("td_H2O2_psi4_b3lyp")
    input_spec_2, molecules_2, result_data_2 = load_procedure_data("td_H2O2_psi4_pbe")

    # Bit of a hack here
    storage_socket.services._max_active_services = 1

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, as_service=True, tag="*", priority=PriorityEnum.low
    )
    meta_2, id_2 = storage_socket.records.torsiondrive.add(
        [molecules_2], input_spec_2, as_service=True, tag="*", priority=PriorityEnum.normal
    )

    storage_socket.services.iterate_services()

    recs = storage_socket.records.get(id_1 + id_2)
    assert recs[0]["status"] == RecordStatusEnum.waiting
    assert recs[1]["status"] == RecordStatusEnum.running
