from __future__ import annotations

from typing import TYPE_CHECKING

from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.record_socket import _collect_nested_outputs
from qcfractal.components.singlepoint.testing_helpers import submit_procedure_data as submit_sp_procedure_data
from qcfractal.testing_helpers import mname1
from qcfractalcompute.compress import compress_result
from qcportal.qcschema_v1 import FailedOperation, ComputeError
from qcportal.record_models import OutputTypeEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_collect_nested_outputs_no_nesting():
    # error.extras doesn't have a "failed_result" at all (the common case for
    # a plain program failure, eg psi4 raising directly)
    assert _collect_nested_outputs(None, "stdout") == []
    assert _collect_nested_outputs({}, "stdout") == []
    assert _collect_nested_outputs({"something_else": 123}, "stdout") == []


def test_collect_nested_outputs_single_level():
    # what geomeTRIC/optking produce: error.extras = {"failed_result": {...}}
    extras = {"failed_result": {"stdout": "geometric log", "stderr": "geometric err"}}
    assert _collect_nested_outputs(extras, "stdout") == ["geometric log"]
    assert _collect_nested_outputs(extras, "stderr") == ["geometric err"]

    # missing/empty keys are just skipped
    assert _collect_nested_outputs({"failed_result": {}}, "stdout") == []


def test_collect_nested_outputs_multi_level():
    # a failed_result whose own "error" carries another nested failed_result
    # (eg a procedure-of-procedures failure)
    extras = {
        "failed_result": {
            "stdout": "outer log",
            "error": {
                "error_type": "unknown_error",
                "error_message": "inner failure",
                "extras": {
                    "failed_result": {
                        "stdout": "inner log",
                    }
                },
            },
        }
    }

    assert _collect_nested_outputs(extras, "stdout") == ["outer log", "inner log"]


def test_record_socket_update_failed_task_nested_stdout(storage_socket: SQLAlchemySocket):
    """
    Regression test: a FailedOperation whose stdout/stderr is nested under
    error.extras["failed_result"] (as produced by procedures like geomeTRIC/optking
    wrapping a failing sub-computation) should still end up as the record's stdout/stderr,
    rather than being silently lost.
    """

    manager_programs = {"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["v3.0"]}
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=manager_programs,
        compute_tags=["tag1"],
    )

    record_id, _ = submit_sp_procedure_data(storage_socket, "sp_psi4_water_energy", "tag1")

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, manager_programs, ["tag1"])
    assert len(tasks) == 1

    result = FailedOperation(
        error=ComputeError(
            error_type="unknown_error",
            error_message="geomeTRIC failed",
            extras={
                "failed_result": {
                    "stdout": "geometric log",
                    "stderr": "geometric err",
                }
            },
        ),
    )

    storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: compress_result(result.model_dump())})

    with storage_socket.session_scope() as session:
        record = session.get(BaseRecordORM, record_id)
        assert record.status == RecordStatusEnum.error

        outputs = record.compute_history[-1].outputs
        assert outputs[OutputTypeEnum.stdout].get_output() == "geometric log"
        assert outputs[OutputTypeEnum.stderr].get_output() == "geometric err"
