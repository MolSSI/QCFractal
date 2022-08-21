from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional, Union, Dict, Any

import pydantic
from qcelemental.models import Molecule, FailedOperation, ComputeError, AtomicResult

from qcfractaltesting.helpers import read_record_data
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification, SinglepointDriver

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName

test_specs = [
    QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    ),
    QCSpecification(
        program="Prog2",
        driver=SinglepointDriver.gradient,
        method="Hf",
        basis="def2-TZVP",
        keywords={"k": "v"},
    ),
    QCSpecification(
        program="Prog3",
        driver=SinglepointDriver.hessian,
        method="pbe0",
        basis="",
        keywords={"o": 1, "v": 2.123},
        protocols=SinglepointProtocols(stdout=False, wavefunction="orbitals_and_eigenvalues"),
    ),
    QCSpecification(
        program="ProG4",
        driver=SinglepointDriver.hessian,
        method="pbe",
        basis=None,
        protocols=SinglepointProtocols(stdout=False, wavefunction="return_results"),
    ),
]


def load_test_data(name: str) -> Tuple[QCSpecification, Molecule, AtomicResult]:
    test_data = read_record_data(name)

    return (
        pydantic.parse_obj_as(QCSpecification, test_data["specification"]),
        pydantic.parse_obj_as(Molecule, test_data["molecule"]),
        pydantic.parse_obj_as(AtomicResult, test_data["result"]),
    )


def submit_test_data(
    storage_socket: SQLAlchemySocket,
    name: str,
    tag: Optional[str] = "*",
    priority: PriorityEnum = PriorityEnum.normal,
) -> Tuple[int, AtomicResult]:

    input_spec, molecule, result = load_test_data(name)
    meta, record_ids = storage_socket.records.singlepoint.add([molecule], input_spec, tag, priority)
    assert meta.success
    assert len(record_ids) == 1
    assert meta.n_inserted == 1

    return record_ids[0], result


def run_test_data(
    storage_socket: SQLAlchemySocket,
    manager_name: ManagerName,
    name: str,
    tag: Optional[str] = "*",
    priority: PriorityEnum = PriorityEnum.normal,
    end_status: RecordStatusEnum = RecordStatusEnum.complete,
):
    record_id, result = submit_test_data(storage_socket, name, tag, priority)

    record = storage_socket.records.get([record_id])[0]
    assert record["status"] == RecordStatusEnum.waiting

    if end_status == RecordStatusEnum.error:
        result = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )

    tasks = storage_socket.tasks.claim_tasks(manager_name.fullname, limit=100)
    assert len(tasks) == 1
    result_dict = {tasks[0]["id"]: result}
    storage_socket.tasks.update_finished(manager_name.fullname, result_dict)

    record = storage_socket.records.get([record_id], include=["status"])[0]
    assert record["status"] == end_status

    return record_id


def compare_singlepoint_specs(
    input_spec: Union[QCSpecification, Dict[str, Any]],
    output_spec: Union[QCSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = QCSpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = QCSpecification(**output_spec)

    return input_spec == output_spec
