from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional, Union, Dict, Any

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from qcelemental.models import Molecule, FailedOperation, ComputeError, AtomicResult

from qcarchivetesting.helpers import read_record_data
from qcfractal.components.singlepoint.record_db_models import SinglepointRecordORM
from qcfractalcompute.compress import compress_result
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.singlepoint import SinglepointProtocols, QCSpecification, SinglepointDriver
from qcportal.utils import now_at_utc

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
    find_existing: bool = True,
) -> Tuple[int, AtomicResult]:
    input_spec, molecule, result = load_test_data(name)
    meta, record_ids = storage_socket.records.singlepoint.add(
        [molecule], input_spec, tag, priority, None, None, find_existing
    )
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
    time_0 = now_at_utc()
    record_id, result = submit_test_data(storage_socket, name, tag, priority)
    time_1 = now_at_utc()

    with storage_socket.session_scope() as session:
        record = session.get(SinglepointRecordORM, record_id)
        assert record.status == RecordStatusEnum.waiting

    if end_status == RecordStatusEnum.error:
        result = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )

    manager_programs = storage_socket.managers.get([manager_name.fullname])[0]["programs"]
    tasks = storage_socket.tasks.claim_tasks(manager_name.fullname, manager_programs, [tag], limit=100)
    assert len(tasks) == 1

    result_compressed = compress_result(result.dict())
    result_dict = {tasks[0]["id"]: result_compressed}

    storage_socket.tasks.update_finished(manager_name.fullname, result_dict)
    time_2 = now_at_utc()

    with storage_socket.session_scope() as session:
        record = session.get(SinglepointRecordORM, record_id)
        assert record.status == end_status
        print(time_0)
        print(record.created_on)
        print(time_1)
        assert time_0 < record.created_on < time_1
        assert time_1 < record.modified_on < time_2
        assert time_1 < record.compute_history[0].modified_on < time_2

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
