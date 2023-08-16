from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional, Dict, Union, Any

import pydantic
from qcelemental.models import Molecule, FailedOperation, ComputeError, AtomicResult

from qcarchivetesting.helpers import read_record_data
from qcfractal.components.manybody.record_db_models import ManybodyRecordORM
from qcfractal.testing_helpers import run_service
from qcportal.manybody import ManybodySpecification, ManybodyKeywords
from qcportal.record_models import PriorityEnum, RecordStatusEnum, RecordTask
from qcportal.singlepoint import SinglepointProtocols, QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName

test_specs = [
    ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    ),
    ManybodySpecification(
        keywords=ManybodyKeywords(max_nbody=1, bsse_correction="none"),
        program="manybody",
        singlepoint_specification=QCSpecification(
            program="Prog2",
            driver="energy",
            method="Hf",
            basis="def2-tzVP",
            keywords={"k": "value"},
        ),
    ),
    ManybodySpecification(
        keywords=ManybodyKeywords(max_nbody=1, bsse_correction="none"),
        program="manybody",
        singlepoint_specification=QCSpecification(
            program="Prog3",
            driver="properties",
            method="Hf",
            basis="sto-3g",
            keywords={"k": "v"},
        ),
    ),
]


def compare_manybody_specs(
    input_spec: Union[ManybodySpecification, Dict[str, Any]],
    output_spec: Union[ManybodySpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = ManybodySpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = ManybodySpecification(**output_spec)

    return input_spec == output_spec


def generate_task_key(task: RecordTask):
    # task is a singlepoint
    inp_data = task.function_kwargs["input_data"]
    assert inp_data["schema_name"] in "qcschema_input"

    mol_hash = inp_data["molecule"]["identifiers"]["molecule_hash"]
    return "singlepoint" + "|" + mol_hash


def load_test_data(name: str) -> Tuple[ManybodySpecification, Molecule, Dict[str, AtomicResult]]:
    test_data = read_record_data(name)

    return (
        pydantic.parse_obj_as(ManybodySpecification, test_data["specification"]),
        pydantic.parse_obj_as(Molecule, test_data["molecule"]),
        pydantic.parse_obj_as(Dict[str, AtomicResult], test_data["results"]),
    )


def submit_test_data(
    storage_socket: SQLAlchemySocket,
    name: str,
    tag: Optional[str] = "*",
    priority: PriorityEnum = PriorityEnum.normal,
) -> Tuple[int, Dict[str, AtomicResult]]:

    input_spec, molecule, result = load_test_data(name)
    meta, record_ids = storage_socket.records.manybody.add([molecule], input_spec, tag, priority, None, None, True)
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

    with storage_socket.session_scope() as session:
        record = session.get(ManybodyRecordORM, record_id)
        assert record.status == RecordStatusEnum.waiting

    if end_status == RecordStatusEnum.error:
        failed_op = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )
        result = {x: failed_op for x in result}

    finished, n_optimizations = run_service(storage_socket, manager_name, record_id, generate_task_key, result, 200)
    assert finished

    with storage_socket.session_scope() as session:
        record = session.get(ManybodyRecordORM, record_id)
        assert record.status == end_status

    return record_id
