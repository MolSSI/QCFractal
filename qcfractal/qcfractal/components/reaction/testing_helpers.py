from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional, Dict, List, Union, Any

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from qcarchivetesting.helpers import read_record_data
from qcelemental.models import (
    Molecule,
    FailedOperation,
    ComputeError,
    AtomicResult as QCEl_AtomicResult,
    OptimizationResult as QCEl_OptimizationResult,
)

from qcfractal.components.reaction.record_db_models import ReactionRecordORM
from qcfractal.testing_helpers import run_service
from qcportal.reaction import ReactionSpecification, ReactionKeywords
from qcportal.record_models import PriorityEnum, RecordStatusEnum, RecordTask
from qcportal.singlepoint import SinglepointProtocols, QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName

test_specs = [
    ReactionSpecification(
        program="reaction",
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        keywords=ReactionKeywords(),
    ),
    ReactionSpecification(
        program="reaction",
        singlepoint_specification=QCSpecification(
            program="Prog2", driver="energy", method="Hf", basis="def2-TZVP", keywords={"k": "v"}
        ),
        keywords=ReactionKeywords(),
    ),
]


def compare_reaction_specs(
    input_spec: Union[ReactionSpecification, Dict[str, Any]],
    output_spec: Union[ReactionSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = ReactionSpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = ReactionSpecification(**output_spec)

    return input_spec == output_spec


def generate_task_key(task: RecordTask):
    inp_data = task.function_kwargs["input_data"]

    if inp_data["schema_name"] == "qcschema_optimization_input":
        record_type = "optimization"
        mol_hash = inp_data["initial_molecule"]["identifiers"]["molecule_hash"]
    elif inp_data["schema_name"] == "qcschema_input":
        record_type = "singlepoint"
        mol_hash = inp_data["molecule"]["identifiers"]["molecule_hash"]
    else:
        raise RuntimeError(f"Unknown result type: {inp_data['schema_name']}")

    return record_type + "|" + mol_hash


def load_test_data(
    name: str,
) -> Tuple[
    ReactionSpecification, List[Tuple[float, Molecule]], Dict[str, Union[QCEl_AtomicResult, QCEl_OptimizationResult]]
]:
    test_data = read_record_data(name)

    return (
        pydantic.parse_obj_as(ReactionSpecification, test_data["specification"]),
        pydantic.parse_obj_as(List[Tuple[float, Molecule]], test_data["stoichiometry"]),
        pydantic.parse_obj_as(Dict[str, Union[QCEl_AtomicResult, QCEl_OptimizationResult]], test_data["results"]),
    )


def submit_test_data(
    storage_socket: SQLAlchemySocket,
    name: str,
    compute_tag: Optional[str] = "*",
    compute_priority: PriorityEnum = PriorityEnum.normal,
) -> Tuple[int, Dict[str, Union[QCEl_AtomicResult, QCEl_OptimizationResult]]]:
    input_spec, stoich, result = load_test_data(name)
    meta, record_ids = storage_socket.records.reaction.add(
        [stoich], input_spec, compute_tag, compute_priority, None, None, True
    )
    assert meta.success
    assert len(record_ids) == 1
    assert meta.n_inserted == 1

    return record_ids[0], result


def run_test_data(
    storage_socket: SQLAlchemySocket,
    manager_name: ManagerName,
    name: str,
    compute_tag: Optional[str] = "*",
    compute_priority: PriorityEnum = PriorityEnum.normal,
    end_status: RecordStatusEnum = RecordStatusEnum.complete,
):
    record_id, result = submit_test_data(storage_socket, name, compute_tag, compute_priority)

    with storage_socket.session_scope() as session:
        record = session.get(ReactionRecordORM, record_id)
        assert record.status == RecordStatusEnum.waiting

    if end_status == RecordStatusEnum.error:
        failed_op = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )
        result = {x: failed_op for x in result}

    finished, n_optimizations = run_service(storage_socket, manager_name, record_id, generate_task_key, result, 200)
    assert finished

    with storage_socket.session_scope() as session:
        record = session.get(ReactionRecordORM, record_id)
        assert record.status == end_status

    return record_id
