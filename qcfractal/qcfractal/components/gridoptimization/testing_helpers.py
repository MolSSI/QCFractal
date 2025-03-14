from __future__ import annotations

import json
from typing import TYPE_CHECKING, Tuple, Optional, Dict, Union, Any

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from qcelemental.models import Molecule, FailedOperation, ComputeError, OptimizationResult as QCEl_OptimizationResult
from qcelemental.models.procedures import OptimizationProtocols

from qcarchivetesting.helpers import read_record_data
from qcfractal.components.gridoptimization.record_db_models import GridoptimizationRecordORM
from qcfractal.testing_helpers import run_service
from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.optimization import OptimizationSpecification
from qcportal.record_models import PriorityEnum, RecordStatusEnum, RecordTask
from qcportal.singlepoint import SinglepointProtocols, QCSpecification
from qcportal.utils import recursive_normalizer

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


def compare_gridoptimization_specs(
    input_spec: Union[GridoptimizationSpecification, Dict[str, Any]],
    output_spec: Union[GridoptimizationSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = GridoptimizationSpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = GridoptimizationSpecification(**output_spec)

    return input_spec == output_spec


test_specs = [
    GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=QCSpecification(
                program="prog2",
                driver="deferred",
                method="b3lyp",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    ),
    GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=True,
            scans=[
                {"type": "dihedral", "indices": [3, 2, 1, 0], "steps": [-90, -45, 0, 45, 90], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=QCSpecification(
                program="prog2",
                driver="deferred",
                method="b3lyp",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(wavefunction="all", stdout=False),
            ),
        ),
    ),
]


def generate_task_key(task: RecordTask):
    # task is an optimization
    inp_data = task.function_kwargs["input_data"]
    assert inp_data["schema_name"] == "qcschema_optimization_input"

    mol_hash = inp_data["initial_molecule"]["identifiers"]["molecule_hash"]
    constraints = inp_data["keywords"].get("constraints", None)

    # Lookups may depend on floating point values
    constraints = recursive_normalizer(constraints)

    # This is the key in the dictionary of optimization results
    constraints_str = json.dumps(constraints, sort_keys=True)
    return mol_hash + "|" + constraints_str


def load_test_data(name: str) -> Tuple[GridoptimizationSpecification, Molecule, Dict[str, QCEl_OptimizationResult]]:
    test_data = read_record_data(name)

    return (
        pydantic.parse_obj_as(GridoptimizationSpecification, test_data["specification"]),
        pydantic.parse_obj_as(Molecule, test_data["initial_molecule"]),
        pydantic.parse_obj_as(Dict[str, QCEl_OptimizationResult], test_data["results"]),
    )


def submit_test_data(
    storage_socket: SQLAlchemySocket,
    name: str,
    compute_tag: Optional[str] = "*",
    compute_priority: PriorityEnum = PriorityEnum.normal,
) -> Tuple[int, Dict[str, QCEl_OptimizationResult]]:
    input_spec, molecule, result = load_test_data(name)
    meta, record_ids = storage_socket.records.gridoptimization.add(
        [molecule], input_spec, compute_tag, compute_priority, None, None, True
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
        record = session.get(GridoptimizationRecordORM, record_id)
        assert record.status == RecordStatusEnum.waiting

    if end_status == RecordStatusEnum.error:
        failed_op = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )
        result = {x: failed_op for x in result}

    finished, n_optimizations = run_service(storage_socket, manager_name, record_id, generate_task_key, result, 200)
    assert finished

    with storage_socket.session_scope() as session:
        record = session.get(GridoptimizationRecordORM, record_id)
        assert record.status == end_status

    return record_id
