from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional, Dict, Union, Any

import pydantic
from qcelemental.models import Molecule, FailedOperation, ComputeError, OptimizationResult
from qcelemental.models.procedures import OptimizationProtocols

from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting.helpers import read_record_data
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.records.optimization import OptimizationSpecification
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification

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


def load_test_data(name: str) -> Tuple[GridoptimizationSpecification, Molecule, Dict[str, OptimizationResult]]:
    test_data = read_record_data(name)

    return (
        pydantic.parse_obj_as(GridoptimizationSpecification, test_data["specification"]),
        pydantic.parse_obj_as(Molecule, test_data["molecule"]),
        pydantic.parse_obj_as(Dict[str, OptimizationResult], test_data["result"]),
    )


def submit_test_data(
    storage_socket: SQLAlchemySocket,
    name: str,
    tag: Optional[str] = "*",
    priority: PriorityEnum = PriorityEnum.normal,
) -> Tuple[int, Dict[str, OptimizationResult]]:

    input_spec, molecule, result = load_test_data(name)
    meta, record_ids = storage_socket.records.gridoptimization.add([molecule], input_spec, tag, priority)
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
        failed_op = FailedOperation(
            error=ComputeError(error_type="test_error", error_message="this is just a test error"),
        )
        result = {x: failed_op for x in result}

    finished, n_optimizations = run_service_constropt(storage_socket, manager_name, record_id, result, 200)
    assert finished

    record = storage_socket.records.get([record_id], include=["status"])[0]
    assert record["status"] == end_status

    return record_id
