from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Optional, Dict, List, Union, Any

import pydantic
from qcelemental.models import Molecule, FailedOperation, ComputeError, OptimizationResult
from qcelemental.models.procedures import OptimizationProtocols

from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting.helpers import read_record_data
from qcportal.records import PriorityEnum, RecordStatusEnum
from qcportal.records.optimization import OptimizationSpecification
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification
from qcportal.records.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


test_specs = [
    TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(1, 2, 3, 4)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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
    TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(7, 2, 9, 4), (5, 11, 3, 10)],
            grid_spacing=[30, 45],
            dihedral_ranges=[[-90, 90], [0, 180]],
            energy_decrease_thresh=1.0,
            energy_upper_limit=0.05,
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


def compare_torsiondrive_specs(
    input_spec: Union[TorsiondriveSpecification, Dict[str, Any]],
    output_spec: Union[TorsiondriveSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = TorsiondriveSpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = TorsiondriveSpecification(**output_spec)

    return input_spec == output_spec


def load_test_data(name: str) -> Tuple[TorsiondriveSpecification, List[Molecule], Dict[str, OptimizationResult]]:
    test_data = read_record_data(name)

    return (
        pydantic.parse_obj_as(TorsiondriveSpecification, test_data["specification"]),
        pydantic.parse_obj_as(List[Molecule], test_data["molecule"]),
        pydantic.parse_obj_as(Dict[str, OptimizationResult], test_data["result"]),
    )


def submit_test_data(
    storage_socket: SQLAlchemySocket,
    name: str,
    tag: Optional[str] = "*",
    priority: PriorityEnum = PriorityEnum.normal,
) -> Tuple[int, Dict[str, OptimizationResult]]:

    input_spec, molecules, result = load_test_data(name)
    meta, record_ids = storage_socket.records.torsiondrive.add([molecules], input_spec, True, tag, priority)
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
