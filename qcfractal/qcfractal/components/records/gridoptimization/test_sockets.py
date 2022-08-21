from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting import load_molecule_data
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.records.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.records.singlepoint import QCSpecification, SinglepointProtocols
from .testing_helpers import compare_gridoptimization_specs, test_specs, load_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


@pytest.mark.parametrize("spec", test_specs)
def test_gridoptimization_socket_add_get(storage_socket: SQLAlchemySocket, spec: GridoptimizationSpecification):
    hooh = load_molecule_data("peroxide2")
    h3ns = load_molecule_data("go_H3NS")

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.gridoptimization.add([hooh, h3ns], spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.gridoptimization.get(id, include=["*", "initial_molecule", "service"])

    assert len(recs) == 2
    for r in recs:
        assert r["record_type"] == "gridoptimization"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_gridoptimization_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    assert recs[0]["initial_molecule"]["identifiers"]["molecule_hash"] == hooh.get_hash()
    assert recs[1]["initial_molecule"]["identifiers"]["molecule_hash"] == h3ns.get_hash()


def test_gridoptimization_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
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
    )

    hooh = load_molecule_data("peroxide2")
    meta, id1 = storage_socket.records.gridoptimization.add([hooh], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.gridoptimization.add([hooh], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_gridoptimization_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=True,
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
                protocols=SinglepointProtocols(wavefunction="none"),
            ),
        ),
    )

    spec2 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationSpecification(
            program="optPROG1",
            keywords={"k": "value"},
            qc_specification=QCSpecification(
                program="prOG2",
                driver="deferred",
                method="b3LYP",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(stdout=True),
            ),
        ),
    )

    mol1 = load_molecule_data("go_H3NS")
    mol2 = load_molecule_data("peroxide2")
    meta, id1 = storage_socket.records.gridoptimization.add([mol1, mol2], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.gridoptimization.add([mol1, mol2], spec2, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id1 == id2


@pytest.mark.parametrize(
    "test_data_name",
    [
        "go_C4H4N2OS_psi4_b3lyp-d3bj",
        "go_H2O2_psi4_b3lyp-d3bj",
        "go_H3NS_psi4_blyp",
    ],
)
def test_gridoptimization_socket_run(
    storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, molecules_1, result_data_1 = load_test_data(test_data_name)

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_optimizations = run_service_constropt(
        storage_socket, activated_manager_name, id_1[0], result_data_1, 100
    )
    time_1 = datetime.utcnow()

    assert finished is True

    rec = storage_socket.records.gridoptimization.get(
        id_1, include=["*", "compute_history.*", "compute_history.outputs", "optimizations", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["modified_on"] < time_1
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 1
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is None
    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"]["stdout"])
    assert "Grid optimization finished successfully!" in out.as_string

    assert len(rec[0]["optimizations"]) == n_optimizations

    for o in rec[0]["optimizations"]:
        optr = storage_socket.records.optimization.get([o["optimization_id"]])
        assert optr[0]["energies"][-1] == o["energy"]
