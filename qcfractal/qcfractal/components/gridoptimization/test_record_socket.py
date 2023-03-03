from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.auth import UserInfo, GroupInfo
from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification, SinglepointProtocols
from .testing_helpers import compare_gridoptimization_specs, test_specs, load_test_data, generate_task_key

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


@pytest.mark.parametrize("spec", test_specs)
def test_gridoptimization_socket_add_get(storage_socket: SQLAlchemySocket, spec: GridoptimizationSpecification):
    hooh = load_molecule_data("peroxide2")
    h3ns = load_molecule_data("go_H3NS")

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.gridoptimization.add([hooh, h3ns], spec, "tag1", PriorityEnum.low, None, None)
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
    meta, id1 = storage_socket.records.gridoptimization.add([hooh], spec, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.gridoptimization.add([hooh], spec, "*", PriorityEnum.normal, None, None)
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
    meta, id1 = storage_socket.records.gridoptimization.add([mol1, mol2], spec1, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.gridoptimization.add([mol1, mol2], spec2, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id1 == id2


@pytest.mark.parametrize(
    "test_data_name",
    [
        "go_C4H4N2OS_mopac_pm6",
        "go_H2O2_psi4_b3lyp",
        "go_H3NS_psi4_pbe",
    ],
)
def test_gridoptimization_socket_run(
    storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, molecules_1, result_data_1 = load_test_data(test_data_name)

    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.users.add(UserInfo(username="submit_user", role="submit", groups=["group1"], enabled=True))

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, "submit_user", "group1"
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_optimizations = run_service(
        storage_socket, activated_manager_name, id_1[0], generate_task_key, result_data_1, 100
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

    out = storage_socket.records.gridoptimization.get_single_output_uncompressed(
        rec[0]["id"], rec[0]["compute_history"][-1]["id"], "stdout"
    )
    assert "Grid optimization finished successfully!" in out

    assert len(rec[0]["optimizations"]) == n_optimizations

    for o in rec[0]["optimizations"]:
        optr = storage_socket.records.optimization.get([o["optimization_id"]])
        assert optr[0]["energies"][-1] == o["energy"]
