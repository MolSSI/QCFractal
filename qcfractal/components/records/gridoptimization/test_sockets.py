"""
Tests the gridoptimization record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.keywords import KeywordSet
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.gridoptimization import (
    GridoptimizationSpecification,
    GridoptimizationInputSpecification,
    GridoptimizationKeywords,
    GridoptimizationQueryBody,
)
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQCInputSpecification,
    OptimizationProtocols,
)
from qcportal.records.singlepoint import (
    SinglepointProtocols,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Dict, Any, Union


def compare_gridoptimization_specs(
    input_spec: Union[GridoptimizationInputSpecification, Dict[str, Any]],
    full_spec: Union[GridoptimizationSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = GridoptimizationInputSpecification(**input_spec)
    if isinstance(full_spec, GridoptimizationSpecification):
        full_spec = full_spec.dict()

    full_spec.pop("id")
    full_spec.pop("optimization_specification_id")
    full_spec["optimization_specification"].pop("id")
    full_spec["optimization_specification"].pop("qc_specification_id")
    full_spec["optimization_specification"]["qc_specification"].pop("id")
    full_spec["optimization_specification"]["qc_specification"].pop("keywords_id")
    full_spec["optimization_specification"]["qc_specification"]["keywords"].pop("id")
    trimmed_spec = GridoptimizationInputSpecification(**full_spec)
    return input_spec == trimmed_spec


_test_specs = [
    GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    ),
    GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=True,
            scans=[
                {"type": "dihedral", "indices": [3, 2, 1, 0], "steps": [-90, -45, 0, 45, 90], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all", stdout=False),
            ),
        ),
    ),
]


@pytest.mark.parametrize("spec", _test_specs)
def test_gridoptimization_socket_add_get(storage_socket: SQLAlchemySocket, spec: GridoptimizationInputSpecification):
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


def test_gridoptimization_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    mol1 = load_molecule_data("go_H3NS")
    mol2 = load_molecule_data("peroxide2")

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([mol2])

    # Now add records
    meta, id = storage_socket.records.gridoptimization.add(
        [mol1, mol2, mol2, mol1], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 2

    recs = storage_socket.records.gridoptimization.get(id, include=["initial_molecule"])
    assert len(recs) == 4
    assert recs[0]["id"] == recs[3]["id"]
    assert recs[1]["id"] == recs[2]["id"]

    rec_mols = {x["initial_molecule"]["id"] for x in recs}
    _, mol_ids_2 = storage_socket.molecules.add([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_gridoptimization_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
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
    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=True,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="none"),
            ),
        ),
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optPROG1",
            keywords={"k": "value"},
            qc_specification=OptimizationQCInputSpecification(
                program="prOG2",
                method="b3LYP",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
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


def test_gridoptimization_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("go_H2O2_psi4_b3lyp")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("go_H2O2_psi4_pbe")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("go_C4H4N2OS_psi4_b3lyp-d3bj")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("go_H3NS_psi4_pbe")

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta_2, id_2 = storage_socket.records.gridoptimization.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta_3, id_3 = storage_socket.records.gridoptimization.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )
    meta_4, id_4 = storage_socket.records.gridoptimization.add(
        [molecule_4], input_spec_4, tag="*", priority=PriorityEnum.normal
    )
    assert meta_1.success and meta_2.success and meta_3.success and meta_4.success

    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_program=["psi4"]))
    assert meta.n_found == 4

    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_program=["nothing"]))
    assert meta.n_found == 0

    _, init_mol_id = storage_socket.molecules.add([molecule_1, molecule_2, molecule_3, molecule_4])
    meta, td = storage_socket.records.gridoptimization.query(
        GridoptimizationQueryBody(initial_molecule_id=[init_mol_id[0], 9999])
    )
    assert meta.n_found == 2

    # query for optimization program
    meta, td = storage_socket.records.gridoptimization.query(
        GridoptimizationQueryBody(optimization_program=["geometric"])
    )
    assert meta.n_found == 4

    # query for optimization program
    meta, td = storage_socket.records.gridoptimization.query(
        GridoptimizationQueryBody(optimization_program=["geometric123"])
    )
    assert meta.n_found == 0

    # query for basis
    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_basis=["sTO-3g"]))
    assert meta.n_found == 3

    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_basis=[None]))
    assert meta.n_found == 0

    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_basis=[""]))
    assert meta.n_found == 0

    # query for method
    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_method=["b3lyP"]))
    assert meta.n_found == 1

    kw_id = td[0]["specification"]["optimization_specification"]["qc_specification"]["keywords_id"]
    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(qc_keywords_id=[kw_id]))
    assert meta.n_found == 3

    # Query by default returns everything
    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody())
    assert meta.n_found == 4

    # Query by default (with a limit)
    meta, td = storage_socket.records.gridoptimization.query(GridoptimizationQueryBody(limit=1))
    assert meta.n_found == 4
    assert meta.n_returned == 1


@pytest.mark.parametrize(
    "test_data_name",
    [
        "go_C4H4N2OS_psi4_b3lyp-d3bj",
        "go_H2O2_psi4_b3lyp-d3bj",
        "go_H2O2_psi4_b3lyp",
        "go_H2O2_psi4_blyp",
        "go_H2O2_psi4_bp86",
        "go_H2O2_psi4_hf",
        "go_H2O2_psi4_pbe0-d3bj",
        "go_H2O2_psi4_pbe0",
        "go_H2O2_psi4_pbe",
        "go_H3NS_psi4_b3lyp-d3bj",
        "go_H3NS_psi4_b3lyp",
        "go_H3NS_psi4_blyp",
        "go_H3NS_psi4_bp86",
        "go_H3NS_psi4_hf",
        "go_H3NS_psi4_pbe0-d3bj",
        "go_H3NS_psi4_pbe0",
        "go_H3NS_psi4_pbe",
    ],
)
def test_gridoptimization_socket_run(storage_socket: SQLAlchemySocket, test_data_name: str):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data(test_data_name)

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_optimizations = run_service_constropt(id_1[0], result_data_1, storage_socket, 100)
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
    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"][0])
    assert "Grid optimization finished successfully!" in out.as_string

    assert len(rec[0]["optimizations"]) == n_optimizations

    for o in rec[0]["optimizations"]:
        optr = storage_socket.records.optimization.get([o["optimization_id"]])
        assert optr[0]["energies"][-1] == o["energy"]
