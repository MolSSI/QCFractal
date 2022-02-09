"""
Tests the torsiondrive record socket
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
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQCInputSpecification,
    OptimizationProtocols,
)
from qcportal.records.singlepoint import (
    SinglepointProtocols,
)
from qcportal.records.torsiondrive import (
    TorsiondriveSpecification,
    TorsiondriveInputSpecification,
    TorsiondriveKeywords,
    TorsiondriveQueryBody,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Dict, Any, Union


def compare_torsiondrive_specs(
    input_spec: Union[TorsiondriveInputSpecification, Dict[str, Any]],
    full_spec: Union[TorsiondriveSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = TorsiondriveInputSpecification(**input_spec)
    if isinstance(full_spec, TorsiondriveSpecification):
        full_spec = full_spec.dict()

    full_spec.pop("id")
    full_spec.pop("optimization_specification_id")
    full_spec["optimization_specification"].pop("id")
    full_spec["optimization_specification"].pop("qc_specification_id")
    full_spec["optimization_specification"]["qc_specification"].pop("id")
    full_spec["optimization_specification"]["qc_specification"].pop("keywords_id")
    full_spec["optimization_specification"]["qc_specification"]["keywords"].pop("id")
    trimmed_spec = TorsiondriveInputSpecification(**full_spec)
    return input_spec == trimmed_spec


_test_specs = [
    TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(1, 2, 3, 4)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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
    TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(7, 2, 9, 4), (5, 11, 3, 10)],
            grid_spacing=[30, 45],
            dihedral_ranges=[[-90, 90], [0, 180]],
            energy_decrease_thresh=1.0,
            energy_upper_limit=0.05,
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
def test_torsiondrive_socket_add_get(storage_socket: SQLAlchemySocket, spec: TorsiondriveInputSpecification):
    hooh = load_molecule_data("peroxide2")
    td_mol_1 = load_molecule_data("td_C9H11NO2_1")
    td_mol_2 = load_molecule_data("td_C9H11NO2_2")

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.torsiondrive.add(
        [[hooh], [td_mol_1, td_mol_2]], spec, as_service=True, tag="tag1", priority=PriorityEnum.low
    )
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.torsiondrive.get(id, include=["*", "initial_molecules", "service"])

    assert len(recs) == 2
    for r in recs:
        assert r["record_type"] == "torsiondrive"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_torsiondrive_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    assert len(recs[0]["initial_molecules"]) == 1
    assert len(recs[1]["initial_molecules"]) == 2

    assert recs[0]["initial_molecules"][0]["identifiers"]["molecule_hash"] == hooh.get_hash()

    # Not necessarily in the input order
    hash1 = recs[1]["initial_molecules"][0]["identifiers"]["molecule_hash"]
    hash2 = recs[1]["initial_molecules"][1]["identifiers"]["molecule_hash"]
    assert {hash1, hash2} == {td_mol_1.get_hash(), td_mol_2.get_hash()}


def test_torsiondrive_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    mol1 = load_molecule_data("td_C9H11NO2_1")
    mol2 = load_molecule_data("td_C9H11NO2_2")

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([mol2])

    # Now add records
    meta, id = storage_socket.records.torsiondrive.add(
        [[mol1, mol2], [mol2, mol1]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1

    recs = storage_socket.records.torsiondrive.get(id, include=["initial_molecules"])
    assert len(recs) == 2
    assert recs[0]["id"] == recs[1]["id"]

    rec_mols = {x["id"] for x in recs[0]["initial_molecules"]}
    _, mol_ids_2 = storage_socket.molecules.add([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_torsiondrive_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[hooh]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[hooh]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_torsiondrive_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # multiple molecule ordering, and duplicate molecules
    spec = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[mol2, mol3, mol1, mol2], [mol3, mol2, mol1, mol1]],
        spec,
        tag="*",
        priority=PriorityEnum.normal,
        as_service=True,
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id2 == [id1[0], id1[0]]


def test_torsiondrive_socket_add_same_3(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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

    spec2 = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optPROG1",
            keywords={"k": "value"},
            qc_specification=OptimizationQCInputSpecification(
                program="prOG2",
                method="b3LYP",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all", stdout=True),
            ),
        ),
    )

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec1, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec2, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_torsiondrive_socket_add_different_1(storage_socket: SQLAlchemySocket):
    # Molecules are a subset of another
    spec = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[mol1], [mol3, mol2], [mol2, mol3, mol1]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta.n_inserted == 2
    assert meta.n_existing == 1
    assert meta.existing_idx == [2]
    assert meta.inserted_idx == [0, 1]
    assert id1[0] == id2[2]


def test_torsiondrive_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("td_H2O2_psi4_b3lyp")
    input_spec_2, molecules_2, result_data_2 = load_procedure_data("td_H2O2_psi4_pbe")
    input_spec_3, molecules_3, result_data_3 = load_procedure_data("td_C9H11NO2_psi4_b3lyp-d3bj")
    input_spec_4, molecules_4, result_data_4 = load_procedure_data("td_H2O2_psi4_bp86")

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    meta_2, id_2 = storage_socket.records.torsiondrive.add(
        [molecules_2], input_spec_2, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    meta_3, id_3 = storage_socket.records.torsiondrive.add(
        [molecules_3], input_spec_3, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    meta_4, id_4 = storage_socket.records.torsiondrive.add(
        [molecules_4], input_spec_4, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta_1.success and meta_2.success and meta_3.success and meta_4.success

    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_program=["psi4"]))
    assert meta.n_found == 4

    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_program=["nothing"]))
    assert meta.n_found == 0

    _, init_mol_id = storage_socket.molecules.add(molecules_1 + molecules_2 + molecules_3 + molecules_4)
    meta, td = storage_socket.records.torsiondrive.query(
        TorsiondriveQueryBody(initial_molecule_id=[init_mol_id[0], 9999])
    )
    assert meta.n_found == 3

    # query for optimization program
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(optimization_program=["geometric"]))
    assert meta.n_found == 4

    # query for optimization program
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(optimization_program=["geometric123"]))
    assert meta.n_found == 0

    # query for basis
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_basis=["sTO-3g"]))
    assert meta.n_found == 3

    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_basis=[None]))
    assert meta.n_found == 0

    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_basis=[""]))
    assert meta.n_found == 0

    # query for method
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_method=["b3lyP"]))
    assert meta.n_found == 1

    kw_id = td[0]["specification"]["optimization_specification"]["qc_specification"]["keywords_id"]
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(qc_keywords_id=[kw_id]))
    assert meta.n_found == 3

    # Query by default returns everything
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody())
    assert meta.n_found == 4

    # Query by default (with a limit)
    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(limit=1))
    assert meta.n_found == 4
    assert meta.n_returned == 1


@pytest.mark.parametrize(
    "test_data_name",
    [
        "td_C9H11NO2_psi4_b3lyp-d3bj",
        "td_H2O2_psi4_b3lyp-d3bj",
        "td_H2O2_psi4_b3lyp",
        "td_H2O2_psi4_blyp",
        "td_H2O2_psi4_bp86",
        "td_H2O2_psi4_hf",
        "td_H2O2_psi4_pbe0-d3bj",
        "td_H2O2_psi4_pbe0",
        "td_H2O2_psi4_pbe",
    ],
)
def test_torsiondrive_socket_run(storage_socket: SQLAlchemySocket, test_data_name: str):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data(test_data_name)

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low, as_service=True
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_optimizations = run_service_constropt(id_1[0], result_data_1, storage_socket, 200)
    time_1 = datetime.utcnow()

    rec = storage_socket.records.torsiondrive.get(
        id_1,
        include=[
            "*",
            "compute_history.*",
            "compute_history.outputs",
            "optimizations.*",
            "optimizations.optimization_record",
            "service",
        ],
    )

    assert rec[0]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["modified_on"] < time_1
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 1
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is None
    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"][0])
    assert "Job Finished" in out.as_string

    assert len(rec[0]["optimizations"]) == n_optimizations
