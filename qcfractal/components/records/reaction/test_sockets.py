from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_simple
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.reaction import ReactionSpecification, ReactionQueryFilters, ReactionKeywords
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Union, Dict, Any


def compare_reaction_specs(
    input_spec: Union[ReactionSpecification, Dict[str, Any]],
    output_spec: Union[ReactionSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = ReactionSpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = ReactionSpecification(**output_spec)

    return input_spec == output_spec


_test_specs = [
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


@pytest.mark.parametrize("spec", _test_specs)
def test_reaction_socket_add_get(storage_socket: SQLAlchemySocket, spec: ReactionSpecification):
    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.reaction.add(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)]], spec, tag="tag1", priority=PriorityEnum.low
    )
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.reaction.get(id, include=["*", "components.*", "components.molecule", "service"])

    assert len(recs) == 2

    for r in recs:
        assert r["record_type"] == "reaction"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_reaction_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    mol_hash_0 = set(x["molecule"]["identifiers"]["molecule_hash"] for x in recs[0]["components"])
    mol_hash_1 = set(x["molecule"]["identifiers"]["molecule_hash"] for x in recs[1]["components"])

    assert mol_hash_0 == {hooh.get_hash(), ne4.get_hash()}
    assert mol_hash_1 == {hooh.get_hash(), water.get_hash()}

    expected_coef = {hooh.get_hash(): 1.0, ne4.get_hash(): 2.0}
    db_coef = {x["molecule"]["identifiers"]["molecule_hash"]: x["coefficient"] for x in recs[0]["components"]}
    assert expected_coef == db_coef

    expected_coef = {hooh.get_hash(): 3.0, water.get_hash(): 4.0}
    db_coef = {x["molecule"]["identifiers"]["molecule_hash"]: x["coefficient"] for x in recs[1]["components"]}
    assert expected_coef == db_coef


def test_reaction_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    mol1 = load_molecule_data("go_H3NS")
    mol2 = load_molecule_data("peroxide2")
    mol3 = load_molecule_data("water_dimer_minima")

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([mol2])

    # Now add records
    meta, id = storage_socket.records.reaction.add(
        [[(1.0, mol1), (2.0, mol2)], [(2.0, mol_ids[0]), (3.0, mol3)]], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.success
    assert meta.n_inserted == 2

    recs = storage_socket.records.reaction.get(id, include=["components"])
    assert len(recs) == 2

    mol_ids_0 = set(x["molecule_id"] for x in recs[0]["components"])
    mol_ids_1 = set(x["molecule_id"] for x in recs[1]["components"])

    assert mol_ids[0] in mol_ids_0
    assert mol_ids[0] in mol_ids_1


def test_reaction_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = ReactionSpecification(
        program="reaction",
        singlepoint_specification=QCSpecification(
            program="proG2",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "v"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        keywords=ReactionKeywords(),
    )

    hooh = load_molecule_data("peroxide2")
    water = load_molecule_data("water_dimer_minima")

    meta, id1 = storage_socket.records.reaction.add(
        [[(2.0, water), (3.0, hooh)]], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.reaction.add(
        [[(3.0, hooh), (2.0, water)]], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_reaction_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("rxn_H2O_psi4_b3lyp_sp")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("rxn_H2_psi4_b3lyp_sp")

    meta_1, id_1 = storage_socket.records.reaction.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta_2, id_2 = storage_socket.records.reaction.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    assert meta_1.success and meta_2.success

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_program=["psi4"]))
    assert meta.n_found == 2

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_program=["nothing"]))
    assert meta.n_found == 0

    mol_H = load_molecule_data("rxn_H")
    mol_H2 = load_molecule_data("rxn_H2")
    _, init_mol_id = storage_socket.molecules.add([mol_H, mol_H2])

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(molecule_id=[init_mol_id[0], 9999]))
    assert meta.n_found == 1

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(molecule_id=[init_mol_id[1], 9999]))
    assert meta.n_found == 2

    # query for basis
    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_basis=["DEF2-tzvp"]))
    assert meta.n_found == 2

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_basis=["sTO-3g"]))
    assert meta.n_found == 0

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_basis=[None]))
    assert meta.n_found == 0

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_basis=[""]))
    assert meta.n_found == 0

    # query for qc_method
    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_method=["hf"]))
    assert meta.n_found == 0

    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(qc_method=["b3lyP"]))
    assert meta.n_found == 2

    # Query by default returns everything
    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters())
    assert meta.n_found == 2

    # Query by default (with a limit)
    meta, rxn = storage_socket.records.reaction.query(ReactionQueryFilters(limit=1))
    assert meta.n_found == 2
    assert len(rxn) == 1


@pytest.mark.parametrize(
    "test_data_name",
    [
        "rxn_H2_psi4_b3lyp_sp",
        "rxn_H2O_psi4_b3lyp_sp",
        "rxn_H2O_psi4_mp2_opt",
        "rxn_H2O_psi4_mp2_optsp",
    ],
)
def test_reaction_socket_run(storage_socket: SQLAlchemySocket, test_data_name: str):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data(test_data_name)

    meta_1, id_1 = storage_socket.records.reaction.add(
        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_singlepoints = run_service_simple(id_1[0], result_data_1, storage_socket, 100)
    time_1 = datetime.utcnow()

    assert finished is True

    rec = storage_socket.records.reaction.get(
        id_1, include=["*", "compute_history.*", "compute_history.outputs", "components", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["modified_on"] < time_1
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 1
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is None
    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"]["stdout"])
    assert "All reaction components are complete" in out.as_string

    assert rec[0]["total_energy"] < 0.0
