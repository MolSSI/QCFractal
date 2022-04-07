"""
Tests the reaction record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.records import PriorityEnum
from qcportal.records.reaction import ReactionQCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

from ..singlepoint.test_sockets import compare_singlepoint_specs
from .test_sockets import _test_specs


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_reaction_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    meta1, id1 = snowflake_client.add_reactions(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)]], "psi4", "hf", "sto-3g", tag=tag, priority=priority
    )

    rec = snowflake_client.get_records(id1, include_service=True)
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", _test_specs)
def test_reaction_client_add_get(snowflake_client: PortalClient, spec: ReactionQCSpecification):
    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    time_0 = datetime.utcnow()
    meta1, id1 = snowflake_client.add_reactions(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)]],
        spec.program,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        tag="tag1",
        priority=PriorityEnum.low,
    )
    time_1 = datetime.utcnow()
    assert meta1.success

    recs = snowflake_client.get_reactions(id1, include_service=True, include_stoichiometries=True)
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "reaction"
        assert r.raw_data.record_type == "reaction"
        assert compare_singlepoint_specs(spec, r.raw_data.specification)

        assert r.raw_data.service.tag == "tag1"
        assert r.raw_data.service.priority == PriorityEnum.low

        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.service.created_on < time_1

    mol_hash_0 = set(x.molecule.identifiers.molecule_hash for x in recs[0].raw_data.stoichiometries)
    mol_hash_1 = set(x.molecule.identifiers.molecule_hash for x in recs[1].raw_data.stoichiometries)

    assert mol_hash_0 == {hooh.get_hash(), ne4.get_hash()}
    assert mol_hash_1 == {hooh.get_hash(), water.get_hash()}

    expected_coef = {hooh.get_hash(): 1.0, ne4.get_hash(): 2.0}
    db_coef = {x.molecule.identifiers.molecule_hash: x.coefficient for x in recs[0].raw_data.stoichiometries}
    assert expected_coef == db_coef

    expected_coef = {hooh.get_hash(): 3.0, water.get_hash(): 4.0}
    db_coef = {x.molecule.identifiers.molecule_hash: x.coefficient for x in recs[1].raw_data.stoichiometries}
    assert expected_coef == db_coef


def test_reaction_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = _test_specs[0]

    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([hooh])

    # Now add records
    meta1, id1 = snowflake_client.add_reactions(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)], [(2.0, ne4), (1.0, hooh)]],
        spec.program,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        tag="tag1",
        priority=PriorityEnum.low,
    )

    assert meta1.success

    assert meta1.success
    assert meta1.n_inserted == 2
    assert meta1.n_existing == 1

    recs = snowflake_client.get_reactions(id1, include_stoichiometries=True)
    assert len(recs) == 3
    assert recs[0].raw_data.id == recs[2].raw_data.id
    assert recs[0].raw_data.id != recs[1].raw_data.id

    rec_mols_1 = set(x.molecule.id for x in recs[0].raw_data.stoichiometries)

    assert mol_ids[0] in rec_mols_1


def test_reaction_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("rxn_H2O_psi4_b3lyp")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("rxn_H2_psi4_b3lyp")

    meta_1, id_1 = storage_socket.records.reaction.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta_2, id_2 = storage_socket.records.reaction.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    assert meta_1.success and meta_2.success

    meta, rxn = snowflake_client.query_reactions(program=["psi4"])
    assert meta.n_found == 2

    meta, rxn = snowflake_client.query_reactions(program=["nothing"])
    assert meta.n_found == 0

    mol_H = load_molecule_data("rxn_H")
    mol_H2 = load_molecule_data("rxn_H2")
    _, init_mol_id = storage_socket.molecules.add([mol_H, mol_H2])

    meta, rxn = snowflake_client.query_reactions(molecule_id=[init_mol_id[0], 9999])
    assert meta.n_found == 1

    meta, rxn = snowflake_client.query_reactions(molecule_id=[init_mol_id[1], 9999])
    assert meta.n_found == 2

    # query for basis
    meta, rxn = snowflake_client.query_reactions(basis=["def2-TZvp"])
    assert meta.n_found == 2

    meta, rxn = snowflake_client.query_reactions(basis=["sTO-3g"])
    assert meta.n_found == 0

    meta, rxn = snowflake_client.query_reactions(basis=[None])
    assert meta.n_found == 0

    meta, rxn = snowflake_client.query_reactions(basis=[""])
    assert meta.n_found == 0

    # query for method
    meta, rxn = snowflake_client.query_reactions(method=["hf"])
    assert meta.n_found == 0

    meta, rxn = snowflake_client.query_reactions(method=["b3lyP"])
    assert meta.n_found == 2

    # Query by default returns everything
    meta, rxn = snowflake_client.query_reactions()
    assert meta.n_found == 2

    # Query by default (with a limit)
    meta, rxn = snowflake_client.query_reactions(limit=1)
    assert meta.n_found == 2
    assert meta.n_returned == 1
