from __future__ import annotations

import itertools
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.reaction import ReactionSpecification, ReactionKeywords
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification
from .testing_helpers import compare_reaction_specs, test_specs, run_test_data, submit_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient
    from qcportal.managers import ManagerName
    from sqlalchemy.orm.session import Session


def test_reaction_client_tag_priority(snowflake_client: PortalClient):
    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        meta1, id1 = snowflake_client.add_reactions(
            [[(1.0, hooh), (2.0, ne4)]],
            "reaction",
            QCSpecification(
                program="Prog2",
                driver="energy",
                method="Hf",
                basis="def2-TZVP",
                keywords={"tag_priority": [tag, priority]},
            ),
            None,
            ReactionKeywords(),
            tag=tag,
            priority=priority,
        )

        assert meta1.n_inserted == 1

        rec = snowflake_client.get_records(id1, include=["service"])
        assert rec[0].service.tag == tag
        assert rec[0].service.priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_reaction_client_add_get(
    submitter_client: PortalClient, spec: ReactionSpecification, owner_group: Optional[str]
):
    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    time_0 = datetime.utcnow()
    meta1, id1 = submitter_client.add_reactions(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)]],
        spec.program,
        spec.singlepoint_specification,
        spec.optimization_specification,
        spec.keywords,
        tag="tag1",
        priority=PriorityEnum.low,
        owner_group=owner_group,
    )
    time_1 = datetime.utcnow()
    assert meta1.success

    recs = submitter_client.get_reactions(id1, include=["service", "components"])
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "reaction"
        assert r.record_type == "reaction"
        assert compare_reaction_specs(spec, r.specification)

        assert r.service.tag == "tag1"
        assert r.service.priority == PriorityEnum.low

        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1
        assert time_0 < r.service.created_on < time_1

    mol_hash_0 = set(x.molecule.identifiers.molecule_hash for x in recs[0].components)
    mol_hash_1 = set(x.molecule.identifiers.molecule_hash for x in recs[1].components)

    assert mol_hash_0 == {hooh.get_hash(), ne4.get_hash()}
    assert mol_hash_1 == {hooh.get_hash(), water.get_hash()}

    expected_coef = {hooh.get_hash(): 1.0, ne4.get_hash(): 2.0}
    db_coef = {x.molecule.identifiers.molecule_hash: x.coefficient for x in recs[0].components}
    assert expected_coef == db_coef

    expected_coef = {hooh.get_hash(): 3.0, water.get_hash(): 4.0}
    db_coef = {x.molecule.identifiers.molecule_hash: x.coefficient for x in recs[1].components}
    assert expected_coef == db_coef


def test_reaction_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = test_specs[0]

    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([hooh])

    # Now add records
    meta1, id1 = snowflake_client.add_reactions(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)], [(2.0, ne4), (1.0, hooh)]],
        spec.program,
        spec.singlepoint_specification,
        spec.optimization_specification,
        spec.keywords,
        tag="tag1",
        priority=PriorityEnum.low,
    )

    assert meta1.success

    assert meta1.success
    assert meta1.n_inserted == 2
    assert meta1.n_existing == 1

    recs = snowflake_client.get_reactions(id1, include=["components"])
    assert len(recs) == 3
    assert recs[0].id == recs[2].id
    assert recs[0].id != recs[1].id

    rec_mols_1 = set(x.molecule.id for x in recs[0].components)

    assert mol_ids[0] in rec_mols_1


def test_reaction_client_delete(
    snowflake_client: PortalClient,
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):

    rxn_id = run_test_data(storage_socket, activated_manager_name, "rxn_H2O_psi4_mp2_optsp")

    rec = session.get(BaseRecordORM, rxn_id)
    child_ids = [x.optimization_id for x in rec.components if x.optimization_id is not None]
    child_ids += [x.singlepoint_id for x in rec.components if x.singlepoint_id is not None]

    n_children = len(child_ids) + sum(len(x.optimization_record.trajectory) for x in rec.components)

    meta = snowflake_client.delete_records(rxn_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)

    snowflake_client.undelete_records(rxn_id)

    meta = snowflake_client.delete_records(rxn_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == n_children

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)

    meta = snowflake_client.delete_records(rxn_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == n_children

    recs = snowflake_client.get_reactions(rxn_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    assert query_res._current_meta.n_found == 0


def test_reaction_client_harddelete_nochildren(
    snowflake_client: PortalClient,
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):

    rxn_id = run_test_data(storage_socket, activated_manager_name, "rxn_H2O_psi4_mp2_optsp")

    rec = session.get(BaseRecordORM, rxn_id)
    child_ids = [x.optimization_id for x in rec.components if x.optimization_id is not None]
    child_ids += [x.singlepoint_id for x in rec.components if x.singlepoint_id is not None]

    meta = snowflake_client.delete_records(rxn_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_reactions(rxn_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


def test_reaction_client_delete_opt_inuse(
    snowflake_client: PortalClient,
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):

    rxn_id = run_test_data(storage_socket, activated_manager_name, "rxn_H2O_psi4_mp2_optsp")

    rec = session.get(BaseRecordORM, rxn_id)
    child_ids = [x.optimization_id for x in rec.components if x.optimization_id is not None]
    child_ids += [x.singlepoint_id for x in rec.components if x.singlepoint_id is not None]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_reaction_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "rxn_H2O_psi4_b3lyp_sp")
    id_2, _ = submit_test_data(storage_socket, "rxn_H2_psi4_b3lyp_sp")

    query_res = snowflake_client.query_reactions(qc_program=["psi4"])
    assert query_res._current_meta.n_found == 2

    query_res = snowflake_client.query_reactions(qc_program=["nothing"])
    assert query_res._current_meta.n_found == 0

    mol_H = load_molecule_data("rxn_H")
    mol_H2 = load_molecule_data("rxn_H2")
    _, init_mol_id = storage_socket.molecules.add([mol_H, mol_H2])

    query_res = snowflake_client.query_reactions(molecule_id=[init_mol_id[0], 9999])
    assert query_res._current_meta.n_found == 1

    query_res = snowflake_client.query_reactions(molecule_id=[init_mol_id[1], 9999])
    assert query_res._current_meta.n_found == 2

    # query for basis
    query_res = snowflake_client.query_reactions(qc_basis=["def2-TZvp"])
    assert query_res._current_meta.n_found == 2

    query_res = snowflake_client.query_reactions(qc_basis=["sTO-3g"])
    assert query_res._current_meta.n_found == 0

    query_res = snowflake_client.query_reactions(qc_basis=[None])
    assert query_res._current_meta.n_found == 0

    query_res = snowflake_client.query_reactions(qc_basis=[""])
    assert query_res._current_meta.n_found == 0

    # query for qc_method
    query_res = snowflake_client.query_reactions(qc_method=["hf"])
    assert query_res._current_meta.n_found == 0

    query_res = snowflake_client.query_reactions(qc_method=["b3lyP"])
    assert query_res._current_meta.n_found == 2

    # Query by default returns everything
    query_res = snowflake_client.query_reactions()
    assert query_res._current_meta.n_found == 2

    # Query by default (with a limit)
    query_res = snowflake_client.query_reactions(limit=1)
    assert query_res._current_meta.n_found == 2
