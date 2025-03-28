from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.reaction.record_db_models import ReactionRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.auth import UserInfo, GroupInfo
from qcportal.reaction import ReactionSpecification, ReactionKeywords
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import SinglepointProtocols, QCSpecification
from qcportal.utils import now_at_utc
from .testing_helpers import compare_reaction_specs, test_specs, load_test_data, generate_task_key

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName
    from sqlalchemy.orm.session import Session


@pytest.mark.parametrize("spec", test_specs)
def test_reaction_socket_add_get(storage_socket: SQLAlchemySocket, session: Session, spec: ReactionSpecification):
    hooh = load_molecule_data("peroxide2")
    ne4 = load_molecule_data("neon_tetramer")
    water = load_molecule_data("water_dimer_minima")

    time_0 = now_at_utc()
    meta, ids = storage_socket.records.reaction.add(
        [[(1.0, hooh), (2.0, ne4)], [(3.0, hooh), (4.0, water)]], spec, "tag1", PriorityEnum.low, None, None, True
    )
    time_1 = now_at_utc()
    assert meta.success

    recs = [session.get(ReactionRecordORM, i) for i in ids]
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "reaction"
        assert r.status == RecordStatusEnum.waiting
        assert compare_reaction_specs(spec, r.specification.model_dict())

        # Service queue entry should exist with the proper tag and priority
        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    mol_hash_0 = set(x.molecule.identifiers["molecule_hash"] for x in recs[0].components)
    mol_hash_1 = set(x.molecule.identifiers["molecule_hash"] for x in recs[1].components)

    assert mol_hash_0 == {hooh.get_hash(), ne4.get_hash()}
    assert mol_hash_1 == {hooh.get_hash(), water.get_hash()}

    expected_coef = {hooh.get_hash(): 1.0, ne4.get_hash(): 2.0}
    db_coef = {x.molecule.identifiers["molecule_hash"]: x.coefficient for x in recs[0].components}
    assert expected_coef == db_coef

    expected_coef = {hooh.get_hash(): 3.0, water.get_hash(): 4.0}
    db_coef = {x.molecule.identifiers["molecule_hash"]: x.coefficient for x in recs[1].components}
    assert expected_coef == db_coef


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
        [[(2.0, water), (3.0, hooh)]], spec, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.reaction.add(
        [[(3.0, hooh), (2.0, water)]], spec, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


@pytest.mark.parametrize(
    "test_data_name",
    [
        "rxn_H2_psi4_b3lyp_sp",
        "rxn_H2O_psi4_b3lyp_sp",
        "rxn_H2O_psi4_mp2_opt",
        "rxn_H2O_psi4_mp2_optsp",
    ],
)
def test_reaction_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, stoich_1, result_data_1 = load_test_data(test_data_name)

    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.users.add(UserInfo(username="submit_user", role="submit", groups=["group1"], enabled=True))

    meta_1, id_1 = storage_socket.records.reaction.add(
        [stoich_1], input_spec_1, "test_tag", PriorityEnum.low, "submit_user", "group1", True
    )
    id_1 = id_1[0]
    assert meta_1.success

    time_0 = now_at_utc()
    finished, n_singlepoints = run_service(
        storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 100
    )
    time_1 = now_at_utc()

    assert finished is True

    rec = session.get(ReactionRecordORM, id_1)

    assert rec.status == RecordStatusEnum.complete
    assert time_0 < rec.modified_on < time_1
    assert len(rec.compute_history) == 1
    assert len(rec.compute_history[-1].outputs) == 1
    assert rec.compute_history[-1].status == RecordStatusEnum.complete
    assert time_0 < rec.compute_history[-1].modified_on < time_1
    assert rec.service is None

    desc_info = storage_socket.records.get_short_descriptions([id_1])[0]
    short_desc = desc_info["description"]
    assert desc_info["record_type"] == rec.record_type
    assert desc_info["created_on"] == rec.created_on
    assert rec.specification.program in short_desc
    if rec.specification.singlepoint_specification is not None:
        assert rec.specification.singlepoint_specification.program in short_desc
        assert rec.specification.singlepoint_specification.method in short_desc
    if rec.specification.optimization_specification is not None:
        assert rec.specification.optimization_specification.program in short_desc
        assert rec.specification.optimization_specification.qc_specification.method in short_desc

    out = rec.compute_history[-1].outputs["stdout"].get_output()
    assert "All reaction components are complete" in out

    assert rec.total_energy < 0.0


def test_reaction_socket_run_duplicate(
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):
    input_spec_1, molecules_1, result_data_1 = load_test_data("rxn_H2O_psi4_mp2_optsp")

    meta_1, id_1 = storage_socket.records.reaction.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, None, True
    )
    id_1 = id_1[0]
    assert meta_1.success

    run_service(storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 1000)

    rec_1 = session.get(ReactionRecordORM, id_1)
    assert rec_1.status == RecordStatusEnum.complete
    opt_ids_1 = [x.optimization_id for x in rec_1.components]
    sp_ids_1 = [x.singlepoint_id for x in rec_1.components]

    # Submit again, without duplicate checking
    meta_2, id_2 = storage_socket.records.reaction.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, None, False
    )
    id_2 = id_2[0]
    assert meta_2.success
    assert id_2 != id_1

    run_service(storage_socket, activated_manager_name, id_2, generate_task_key, result_data_1, 1000)

    rec_2 = session.get(ReactionRecordORM, id_2)
    assert rec_2.status == RecordStatusEnum.complete
    opt_ids_2 = [x.optimization_id for x in rec_2.components]
    sp_ids_2 = [x.singlepoint_id for x in rec_2.components]

    assert set(opt_ids_1 + sp_ids_1).isdisjoint(opt_ids_2 + sp_ids_2)
