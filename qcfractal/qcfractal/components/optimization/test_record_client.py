from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcportal.optimization import (
    OptimizationSpecification,
)
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal import PortalClient

from qcfractal.components.optimization.testing_helpers import (
    compare_optimization_specs,
    test_specs,
    submit_test_data,
    run_test_data,
)


def test_optimization_client_tag_priority(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        meta1, id1 = snowflake_client.add_optimizations(
            [water],
            "prog",
            QCSpecification(
                program="prog",
                method="hf",
                basis="sto-3g",
                driver="deferred",
                keywords={"tag_priority": [tag, priority]},
            ),
            compute_priority=priority,
            compute_tag=tag,
        )

        assert meta1.n_inserted == 1
        rec = snowflake_client.get_records(id1, include=["task"])
        assert rec[0].task.compute_tag == tag
        assert rec[0].task.compute_priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_optimization_client_add_get(
    submitter_client: PortalClient, spec: OptimizationSpecification, owner_group: Optional[str]
):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = now_at_utc()
    meta, id = submitter_client.add_optimizations(
        initial_molecules=all_mols,
        program=spec.program,
        keywords=spec.keywords,
        protocols=spec.protocols,
        qc_specification=spec.qc_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=owner_group,
    )

    time_1 = now_at_utc()
    assert meta.success

    recs = submitter_client.get_optimizations(id, include=["task", "initial_molecule"])

    assert len(recs) == 3

    for r in recs:
        assert r.record_type == "optimization"
        assert r.record_type == "optimization"
        assert compare_optimization_specs(spec, r.specification)

        assert r.status == RecordStatusEnum.waiting
        assert r.children_status == {}

        assert r.task.function is None
        assert r.task.compute_tag == "tag1"
        assert r.task.compute_priority == PriorityEnum.low

        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    mol1 = submitter_client.get_molecules([recs[0].initial_molecule_id])[0]
    mol2 = submitter_client.get_molecules([recs[1].initial_molecule_id])[0]
    mol3 = submitter_client.get_molecules([recs[2].initial_molecule_id])[0]
    assert mol1.identifiers.molecule_hash == water.get_hash()
    assert recs[0].initial_molecule.identifiers.molecule_hash == water.get_hash()

    assert mol2.identifiers.molecule_hash == hooh.get_hash()
    assert recs[1].initial_molecule.identifiers.molecule_hash == hooh.get_hash()

    assert mol3.identifiers.molecule_hash == ne4.get_hash()
    assert recs[2].initial_molecule.identifiers.molecule_hash == ne4.get_hash()


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("find_existing", [True, False])
def test_optimization_client_add_duplicate(
    submitter_client: PortalClient, spec: OptimizationSpecification, find_existing: bool
):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    meta, id = submitter_client.add_optimizations(
        initial_molecules=all_mols,
        program=spec.program,
        keywords=spec.keywords,
        protocols=spec.protocols,
        qc_specification=spec.qc_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=True,
    )

    assert meta.success
    assert meta.n_inserted == len(all_mols)

    meta, id2 = submitter_client.add_optimizations(
        initial_molecules=all_mols,
        program=spec.program,
        keywords=spec.keywords,
        protocols=spec.protocols,
        qc_specification=spec.qc_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=find_existing,
    )

    if find_existing:
        assert meta.n_existing == len(all_mols)
        assert meta.n_inserted == 0
        assert id == id2
    else:
        assert meta.n_existing == 0
        assert meta.n_inserted == len(all_mols)
        assert set(id).isdisjoint(id2)


def test_optimization_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = test_specs[0]

    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([ne4])

    # Now add records
    meta, id = snowflake_client.add_optimizations(
        initial_molecules=all_mols,
        program=spec.program,
        keywords=spec.keywords,
        protocols=spec.protocols,
        qc_specification=spec.qc_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
    )
    recs = snowflake_client.get_optimizations(id)

    assert len(recs) == 3
    assert recs[2].initial_molecule_id == mol_ids[0]


@pytest.mark.parametrize("opt_file", ["opt_psi4_benzene", "opt_psi4_fluoroethane_notraj"])
def test_optimization_client_delete(snowflake: QCATestingSnowflake, opt_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    opt_id = run_test_data(storage_socket, activated_manager_name, opt_file)

    rec = snowflake_client.get_optimizations(opt_id)
    child_ids = [x.id for x in rec.trajectory]

    meta = snowflake_client.delete_records(opt_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)
    opt_rec = snowflake_client.get_records(opt_id)
    if child_ids:
        assert opt_rec.children_status == {RecordStatusEnum.complete: len(child_ids)}

    # Undo what we just did
    snowflake_client.undelete_records(opt_id)

    meta = snowflake_client.delete_records(opt_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)
    opt_rec = snowflake_client.get_records(opt_id)
    if child_ids:
        assert opt_rec.children_status == {RecordStatusEnum.deleted: len(child_ids)}

    meta = snowflake_client.delete_records(opt_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = snowflake_client.get_optimizations(opt_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    query_res_l = list(query_res)
    assert len(query_res_l) == 0


def test_optimization_client_harddelete_nochildren(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    opt_id = run_test_data(storage_socket, activated_manager_name, "opt_psi4_benzene")

    rec = snowflake_client.get_optimizations(opt_id)
    child_ids = [x.id for x in rec.trajectory]

    meta = snowflake_client.delete_records(opt_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_optimizations(opt_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


@pytest.mark.parametrize("opt_file", ["opt_psi4_benzene", "opt_psi4_methane_sometraj"])
def test_optimization_client_delete_traj_inuse(snowflake: QCATestingSnowflake, opt_file: str):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    opt_id = run_test_data(storage_socket, activated_manager_name, opt_file)

    rec = snowflake_client.get_optimizations(opt_id)
    child_ids = [x.id for x in rec.trajectory]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


@pytest.mark.parametrize("opt_file", ["opt_psi4_benzene", "opt_psi4_methane_sometraj"])
@pytest.mark.parametrize("fetch_traj", [True, False])
def test_optimization_client_traj(snowflake: QCATestingSnowflake, opt_file: str, fetch_traj: bool):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    opt_id = run_test_data(storage_socket, activated_manager_name, opt_file)

    rec = snowflake_client.get_optimizations(opt_id)
    rec_traj = snowflake_client.get_optimizations(opt_id, include=["trajectory"])

    assert rec_traj.trajectory is not None

    if fetch_traj:
        rec._fetch_trajectory()
        assert rec.trajectory_ids_ is not None
        assert rec._trajectory_records is not None
    else:
        assert rec.trajectory_ids_ is None
        assert rec._trajectory_records is None

    assert rec.trajectory_element(0).id == rec_traj.trajectory[0].id
    assert rec.trajectory_element(-1).id == rec_traj.trajectory[-1].id


def test_optimization_client_query(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    id_1, _ = submit_test_data(storage_socket, "opt_psi4_fluoroethane_notraj")
    id_2, _ = submit_test_data(storage_socket, "opt_psi4_benzene")
    id_3, _ = submit_test_data(storage_socket, "opt_psi4_methane_sometraj")

    recs = snowflake_client.get_optimizations([id_1, id_2, id_3])

    # query for molecule
    query_res = snowflake_client.query_optimizations(initial_molecule_id=[recs[1].initial_molecule_id])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # query for program
    query_res = snowflake_client.query_optimizations(program=["psi4"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for program
    query_res = snowflake_client.query_optimizations(program=["geometric"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    query_res = snowflake_client.query_optimizations(qc_program=["psi4"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # query for basis
    query_res = snowflake_client.query_optimizations(qc_basis=["sTO-3g"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_optimizations(qc_basis=[None])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_optimizations(qc_basis=[""])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for method
    query_res = snowflake_client.query_optimizations(qc_method=["b3lyP"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # Some empty queries
    query_res = snowflake_client.query_optimizations(program=["madeupprog"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # Query by default returns everything
    query_res = snowflake_client.query_optimizations()
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # Query by default (with a limit)
    query_res = snowflake_client.query_optimizations(limit=1)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
