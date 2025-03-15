from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.gridoptimization.record_db_models import GridoptimizationRecordORM
from qcportal.gridoptimization import GridoptimizationKeywords, GridoptimizationSpecification
from qcportal.optimization import OptimizationSpecification
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification
from qcportal.utils import now_at_utc
from .testing_helpers import compare_gridoptimization_specs, test_specs, submit_test_data, run_test_data

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal import PortalClient


def test_gridoptimization_client_tag_priority(snowflake_client: PortalClient):
    peroxide2 = load_molecule_data("peroxide2")

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        # add tag/priority to keywords to force adding new record
        meta1, id1 = snowflake_client.add_gridoptimizations(
            [peroxide2],
            "gridoptimization",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(
                    program="psi4",
                    driver="deferred",
                    method="hf",
                    basis="sto-3g",
                    keywords={"tag_priority": [tag, priority]},
                ),
            ),
            keywords=GridoptimizationKeywords(
                preoptimization=False,
                scans=[
                    {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                    {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
                ],
            ),
            compute_priority=priority,
            compute_tag=tag,
        )

        assert meta1.n_inserted == 1
        rec = snowflake_client.get_records(id1, include=["service"])
        assert rec[0].service.compute_tag == tag
        assert rec[0].service.compute_priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_gridoptimization_client_add_get(
    submitter_client: PortalClient, spec: GridoptimizationSpecification, owner_group: Optional[str]
):
    hooh = load_molecule_data("peroxide2")
    h3ns = load_molecule_data("go_H3NS")

    time_0 = now_at_utc()
    meta, id = submitter_client.add_gridoptimizations(
        [hooh, h3ns],
        spec.program,
        spec.optimization_specification,
        spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=owner_group,
    )
    time_1 = now_at_utc()
    assert meta.success

    recs = submitter_client.get_gridoptimizations(id, include=["service", "initial_molecule"])
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "gridoptimization"
        assert r.record_type == "gridoptimization"
        assert compare_gridoptimization_specs(spec, r.specification)

        assert r.status == RecordStatusEnum.waiting
        assert r.children_status == {}

        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert recs[0].initial_molecule.identifiers.molecule_hash == hooh.get_hash()
    assert recs[1].initial_molecule.identifiers.molecule_hash == h3ns.get_hash()


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("find_existing", [True, False])
def test_gridoptimization_client_add_duplicate(
    submitter_client: PortalClient,
    spec: GridoptimizationSpecification,
    find_existing,
):
    hooh = load_molecule_data("peroxide2")
    h3ns = load_molecule_data("go_H3NS")
    all_mols = [hooh, h3ns]

    meta, id = submitter_client.add_gridoptimizations(
        all_mols,
        spec.program,
        spec.optimization_specification,
        spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=True,
    )
    assert meta.success

    meta, id2 = submitter_client.add_gridoptimizations(
        all_mols,
        spec.program,
        spec.optimization_specification,
        spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=find_existing,
    )
    assert meta.success

    if find_existing:
        assert meta.n_existing == len(all_mols)
        assert meta.n_inserted == 0
        assert id == id2
    else:
        assert meta.n_existing == 0
        assert meta.n_inserted == len(all_mols)
        assert set(id).isdisjoint(id2)


def test_gridoptimization_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = test_specs[0]

    mol1 = load_molecule_data("go_H3NS")
    mol2 = load_molecule_data("peroxide2")

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([mol2])

    # Now add records
    meta, id = snowflake_client.add_gridoptimizations(
        [mol1, mol2, mol2, mol1],
        "gridoptimization",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
    )

    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 2

    recs = snowflake_client.get_gridoptimizations(id, include=["initial_molecule"])
    assert len(recs) == 4
    assert recs[0].id == recs[3].id
    assert recs[1].id == recs[2].id

    rec_mols = {x.initial_molecule.id for x in recs}
    _, mol_ids_2 = snowflake_client.add_molecules([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_gridoptimization_client_delete(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    go_id = run_test_data(storage_socket, activated_manager_name, "go_H2O2_psi4_pbe")

    with storage_socket.session_scope() as session:
        rec = session.get(GridoptimizationRecordORM, go_id)
        child_ids = [x.optimization_id for x in rec.optimizations]

    meta = snowflake_client.delete_records(go_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)
    go_rec = snowflake_client.get_records(go_id)
    assert go_rec.children_status == {RecordStatusEnum.complete: len(child_ids)}

    snowflake_client.undelete_records(go_id)

    meta = snowflake_client.delete_records(go_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)
    go_rec = snowflake_client.get_records(go_id)
    assert go_rec.children_status == {RecordStatusEnum.deleted: len(child_ids)}

    meta = snowflake_client.delete_records(go_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = snowflake_client.get_gridoptimizations(go_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    query_res_l = list(query_res)
    assert len(query_res_l) == 0


def test_gridoptimization_client_harddelete_nochildren(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    go_id = run_test_data(storage_socket, activated_manager_name, "go_H2O2_psi4_pbe")

    with storage_socket.session_scope() as session:
        rec = session.get(GridoptimizationRecordORM, go_id)
        child_ids = [x.optimization_id for x in rec.optimizations]

    meta = snowflake_client.delete_records(go_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_gridoptimizations(go_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


def test_gridoptimization_client_delete_opt_inuse(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    go_id = run_test_data(storage_socket, activated_manager_name, "go_H2O2_psi4_pbe")

    with storage_socket.session_scope() as session:
        rec = session.get(GridoptimizationRecordORM, go_id)
        child_ids = [x.optimization_id for x in rec.optimizations]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_gridoptimization_client_query(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id_1, _ = submit_test_data(storage_socket, "go_H2O2_psi4_b3lyp")
    id_2, _ = submit_test_data(storage_socket, "go_H2O2_psi4_pbe")
    id_3, _ = submit_test_data(storage_socket, "go_C4H4N2OS_mopac_pm6")
    id_4, _ = submit_test_data(storage_socket, "go_H3NS_psi4_pbe")

    all_gos = snowflake_client.get_gridoptimizations([id_1, id_2, id_3, id_4])
    mol_ids = [x.initial_molecule_id for x in all_gos]

    query_res = snowflake_client.query_gridoptimizations(qc_program=["psi4"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    query_res = snowflake_client.query_gridoptimizations(qc_program=["nothing"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_gridoptimizations(initial_molecule_id=[mol_ids[0], 9999])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # query for optimization program
    query_res = snowflake_client.query_gridoptimizations(optimization_program=["geometric"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # query for optimization program
    query_res = snowflake_client.query_gridoptimizations(optimization_program=["geometric123"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for basis
    query_res = snowflake_client.query_gridoptimizations(qc_basis=["sTO-3g"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    query_res = snowflake_client.query_gridoptimizations(qc_basis=[None])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_gridoptimizations(qc_basis=[""])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # query for method
    query_res = snowflake_client.query_gridoptimizations(qc_method=["b3lyP"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # Query by default returns everything
    query_res = snowflake_client.query_gridoptimizations()
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # Query by default (with a limit)
    query_res = snowflake_client.query_gridoptimizations(limit=1)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
