from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.manybody.record_db_models import ManybodyRecordORM
from qcportal.manybody import ManybodySpecification
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification
from qcportal.utils import now_at_utc
from .testing_helpers import compare_manybody_specs, test_specs, submit_test_data, run_test_data

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal import PortalClient


def test_manybody_client_tag_priority(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        sp_spec = QCSpecification(
            program="prog",
            driver="energy",
            method="hf",
            basis="sto-3g",
            keywords={"tag_priority": [tag, priority]},
        )

        meta1, id1 = snowflake_client.add_manybodys(
            [water],
            "qcmanybody",
            bsse_correction=["nocp"],
            levels={1: sp_spec},
            keywords={"return_total_data": True},
            compute_tag=tag,
            compute_priority=priority,
        )

        assert meta1.n_inserted == 1
        rec = snowflake_client.get_records(id1, include=["service"])
        assert rec[0].service.compute_tag == tag
        assert rec[0].service.compute_priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_manybody_client_add_get(
    submitter_client: PortalClient, spec: ManybodySpecification, owner_group: Optional[str]
):
    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")

    time_0 = now_at_utc()
    meta1, id1 = submitter_client.add_manybodys(
        [water2, water4],
        spec.program,
        spec.levels,
        spec.bsse_correction,
        keywords=spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=owner_group,
    )
    time_1 = now_at_utc()
    assert meta1.success

    recs = submitter_client.get_manybodys(id1, include=["service", "clusters", "initial_molecule"])
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "manybody"
        assert r.record_type == "manybody"
        assert compare_manybody_specs(spec, r.specification)

        assert r.status == RecordStatusEnum.waiting
        assert r.children_status == {}

        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert recs[0].initial_molecule.get_hash() == water2.get_hash()
    assert recs[1].initial_molecule.get_hash() == water4.get_hash()


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("find_existing", [True, False])
def test_manybody_client_add_duplicate(
    submitter_client: PortalClient, spec: ManybodySpecification, find_existing: bool
):
    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")
    all_mols = [water2, water4]

    meta, id = submitter_client.add_manybodys(
        all_mols,
        spec.program,
        spec.levels,
        spec.bsse_correction,
        keywords=spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=True,
    )
    assert meta.success
    assert meta.n_inserted == len(all_mols)

    meta, id2 = submitter_client.add_manybodys(
        all_mols,
        spec.program,
        spec.levels,
        spec.bsse_correction,
        keywords=spec.keywords,
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


def test_manybody_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = test_specs[0]

    mol1 = load_molecule_data("water_dimer_minima")
    mol2 = load_molecule_data("water_stacked")

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([mol1])

    # Now add records
    meta1, id1 = snowflake_client.add_manybodys(
        [mol1, mol2, mol1],
        spec.program,
        spec.levels,
        spec.bsse_correction,
        keywords=spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
    )

    assert meta1.success

    assert meta1.success
    assert meta1.n_inserted == 2
    assert meta1.n_existing == 1

    recs = snowflake_client.get_manybodys(id1, include=["initial_molecule"])
    assert len(recs) == 3
    assert recs[0].id == recs[2].id
    assert recs[0].id != recs[1].id

    assert recs[0].initial_molecule.get_hash() == mol1.get_hash()
    assert recs[1].initial_molecule.get_hash() == mol2.get_hash()


def test_manybody_client_delete(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    mb_id = run_test_data(storage_socket, activated_manager_name, "mb_cp_he4_psi4_mp2")

    with storage_socket.session_scope() as session:
        rec = session.get(ManybodyRecordORM, mb_id)
        child_ids = [x.singlepoint_id for x in rec.clusters]

    meta = snowflake_client.delete_records(mb_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)
    mb_rec = snowflake_client.get_records(mb_id)
    assert mb_rec.children_status == {RecordStatusEnum.complete: len(child_ids)}

    snowflake_client.undelete_records(mb_id)

    meta = snowflake_client.delete_records(mb_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(set(child_ids))

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)
    mb_rec = snowflake_client.get_records(mb_id)
    assert mb_rec.children_status == {RecordStatusEnum.deleted: len(child_ids)}

    meta = snowflake_client.delete_records(mb_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(set(child_ids))

    recs = snowflake_client.get_manybodys(mb_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    query_res_l = list(query_res)
    assert len(query_res_l) == 0


def test_manybody_client_harddelete_nochildren(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    mb_id = run_test_data(storage_socket, activated_manager_name, "mb_cp_he4_psi4_mp2")

    with storage_socket.session_scope() as session:
        rec = session.get(ManybodyRecordORM, mb_id)
        child_ids = [x.singlepoint_id for x in rec.clusters]

    meta = snowflake_client.delete_records(mb_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_manybodys(mb_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


def test_manybody_client_delete_opt_inuse(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    mb_id = run_test_data(storage_socket, activated_manager_name, "mb_cp_he4_psi4_mp2")

    with storage_socket.session_scope() as session:
        rec = session.get(ManybodyRecordORM, mb_id)
        child_ids = [x.singlepoint_id for x in rec.clusters]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_manybody_client_query(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id_1, _ = submit_test_data(storage_socket, "mb_cp_he4_psi4_mp2")
    id_2, _ = submit_test_data(storage_socket, "mb_all_he4_psi4_multiss")

    all_mbs = snowflake_client.get_manybodys([id_1, id_2])
    mol_ids = [x.initial_molecule_id for x in all_mbs]

    query_res = snowflake_client.query_manybodys(program=["qcmanybody"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_manybodys(program=["nothing"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_manybodys(initial_molecule_id=[9999])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_manybodys(initial_molecule_id=[mol_ids[0], 9999])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # query for qc program
    query_res = snowflake_client.query_manybodys(qc_program=["Psi4"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_manybodys(qc_program=["abc"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for basis
    query_res = snowflake_client.query_manybodys(qc_basis=["DEF2-tzvp"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_manybodys(qc_basis=["6-31g*"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_manybodys(qc_basis=["6-31g"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_manybodys(qc_basis=[None])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_manybodys(qc_basis=[""])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for method
    query_res = snowflake_client.query_manybodys(qc_method=["hf"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_manybodys(qc_method=["mp2"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # Query by default returns everything
    query_res = snowflake_client.query_manybodys()
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # Query by default (with a limit)
    query_res = snowflake_client.query_manybodys(limit=1)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
