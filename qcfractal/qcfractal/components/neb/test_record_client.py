from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.neb.record_db_models import NEBRecordORM
from qcfractal.components.neb.testing_helpers import (
    compare_neb_specs,
    test_specs,
    submit_test_data,
    run_test_data,
)
from qcportal.neb import (
    NEBSpecification,
    NEBKeywords,
)
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal import PortalClient


def test_neb_client_tag_priority(snowflake_client: PortalClient):
    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        meta1, id1 = snowflake_client.add_nebs(
            [chain],
            "geometric",
            QCSpecification(
                program="psi4",
                method="hf",
                basis="sto-3g",
                driver="gradient",
                keywords={"tag_priority": [tag, priority]},
            ),
            None,
            NEBKeywords(),
            compute_priority=priority,
            compute_tag=tag,
        )
        assert meta1.n_inserted == 1
        rec = snowflake_client.get_records(id1, include=["service"])
        assert rec[0].service.compute_tag == tag
        assert rec[0].service.compute_priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_neb_client_add_get(submitter_client: PortalClient, spec: NEBSpecification, owner_group: Optional[str]):
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)]

    time_0 = now_at_utc()
    meta, id = submitter_client.add_nebs(
        initial_chains=[chain1, chain2],
        program=spec.program,
        singlepoint_specification=spec.singlepoint_specification,
        optimization_specification=None,
        keywords=spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=owner_group,
    )

    time_1 = now_at_utc()
    assert meta.success

    recs = submitter_client.get_nebs(id, include=["service", "initial_chain"])

    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "neb"
        assert r.record_type == "neb"
        assert compare_neb_specs(spec, r.specification)

        assert r.status == RecordStatusEnum.waiting
        assert r.children_status == {}

        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert len(recs[0].initial_chain) == 11  # default image number
    assert len(recs[1].initial_chain) == 11

    assert recs[0].initial_chain[0].get_hash() == chain1[0].get_hash()

    hash1 = recs[0].initial_chain[0].get_hash()
    hash2 = recs[1].initial_chain[-1].get_hash()
    assert {hash1, hash2} == {chain1[0].get_hash(), chain2[-1].get_hash()}


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("find_existing", [True, False])
def test_neb_client_add_duplicate(submitter_client: PortalClient, spec: NEBSpecification, find_existing: bool):
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)]
    all_chains = [chain1, chain2]

    meta, id = submitter_client.add_nebs(
        initial_chains=all_chains,
        program=spec.program,
        singlepoint_specification=spec.singlepoint_specification,
        optimization_specification=None,
        keywords=spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=True,
    )

    assert meta.success
    print(meta)
    assert meta.n_inserted == len(all_chains)

    meta, id2 = submitter_client.add_nebs(
        initial_chains=all_chains,
        program=spec.program,
        singlepoint_specification=spec.singlepoint_specification,
        optimization_specification=None,
        keywords=spec.keywords,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=find_existing,
    )

    if find_existing:
        assert meta.n_existing == len(all_chains)
        assert meta.n_inserted == 0
        assert id == id2
    else:
        assert meta.n_existing == 0
        assert meta.n_inserted == len(all_chains)
        assert set(id).isdisjoint(id2)


def test_neb_client_add_existing_chain(snowflake_client: PortalClient):
    spec = test_specs[0]
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)]

    # Add a chain separately
    _, mol_ids = snowflake_client.add_molecules(chain1)

    # Now add records
    meta, id = snowflake_client.add_nebs(
        initial_chains=[chain1, chain2],
        program=spec.program,
        keywords=spec.keywords,
        singlepoint_specification=spec.singlepoint_specification,
        optimization_specification=None,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
    )
    recs = snowflake_client.get_nebs(id)

    assert len(recs) == 2
    assert recs[0].initial_chain[0].id == mol_ids[0]


def test_neb_client_delete(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    neb_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_pbe_opt2")

    with storage_socket.session_scope() as session:
        rec = session.get(NEBRecordORM, neb_id)

        # Children are singlepoints, optimizations, and the trajectory of the optimizations (also singlepoints)
        direct_child_ids = [x.singlepoint_id for x in rec.singlepoints]
        opt_ids = [x.optimization_id for x in rec.optimizations]
        direct_child_ids.extend(opt_ids)

        child_ids = direct_child_ids.copy()
        for opt in rec.optimizations:
            traj_ids = [x.singlepoint_id for x in opt.optimization_record.trajectory]
            child_ids.extend(traj_ids)

    # Some duplicates here
    direct_child_ids = list(set(direct_child_ids))
    child_ids = list(set(child_ids))

    meta = snowflake_client.delete_records(neb_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)
    neb_rec = snowflake_client.get_records(neb_id)
    assert neb_rec.children_status == {RecordStatusEnum.complete: len(direct_child_ids)}

    snowflake_client.undelete_records(neb_id)

    meta = snowflake_client.delete_records(neb_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)
    neb_rec = snowflake_client.get_records(neb_id)
    assert neb_rec.children_status == {RecordStatusEnum.deleted: len(direct_child_ids)}

    meta = snowflake_client.delete_records(neb_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = snowflake_client.get_nebs(neb_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    query_res_l = list(query_res)
    assert len(query_res_l) == 0


def test_neb_client_harddelete_nochildren(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    neb_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_pbe_opt2")

    with storage_socket.session_scope() as session:
        rec = session.get(NEBRecordORM, neb_id)
        child_ids = [x.singlepoint_id for x in rec.singlepoints]

    meta = snowflake_client.delete_records(neb_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    for cid in child_ids:
        child_rec = snowflake_client.get_records(cid, missing_ok=True)
        assert child_rec is not None


def test_neb_client_delete_opt_inuse(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    neb_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_pbe_opt2")

    with storage_socket.session_scope() as session:
        rec = session.get(NEBRecordORM, neb_id)
        child_ids = [x.singlepoint_id for x in rec.singlepoints]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_neb_client_query(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id_1, _ = submit_test_data(storage_socket, "neb_HCN_psi4_pbe")
    id_2, _ = submit_test_data(storage_socket, "neb_HCN_psi4_pbe0_opt1")
    id_3, _ = submit_test_data(storage_socket, "neb_HCN_psi4_pbe_opt2")
    id_4, _ = submit_test_data(storage_socket, "neb_HCN_psi4_b3lyp_opt3")

    all_records = snowflake_client.get_nebs([id_1, id_2, id_3, id_4], include=["initial_chain"])

    # mol_ids of just first chain (11 images, the other three have 7 images).
    mol_ids = [x.id for x in all_records[1].initial_chain]

    query_res = snowflake_client.query_nebs(qc_program=["psi4"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    query_res = snowflake_client.query_nebs(qc_program=["nothing"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_nebs(molecule_id=mol_ids[1])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # Multiple molecules that belong to the same NEB calculation
    query_res = snowflake_client.query_nebs(molecule_id=mol_ids[1:4])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    query_res = snowflake_client.query_nebs(molecule_id=[mol_ids[10]])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    query_res = snowflake_client.query_nebs(molecule_id=999999999)
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_nebs(program=["geometric"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    query_res = snowflake_client.query_nebs(program=["geometric123"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_nebs(qc_basis=["6-31g"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_nebs(qc_basis=[None])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_nebs(qc_basis=[""])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for method
    query_res = snowflake_client.query_nebs(qc_method=["b3lyP"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # Query by default returns everything
    query_res = snowflake_client.query_nebs()
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # Query by default (with a limit)
    query_res = snowflake_client.query_nebs(limit=1)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
