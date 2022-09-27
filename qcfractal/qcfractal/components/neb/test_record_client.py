from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.neb import (
    NEBSpecification,
    NEBKeywords,
)
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient
    from qcportal.managers import ManagerName

from qcfractal.components.neb.testing_helpers import (
    compare_neb_specs,
    test_specs,
    submit_test_data,
    run_test_data,
)


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_neb_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    meta1, id1 = snowflake_client.add_nebs(
        [chain],
        "geometric",
        QCSpecification(program="psi4", method="hf", basis="sto-3g", driver="gradient"),
        NEBKeywords(),
        priority=priority,
        tag=tag,
    )
    rec = snowflake_client.get_records(id1, include=["service"])
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", test_specs)
def test_neb_client_add_get(snowflake_client: PortalClient, spec: NEBSpecification):
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)]

    time_0 = datetime.utcnow()
    meta, id = snowflake_client.add_nebs(
        initial_chains=[chain1, chain2],
        program=spec.program,
        keywords=spec.keywords,
        singlepoint_specification=spec.singlepoint_specification,
        tag="tag1",
        priority=PriorityEnum.low,
    )

    time_1 = datetime.utcnow()
    assert meta.success

    recs = snowflake_client.get_nebs(id, include=["service", "initial_chain"])

    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "neb"
        assert r.raw_data.record_type == "neb"
        assert compare_neb_specs(spec, r.raw_data.specification)

        assert r.raw_data.service.tag == "tag1"
        assert r.raw_data.service.priority == PriorityEnum.low

        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.service.created_on < time_1

    assert len(recs[0].initial_chain) == 11  # default image number
    assert len(recs[1].initial_chain) == 11

    assert recs[0].initial_chain[0].get_hash() == chain1[0].get_hash()

    hash1 = recs[0].initial_chain[0].get_hash()
    hash2 = recs[1].initial_chain[-1].get_hash()
    assert {hash1, hash2} == {chain1[0].get_hash(), chain2[-1].get_hash()}


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
        tag="tag1",
        priority=PriorityEnum.low,
    )
    recs = snowflake_client.get_nebs(id)

    assert len(recs) == 2
    assert recs[0].initial_chain[0].id == mol_ids[0]


# TODO: run_test_data is not working for neb. Probably bad procedure json files..
def test_neb_client_delete(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    neb_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_b3lyp")

    rec = storage_socket.records.neb.get([neb_id], include=["singlepoints"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["singlepoints"][-1]]

    meta = snowflake_client.delete_records(neb_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)

    snowflake_client.undelete_records(neb_id)

    meta = snowflake_client.delete_records(neb_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)

    meta = snowflake_client.delete_records(neb_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = snowflake_client.get_torsiondrives(neb_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    assert query_res.current_meta.n_found == 0


def test_torsiondrive_client_harddelete_nochildren(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    neb_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_b3lyp")

    rec = storage_socket.records.torsiondrive.get([neb_id], include=["singlepoints"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["singlepoints"]]

    meta = snowflake_client.delete_records(neb_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_torsiondrives(neb_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


def test_neb_client_delete_opt_inuse(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    neb_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_b3lyp")

    rec = storage_socket.records.neb.get([neb_id], include=["singlepoints"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["singlepoints"]]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_torsiondrive_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "neb_HCN_psi4_b3lyp")
    id_2, _ = submit_test_data(storage_socket, "neb_HCN_psi4_pbe")
    id_3, _ = submit_test_data(storage_socket, "neb_HCN_psi4_hf")
    id_4, _ = submit_test_data(storage_socket, "neb_HCN_psi4_bp86")

    all_records = snowflake_client.get_nebs([id_1, id_2, id_3, id_4], include=["initial_chain"])
    # mol_ids of just first chain (11 images, the other three have 7 images).
    neb_ids = [x.id for x in all_records]
    print(neb_ids)

    query_res = snowflake_client.query_nebs(qc_program=["psi4"])
    assert query_res.current_meta.n_found == 4

    query_res = snowflake_client.query_nebs(qc_program=["nothing"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_nebs(initial_chain_id=[neb_ids[0], 9999])
    assert query_res.current_meta.n_found == 11

    query_res = snowflake_client.query_nebs(program=["geometric"])
    assert query_res.current_meta.n_found == 4

    query_res = snowflake_client.query_nebs(program=["geometric123"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_nebs(qc_basis=["6-31g"])
    assert query_res.current_meta.n_found == 4

    query_res = snowflake_client.query_nebs(qc_basis=[None])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_nebs(qc_basis=[""])
    assert query_res.current_meta.n_found == 0

    # query for method
    query_res = snowflake_client.query_nebs(qc_method=["b3lyP"])
    assert query_res.current_meta.n_found == 1

    # Query by default returns everything
    query_res = snowflake_client.query_nebs()
    assert query_res.current_meta.n_found == 4

    # Query by default (with a limit)
    query_res = snowflake_client.query_nebs(limit=1)
    assert query_res.current_meta.n_found == 4
