from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.torsiondrive.record_db_models import TorsiondriveRecordORM
from qcportal.optimization import OptimizationSpecification
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification
from qcportal.torsiondrive import TorsiondriveKeywords, TorsiondriveSpecification
from qcportal.utils import now_at_utc
from .testing_helpers import compare_torsiondrive_specs, test_specs, submit_test_data, run_test_data

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal import PortalClient


def test_torsiondrive_client_tag_priority_as_service(snowflake_client: PortalClient):
    peroxide2 = load_molecule_data("peroxide2")

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        meta1, id1 = snowflake_client.add_torsiondrives(
            [[peroxide2]],
            "torsiondrive",
            optimization_specification=OptimizationSpecification(
                program="geometric",
                qc_specification=QCSpecification(
                    program="psi4",
                    method="hf",
                    basis="sto-3g",
                    driver="deferred",
                    keywords={"tag_priority": [tag, priority]},
                ),
            ),
            keywords=TorsiondriveKeywords(dihedrals=[(1, 2, 3, 4)], grid_spacing=[15], energy_upper_limit=0.04),
            compute_priority=priority,
            compute_tag=tag,
        )

        assert meta1.n_inserted == 1
        rec = snowflake_client.get_records(id1, include=["service"])
        assert rec[0].service.compute_tag == tag
        assert rec[0].service.compute_priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_torsiondrive_client_add_get(
    submitter_client: PortalClient, spec: TorsiondriveSpecification, owner_group: Optional[str]
):
    hooh = load_molecule_data("peroxide2")
    td_mol_1 = load_molecule_data("td_C9H11NO2_1")
    td_mol_2 = load_molecule_data("td_C9H11NO2_2")

    time_0 = now_at_utc()
    meta, id = submitter_client.add_torsiondrives(
        [[hooh], [td_mol_1, td_mol_2]],
        "torsiondrive",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=owner_group,
    )
    time_1 = now_at_utc()
    assert meta.success

    recs = submitter_client.get_torsiondrives(id, include=["service", "initial_molecules"])
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "torsiondrive"
        assert r.record_type == "torsiondrive"
        assert compare_torsiondrive_specs(spec, r.specification)

        assert r.status == RecordStatusEnum.waiting
        assert r.children_status == {}

        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert len(recs[0].initial_molecules) == 1
    assert len(recs[1].initial_molecules) == 2

    assert recs[0].initial_molecules[0].get_hash() == hooh.get_hash()

    # Not necessarily in the input order
    hash1 = recs[1].initial_molecules[0].get_hash()
    hash2 = recs[1].initial_molecules[1].get_hash()
    assert {hash1, hash2} == {td_mol_1.get_hash(), td_mol_2.get_hash()}


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("find_existing", [True, False])
def test_torsiondrive_client_add_duplicate(
    submitter_client: PortalClient,
    spec: TorsiondriveSpecification,
    find_existing: bool,
):
    hooh = load_molecule_data("peroxide2")
    td_mol_1 = load_molecule_data("td_C9H11NO2_1")
    td_mol_2 = load_molecule_data("td_C9H11NO2_2")

    all_mols = [[hooh], [td_mol_1, td_mol_2]]

    meta, id = submitter_client.add_torsiondrives(
        all_mols,
        "torsiondrive",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
        owner_group=None,
        find_existing=True,
    )

    assert meta.success
    assert meta.n_inserted == len(all_mols)

    meta, id2 = submitter_client.add_torsiondrives(
        all_mols,
        "torsiondrive",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
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


def test_torsiondrive_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = test_specs[0]

    mol1 = load_molecule_data("td_C9H11NO2_1")
    mol2 = load_molecule_data("td_C9H11NO2_2")

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([mol2])

    # Now add records
    meta, id = snowflake_client.add_torsiondrives(
        [[mol1, mol2], [mol2, mol1]],
        "torsiondrive",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
        compute_tag="tag1",
        compute_priority=PriorityEnum.low,
    )

    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1

    recs = snowflake_client.get_torsiondrives(id, include=["initial_molecules"])
    assert len(recs) == 2
    assert recs[0].id == recs[1].id

    rec_mols = {x.id for x in recs[0].initial_molecules}
    _, mol_ids_2 = snowflake_client.add_molecules([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_torsiondrive_client_delete(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    td_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_mopac_pm6")

    with storage_socket.session_scope() as session:
        rec = session.get(TorsiondriveRecordORM, td_id)
        child_ids = [x.optimization_id for x in rec.optimizations]

    meta = snowflake_client.delete_records(td_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)
    td_rec = snowflake_client.get_records(td_id)
    assert td_rec.children_status == {RecordStatusEnum.complete: len(child_ids)}

    snowflake_client.undelete_records(td_id)

    meta = snowflake_client.delete_records(td_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)
    td_rec = snowflake_client.get_records(td_id)
    assert td_rec.children_status == {RecordStatusEnum.deleted: len(child_ids)}

    meta = snowflake_client.delete_records(td_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = snowflake_client.get_torsiondrives(td_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)

    # DB should be pretty empty now
    query_res = snowflake_client.query_records()
    query_res_l = list(query_res)
    assert len(query_res_l) == 0


def test_torsiondrive_client_harddelete_nochildren(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    td_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_mopac_pm6")

    with storage_socket.session_scope() as session:
        rec = session.get(TorsiondriveRecordORM, td_id)
        child_ids = [x.optimization_id for x in rec.optimizations]

    meta = snowflake_client.delete_records(td_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_torsiondrives(td_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


def test_torsiondrive_client_delete_opt_inuse(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    td_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_mopac_pm6")

    with storage_socket.session_scope() as session:
        rec = session.get(TorsiondriveRecordORM, td_id)
        child_ids = [x.optimization_id for x in rec.optimizations]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_torsiondrive_client_query(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id_1, _ = submit_test_data(storage_socket, "td_H2O2_mopac_pm6")
    id_2, _ = submit_test_data(storage_socket, "td_H2O2_psi4_pbe")
    id_3, _ = submit_test_data(storage_socket, "td_C9H11NO2_mopac_pm6")
    id_4, _ = submit_test_data(storage_socket, "td_H2O2_psi4_pbe0")

    all_tds = snowflake_client.get_torsiondrives([id_1, id_2, id_3, id_4], include=["initial_molecules"])
    mol_ids = [x.initial_molecules[0].id for x in all_tds]

    query_res = snowflake_client.query_torsiondrives(qc_program=["psi4"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_torsiondrives(qc_program=["nothing"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    query_res = snowflake_client.query_torsiondrives(initial_molecule_id=[mol_ids[0], 9999])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # query for optimization program
    query_res = snowflake_client.query_torsiondrives(optimization_program=["geometric"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # query for optimization program
    query_res = snowflake_client.query_torsiondrives(optimization_program=["geometric123"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # query for basis
    query_res = snowflake_client.query_torsiondrives(qc_basis=["sTO-3g"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_torsiondrives(qc_basis=[None])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_torsiondrives(qc_basis=[""])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # query for method
    query_res = snowflake_client.query_torsiondrives(qc_method=["pm6"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # Query by default returns everything
    query_res = snowflake_client.query_torsiondrives()
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # Query by default (with a limit)
    query_res = snowflake_client.query_torsiondrives(limit=1)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
