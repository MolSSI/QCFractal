from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.optimization import OptimizationSpecification
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.torsiondrive import TorsiondriveKeywords, TorsiondriveSpecification
from .testing_helpers import compare_torsiondrive_specs, test_specs, submit_test_data, run_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient
    from qcportal.managers import ManagerName


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_torsiondrive_client_tag_priority_as_service(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    peroxide2 = load_molecule_data("peroxide2")
    meta1, id1 = snowflake_client.add_torsiondrives(
        [[peroxide2]],
        "torsiondrive",
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(program="psi4", method="hf", basis="sto-3g", driver="deferred"),
        ),
        keywords=TorsiondriveKeywords(dihedrals=[(1, 2, 3, 4)], grid_spacing=[15], energy_upper_limit=0.04),
        priority=priority,
        tag=tag,
    )
    rec = snowflake_client.get_records(id1, include=["service"])
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", test_specs)
def test_torsiondrive_client_add_get(snowflake_client: PortalClient, spec: TorsiondriveSpecification):
    hooh = load_molecule_data("peroxide2")
    td_mol_1 = load_molecule_data("td_C9H11NO2_1")
    td_mol_2 = load_molecule_data("td_C9H11NO2_2")

    time_0 = datetime.utcnow()
    meta, id = snowflake_client.add_torsiondrives(
        [[hooh], [td_mol_1, td_mol_2]],
        "torsiondrive",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
        tag="tag1",
        priority=PriorityEnum.low,
    )
    time_1 = datetime.utcnow()
    assert meta.success

    recs = snowflake_client.get_torsiondrives(id, include=["service", "initial_molecules"])
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "torsiondrive"
        assert r.raw_data.record_type == "torsiondrive"
        assert compare_torsiondrive_specs(spec, r.raw_data.specification)

        assert r.raw_data.service.tag == "tag1"
        assert r.raw_data.service.priority == PriorityEnum.low

        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.service.created_on < time_1

    assert len(recs[0].raw_data.initial_molecules) == 1
    assert len(recs[1].raw_data.initial_molecules) == 2

    assert recs[0].raw_data.initial_molecules[0].get_hash() == hooh.get_hash()

    # Not necessarily in the input order
    hash1 = recs[1].raw_data.initial_molecules[0].get_hash()
    hash2 = recs[1].raw_data.initial_molecules[1].get_hash()
    assert {hash1, hash2} == {td_mol_1.get_hash(), td_mol_2.get_hash()}


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
        tag="tag1",
        priority=PriorityEnum.low,
    )

    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1

    recs = snowflake_client.get_torsiondrives(id, include=["initial_molecules"])
    assert len(recs) == 2
    assert recs[0].raw_data.id == recs[1].raw_data.id

    rec_mols = {x.id for x in recs[0].raw_data.initial_molecules}
    _, mol_ids_2 = snowflake_client.add_molecules([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_torsiondrive_client_delete(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    td_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_psi4_b3lyp")

    rec = storage_socket.records.torsiondrive.get([td_id], include=["optimizations"])
    child_ids = [x["optimization_id"] for x in rec[0]["optimizations"]]

    meta = snowflake_client.delete_records(td_id, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.complete for x in child_recs)

    snowflake_client.undelete_records(td_id)

    meta = snowflake_client.delete_records(td_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)

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
    assert query_res.current_meta.n_found == 0


def test_torsiondrive_client_harddelete_nochildren(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    td_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_psi4_b3lyp")

    rec = storage_socket.records.torsiondrive.get([td_id], include=["optimizations"])
    child_ids = [x["optimization_id"] for x in rec[0]["optimizations"]]

    meta = snowflake_client.delete_records(td_id, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = snowflake_client.get_torsiondrives(td_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is not None for x in child_recs)


def test_torsiondrive_client_delete_opt_inuse(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):

    td_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_psi4_b3lyp")

    rec = storage_socket.records.torsiondrive.get([td_id], include=["optimizations"])
    child_ids = [x["optimization_id"] for x in rec[0]["optimizations"]]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_torsiondrive_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "td_H2O2_psi4_b3lyp")
    id_2, _ = submit_test_data(storage_socket, "td_H2O2_psi4_pbe")
    id_3, _ = submit_test_data(storage_socket, "td_C9H11NO2_psi4_b3lyp-d3bj")
    id_4, _ = submit_test_data(storage_socket, "td_H2O2_psi4_bp86")

    all_tds = snowflake_client.get_torsiondrives([id_1, id_2, id_3, id_4], include=["initial_molecules"])
    mol_ids = [x.initial_molecules[0].id for x in all_tds]

    query_res = snowflake_client.query_torsiondrives(qc_program=["psi4"])
    assert query_res.current_meta.n_found == 4

    query_res = snowflake_client.query_torsiondrives(qc_program=["nothing"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_torsiondrives(initial_molecule_id=[mol_ids[0], 9999])
    assert query_res.current_meta.n_found == 3

    # query for optimization program
    query_res = snowflake_client.query_torsiondrives(optimization_program=["geometric"])
    assert query_res.current_meta.n_found == 4

    # query for optimization program
    query_res = snowflake_client.query_torsiondrives(optimization_program=["geometric123"])
    assert query_res.current_meta.n_found == 0

    # query for basis
    query_res = snowflake_client.query_torsiondrives(qc_basis=["sTO-3g"])
    assert query_res.current_meta.n_found == 3

    query_res = snowflake_client.query_torsiondrives(qc_basis=[None])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_torsiondrives(qc_basis=[""])
    assert query_res.current_meta.n_found == 0

    # query for method
    query_res = snowflake_client.query_torsiondrives(qc_method=["b3lyP"])
    assert query_res.current_meta.n_found == 1

    # Query by default returns everything
    query_res = snowflake_client.query_torsiondrives()
    assert query_res.current_meta.n_found == 4

    # Query by default (with a limit)
    query_res = snowflake_client.query_torsiondrives(limit=1)
    assert query_res.current_meta.n_found == 4
