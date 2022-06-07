from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data
from qcportal.managers import ManagerName
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.optimization import (
    OptimizationSpecification,
)
from qcportal.records.singlepoint import QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

from qcfractal.components.records.optimization.testing_helpers import (
    compare_optimization_specs,
    test_specs,
    submit_test_data,
    run_test_data,
)


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_optimization_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    water = load_molecule_data("water_dimer_minima")
    meta1, id1 = snowflake_client.add_optimizations(
        [water],
        "prog",
        QCSpecification(program="prog", method="hf", basis="sto-3g", driver="deferred"),
        priority=priority,
        tag=tag,
    )
    rec = snowflake_client.get_records(id1, include=["task"])
    assert rec[0].raw_data.task.tag == tag
    assert rec[0].raw_data.task.priority == priority


@pytest.mark.parametrize("spec", test_specs)
def test_optimization_client_add_get(snowflake_client: PortalClient, spec: OptimizationSpecification):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = snowflake_client.add_optimizations(
        initial_molecules=all_mols,
        program=spec.program,
        keywords=spec.keywords,
        protocols=spec.protocols,
        qc_specification=spec.qc_specification,
        tag="tag1",
        priority=PriorityEnum.low,
    )

    time_1 = datetime.utcnow()
    assert meta.success

    recs = snowflake_client.get_optimizations(id, include=["task", "initial_molecule"])

    assert len(recs) == 3

    for r in recs:
        assert r.record_type == "optimization"
        assert r.raw_data.record_type == "optimization"
        assert compare_optimization_specs(spec, r.raw_data.specification)

        assert r.task.spec is None
        assert r.raw_data.task.tag == "tag1"
        assert r.raw_data.task.priority == PriorityEnum.low

        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.task.created_on < time_1

    mol1 = snowflake_client.get_molecules([recs[0].raw_data.initial_molecule_id])[0]
    mol2 = snowflake_client.get_molecules([recs[1].raw_data.initial_molecule_id])[0]
    mol3 = snowflake_client.get_molecules([recs[2].raw_data.initial_molecule_id])[0]
    assert mol1.identifiers.molecule_hash == water.get_hash()
    assert recs[0].raw_data.initial_molecule.identifiers.molecule_hash == water.get_hash()

    assert mol2.identifiers.molecule_hash == hooh.get_hash()
    assert recs[1].raw_data.initial_molecule.identifiers.molecule_hash == hooh.get_hash()

    assert mol3.identifiers.molecule_hash == ne4.get_hash()
    assert recs[2].raw_data.initial_molecule.identifiers.molecule_hash == ne4.get_hash()


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
        tag="tag1",
        priority=PriorityEnum.low,
    )
    recs = snowflake_client.get_optimizations(id)

    assert len(recs) == 3
    assert recs[2].raw_data.initial_molecule_id == mol_ids[0]


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
def test_optimization_client_delete(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, opt_file: str
):
    opt_id = run_test_data(storage_socket, activated_manager_name, opt_file)

    rec = storage_socket.records.optimization.get([opt_id], include=["trajectory"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]

    meta = snowflake_client.delete_records(opt_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x.status == RecordStatusEnum.deleted for x in child_recs)

    meta = snowflake_client.delete_records(opt_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = snowflake_client.get_optimizations(opt_id, missing_ok=True)
    assert recs is None

    child_recs = snowflake_client.get_records(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_methane_opt_sometraj"])
def test_optimization_client_delete_traj_inuse(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, opt_file: str
):
    opt_id = run_test_data(storage_socket, activated_manager_name, opt_file)

    rec = storage_socket.records.optimization.get([opt_id], include=["trajectory"])
    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]

    meta = snowflake_client.delete_records(child_ids[0], soft_delete=False)
    assert meta.success is False
    assert meta.error_idx == [0]

    ch_rec = snowflake_client.get_records(child_ids[0])
    assert ch_rec is not None


def test_optimization_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "psi4_fluoroethane_opt_notraj")
    id_2, _ = submit_test_data(storage_socket, "psi4_benzene_opt")
    id_3, _ = submit_test_data(storage_socket, "psi4_methane_opt_sometraj")

    recs = snowflake_client.get_optimizations([id_1, id_2, id_3])

    # query for molecule
    query_res = snowflake_client.query_optimizations(initial_molecule_id=[recs[1].raw_data.initial_molecule_id])
    assert query_res.current_meta.n_found == 1

    # query for program
    query_res = snowflake_client.query_optimizations(program=["psi4"])
    assert query_res.current_meta.n_found == 0

    # query for program
    query_res = snowflake_client.query_optimizations(program=["geometric"])
    assert query_res.current_meta.n_found == 3

    query_res = snowflake_client.query_optimizations(qc_program=["psi4"])
    assert query_res.current_meta.n_found == 3

    # query for basis
    query_res = snowflake_client.query_optimizations(qc_basis=["sTO-3g"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_optimizations(qc_basis=[None])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_optimizations(qc_basis=[""])
    assert query_res.current_meta.n_found == 0

    # query for method
    query_res = snowflake_client.query_optimizations(qc_method=["b3lyP"])
    assert query_res.current_meta.n_found == 3

    # Some empty queries
    query_res = snowflake_client.query_optimizations(program=["madeupprog"])
    assert query_res.current_meta.n_found == 0

    # Query by default returns everything
    query_res = snowflake_client.query_optimizations()
    assert query_res.current_meta.n_found == 3

    # Query by default (with a limit)
    query_res = snowflake_client.query_optimizations(limit=1)
    assert query_res.current_meta.n_found == 3
