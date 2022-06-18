from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractaltesting import load_molecule_data
from qcportal.managers import ManagerName
from qcportal.records import PriorityEnum
from qcportal.records.singlepoint import QCSpecification, SinglepointDriver
from .testing_helpers import submit_test_data, run_test_data, compare_singlepoint_specs, test_specs

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_singlepoint_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    water = load_molecule_data("water_dimer_minima")
    meta1, id1 = snowflake_client.add_singlepoints(
        [water], "prog", SinglepointDriver.energy, "hf", "sto-3g", None, None, priority=priority, tag=tag
    )
    rec = snowflake_client.get_records(id1, include=["task"])
    assert rec[0].raw_data.task.tag == tag
    assert rec[0].raw_data.task.priority == priority


@pytest.mark.parametrize("spec", test_specs)
def test_singlepoint_client_add_get(snowflake_client: PortalClient, spec: QCSpecification):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = snowflake_client.add_singlepoints(
        all_mols,
        spec.program,
        spec.driver,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        "tag1",
        PriorityEnum.high,
    )
    time_1 = datetime.utcnow()

    recs = snowflake_client.get_singlepoints(id, include=["task", "molecule"])

    for r in recs:
        assert r.record_type == "singlepoint"
        assert r.raw_data.record_type == "singlepoint"
        assert compare_singlepoint_specs(spec, r.raw_data.specification)
        assert r.raw_data.task.spec is None
        assert r.raw_data.task.tag == "tag1"
        assert r.raw_data.task.priority == PriorityEnum.high
        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.task.created_on < time_1

    assert recs[0].raw_data.molecule == water
    assert recs[1].raw_data.molecule == hooh
    assert recs[2].raw_data.molecule == ne4


def test_singlepoint_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = test_specs[0]

    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([ne4])

    # Now add records
    meta, ids = snowflake_client.add_singlepoints(
        all_mols,
        spec.program,
        spec.driver,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        "tag1",
        PriorityEnum.high,
    )

    assert meta.success
    recs = snowflake_client.get_singlepoints(ids, include=["molecule"])

    assert len(recs) == 3
    assert recs[2].raw_data.molecule_id == mol_ids[0]
    assert recs[2].raw_data.molecule == ne4


def test_singlepoint_client_delete(
    snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName
):
    sp_id = run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")

    # deleting with children is ok (even though we don't have children)
    meta = snowflake_client.delete_records(sp_id, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]

    meta = snowflake_client.delete_records(sp_id, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]

    recs = snowflake_client.get_singlepoints(sp_id, missing_ok=True)
    assert recs is None


def test_singlepoint_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1")
    id_2, _ = submit_test_data(storage_socket, "sp_psi4_peroxide_energy_wfn")
    id_3, _ = submit_test_data(storage_socket, "sp_rdkit_water_energy")

    recs = storage_socket.records.singlepoint.get([id_1, id_2, id_3])

    # query for molecule
    query_res = snowflake_client.query_singlepoints(molecule_id=[recs[1]["molecule_id"]])
    assert query_res.current_meta.n_found == 1

    # query for program
    query_res = snowflake_client.query_singlepoints(program="psi4")
    assert query_res.current_meta.n_found == 2

    # query for basis
    query_res = snowflake_client.query_singlepoints(basis="sTO-3g")
    assert query_res.current_meta.n_found == 1

    query_res = snowflake_client.query_singlepoints(basis=[None])
    assert query_res.current_meta.n_found == 1

    query_res = snowflake_client.query_singlepoints(basis="")
    assert query_res.current_meta.n_found == 1

    # query for method
    query_res = snowflake_client.query_singlepoints(method=["b3lyP"])
    assert query_res.current_meta.n_found == 2

    # driver
    query_res = snowflake_client.query_singlepoints(driver=[SinglepointDriver.energy])
    assert query_res.current_meta.n_found == 3

    # Some empty queries
    query_res = snowflake_client.query_singlepoints(driver=[SinglepointDriver.properties])
    assert query_res.current_meta.n_found == 0

    # Some empty queries
    query_res = snowflake_client.query_singlepoints(basis=["madeupbasis"])
    assert query_res.current_meta.n_found == 0

    # Query by default returns everything
    query_res = snowflake_client.query_singlepoints()
    assert query_res.current_meta.n_found == 3

    # Query by default (with a limit)
    query_res = snowflake_client.query_singlepoints(limit=1)
    assert query_res.current_meta.n_found == 3
    assert len(list(query_res)) == 1
