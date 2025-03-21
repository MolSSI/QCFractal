from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Optional

import pytest

from qcarchivetesting import load_molecule_data
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.singlepoint import QCSpecification, SinglepointDriver
from qcportal.utils import now_at_utc
from .testing_helpers import submit_test_data, run_test_data, compare_singlepoint_specs, test_specs

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal import PortalClient


def test_singlepoint_client_tag_priority(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")

    for tag, priority in itertools.product(["*", "tag99"], list(PriorityEnum)):
        meta1, id1 = snowflake_client.add_singlepoints(
            [water],
            "prog",
            SinglepointDriver.energy,
            "hf",
            "sto-3g",
            {"tag_priority": [tag, priority]},
            None,
            compute_priority=priority,
            compute_tag=tag,
        )
        assert meta1.n_inserted == 1

        rec = snowflake_client.get_records(id1, include=["task"])
        assert rec[0].task.compute_tag == tag
        assert rec[0].task.compute_priority == priority


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_singlepoint_client_add_get(submitter_client: PortalClient, spec: QCSpecification, owner_group: Optional[str]):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = now_at_utc()
    meta, id = submitter_client.add_singlepoints(
        all_mols,
        spec.program,
        spec.driver,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        "tag1",
        PriorityEnum.high,
        owner_group,
    )
    time_1 = now_at_utc()

    recs = submitter_client.get_singlepoints(id, include=["task", "molecule"])

    for r in recs:
        assert r.record_type == "singlepoint"
        assert r.record_type == "singlepoint"
        assert compare_singlepoint_specs(spec, r.specification)

        assert r.status == RecordStatusEnum.waiting
        assert r.children_status == {}

        assert r.task.function is None
        assert r.task.compute_tag == "tag1"
        assert r.task.compute_priority == PriorityEnum.high
        assert r.owner_user == submitter_client.username
        assert r.owner_group == owner_group
        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert recs[0].molecule == water
    assert recs[1].molecule == hooh
    assert recs[2].molecule == ne4


@pytest.mark.parametrize("spec", test_specs)
@pytest.mark.parametrize("find_existing", [True, False])
def test_singlepoint_client_add_duplicate(submitter_client: PortalClient, spec: QCSpecification, find_existing: bool):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    meta, id = submitter_client.add_singlepoints(
        all_mols,
        spec.program,
        spec.driver,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        "tag1",
        PriorityEnum.high,
        None,
        find_existing=True,
    )

    assert meta.n_inserted == len(all_mols)

    meta, id2 = submitter_client.add_singlepoints(
        all_mols,
        spec.program,
        spec.driver,
        spec.method,
        spec.basis,
        spec.keywords,
        spec.protocols,
        "tag1",
        PriorityEnum.high,
        None,
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


def test_singlepoint_client_properties(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

    sp_id = run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")

    rec = snowflake_client.get_singlepoints(sp_id)

    assert len(rec.properties) > 0
    assert rec.properties["calcinfo_nbasis"] == rec.properties["calcinfo_nmo"] == 12
    assert rec.properties["calcinfo_nbasis"] == rec.properties["calcinfo_nmo"] == 12


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
    assert recs[2].molecule_id == mol_ids[0]
    assert recs[2].molecule == ne4


def test_singlepoint_client_delete(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()
    snowflake_client = snowflake.client()

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


def test_singlepoint_client_query(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    id_1, _ = submit_test_data(storage_socket, "sp_psi4_benzene_energy_2")
    id_2, _ = submit_test_data(storage_socket, "sp_psi4_peroxide_energy_wfn")
    id_3, _ = submit_test_data(storage_socket, "sp_rdkit_water_energy")

    recs = snowflake_client.get_singlepoints([id_1, id_2, id_3])

    # query for molecule
    query_res = snowflake_client.query_singlepoints(molecule_id=[recs[1].molecule_id])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # query for program
    query_res = snowflake_client.query_singlepoints(program="psi4")
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # query for basis
    query_res = snowflake_client.query_singlepoints(basis="sTO-3g")
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_singlepoints(basis=[None])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_singlepoints(basis="")
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # query for method
    query_res = snowflake_client.query_singlepoints(method=["b3lyP"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # driver
    query_res = snowflake_client.query_singlepoints(driver=[SinglepointDriver.energy])
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # keywords
    query_res = snowflake_client.query_singlepoints(keywords={})
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    query_res = snowflake_client.query_singlepoints(keywords={"maxiter": 100})
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_singlepoints(keywords=[{"maxiter": 100}, {"something": 100}])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_singlepoints(keywords=[{}], program="rdkit")
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_singlepoints(keywords={"maxiter": 100})
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = snowflake_client.query_singlepoints(keywords={"something": 100})
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # Some empty queries
    query_res = snowflake_client.query_singlepoints(driver=[SinglepointDriver.properties])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # Some empty queries
    query_res = snowflake_client.query_singlepoints(basis=["madeupbasis"])
    query_res_l = list(query_res)
    assert len(query_res_l) == 0

    # Query by default returns everything
    query_res = snowflake_client.query_singlepoints()
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # Query by default (with a limit)
    query_res = snowflake_client.query_singlepoints(limit=1)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
