from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data
from qcportal.records import PriorityEnum
from qcportal.records.manybody import ManybodySpecification, ManybodyKeywords
from qcportal.records.singlepoint import QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

from qcfractal.components.records.manybody.testing_helpers import (
    compare_manybody_specs,
    test_specs,
    submit_test_data,
)


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_manybody_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    water = load_molecule_data("water_dimer_minima")

    sp_spec = QCSpecification(
        program="prog",
        driver="energy",
        method="hf",
        basis="sto-3g",
    )

    kw = ManybodyKeywords(max_nbody=1, bsse_correction="none")

    meta1, id1 = snowflake_client.add_manybodys([water], "manybody", sp_spec, kw, tag=tag, priority=priority)

    rec = snowflake_client.get_records(id1, include=["service"])
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", test_specs)
def test_manybody_client_add_get(snowflake_client: PortalClient, spec: ManybodySpecification):
    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")

    time_0 = datetime.utcnow()
    meta1, id1 = snowflake_client.add_manybodys(
        [water2, water4],
        spec.program,
        spec.singlepoint_specification,
        spec.keywords,
        tag="tag1",
        priority=PriorityEnum.low,
    )
    time_1 = datetime.utcnow()
    assert meta1.success

    recs = snowflake_client.get_manybodys(id1, include=["service", "clusters", "initial_molecule"])
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "manybody"
        assert r.raw_data.record_type == "manybody"
        assert compare_manybody_specs(spec, r.raw_data.specification)

        assert r.raw_data.service.tag == "tag1"
        assert r.raw_data.service.priority == PriorityEnum.low

        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.service.created_on < time_1

    assert recs[0].raw_data.initial_molecule.get_hash() == water2.get_hash()
    assert recs[1].raw_data.initial_molecule.get_hash() == water4.get_hash()


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
        spec.singlepoint_specification,
        spec.keywords,
        tag="tag1",
        priority=PriorityEnum.low,
    )

    assert meta1.success

    assert meta1.success
    assert meta1.n_inserted == 2
    assert meta1.n_existing == 1

    recs = snowflake_client.get_manybodys(id1, include=["initial_molecule"])
    assert len(recs) == 3
    assert recs[0].raw_data.id == recs[2].raw_data.id
    assert recs[0].raw_data.id != recs[1].raw_data.id

    assert recs[0].raw_data.initial_molecule.get_hash() == mol1.get_hash()
    assert recs[1].raw_data.initial_molecule.get_hash() == mol2.get_hash()


def test_manybody_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "mb_none_he4_psi4_mp2")
    id_2, _ = submit_test_data(storage_socket, "mb_cp_he4_psi4_mp2")

    all_mbs = snowflake_client.get_manybodys([id_1, id_2])
    mol_ids = [x.initial_molecule_id for x in all_mbs]

    query_res = snowflake_client.query_manybodys(program=["manybody"])
    assert query_res.current_meta.n_found == 2

    query_res = snowflake_client.query_manybodys(program=["nothing"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_manybodys(initial_molecule_id=[9999])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_manybodys(initial_molecule_id=[mol_ids[0], 9999])
    assert query_res.current_meta.n_found == 2

    # query for basis
    query_res = snowflake_client.query_manybodys(qc_basis=["DEF2-tzvp"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_manybodys(qc_basis=["auG-cC-pVDZ"])
    assert query_res.current_meta.n_found == 2

    query_res = snowflake_client.query_manybodys(qc_basis=[None])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_manybodys(qc_basis=[""])
    assert query_res.current_meta.n_found == 0

    # query for method
    query_res = snowflake_client.query_manybodys(qc_method=["hf"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_manybodys(qc_method=["mp2"])
    assert query_res.current_meta.n_found == 2

    # Query by default returns everything
    query_res = snowflake_client.query_manybodys()
    assert query_res.current_meta.n_found == 2

    # Query by default (with a limit)
    query_res = snowflake_client.query_manybodys(limit=1)
    assert query_res.current_meta.n_found == 2
