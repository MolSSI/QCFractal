from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.records import PriorityEnum
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.manybody import ManybodySpecification, ManybodyKeywords

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

from .test_sockets import _test_specs, compare_manybody_specs


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

    rec = snowflake_client.get_records(id1, include_service=True)
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", _test_specs)
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

    recs = snowflake_client.get_manybodys(
        id1, include_service=True, include_clusters=True, include_initial_molecule=True
    )
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
    spec = _test_specs[0]

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

    recs = snowflake_client.get_manybodys(id1, include_initial_molecule=True)
    assert len(recs) == 3
    assert recs[0].raw_data.id == recs[2].raw_data.id
    assert recs[0].raw_data.id != recs[1].raw_data.id

    assert recs[0].raw_data.initial_molecule.get_hash() == mol1.get_hash()
    assert recs[1].raw_data.initial_molecule.get_hash() == mol2.get_hash()


def test_manybody_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("mb_none_he4_psi4_mp2")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("mb_cp_he4_psi4_mp2")

    meta_1, id_1 = storage_socket.records.manybody.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta_2, id_2 = storage_socket.records.manybody.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    assert meta_1.success and meta_2.success

    meta, mb = snowflake_client.query_manybodys(program=["manybody"])
    assert meta.n_found == 2

    meta, mb = snowflake_client.query_manybodys(program=["nothing"])
    assert meta.n_found == 0

    _, init_mol_id = storage_socket.molecules.add([molecule_1, molecule_2])

    meta, mb = snowflake_client.query_manybodys(initial_molecule_id=[9999])
    assert meta.n_found == 0

    meta, mb = snowflake_client.query_manybodys(initial_molecule_id=[init_mol_id[0], 9999])
    assert meta.n_found == 2

    # query for basis
    meta, mb = snowflake_client.query_manybodys(qc_basis=["DEF2-tzvp"])
    assert meta.n_found == 0

    meta, mb = snowflake_client.query_manybodys(qc_basis=["auG-cC-pVDZ"])
    assert meta.n_found == 2

    meta, mb = snowflake_client.query_manybodys(qc_basis=[None])
    assert meta.n_found == 0

    meta, mb = snowflake_client.query_manybodys(qc_basis=[""])
    assert meta.n_found == 0

    # query for method
    meta, mb = snowflake_client.query_manybodys(qc_method=["hf"])
    assert meta.n_found == 0

    meta, mb = snowflake_client.query_manybodys(qc_method=["mp2"])
    assert meta.n_found == 2

    # Query by default returns everything
    meta, mb = snowflake_client.query_manybodys()
    assert meta.n_found == 2

    # Query by default (with a limit)
    meta, mb = snowflake_client.query_manybodys(limit=1)
    assert meta.n_found == 2
    assert meta.n_returned == 1
