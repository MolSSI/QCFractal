"""
Tests the singlepoint record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.records import PriorityEnum
from qcportal.records.singlepoint import (
    QCSpecification,
    SinglepointDriver,
)
from .test_sockets import _test_specs

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient
    from typing import Optional

from .test_sockets import compare_singlepoint_specs


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_singlepoint_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    water = load_molecule_data("water_dimer_minima")
    meta1, id1 = snowflake_client.add_singlepoints(
        [water], "prog", SinglepointDriver.energy, "hf", "sto-3g", None, None, priority=priority, tag=tag
    )
    rec = snowflake_client.get_records(id1, include_task=True)
    assert rec[0].raw_data.task.tag == tag
    assert rec[0].raw_data.task.priority == priority


@pytest.mark.parametrize("spec", _test_specs)
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

    recs = snowflake_client.get_singlepoints(id, include_task=True, include_molecule=True)

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
    spec = _test_specs[0]

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
    recs = snowflake_client.get_singlepoints(ids, include_molecule=True)

    assert len(recs) == 3
    assert recs[2].raw_data.molecule_id == mol_ids[0]
    assert recs[2].raw_data.molecule == ne4


def test_singlepoint_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("rdkit_water_energy")

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta2, id2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta3, id3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )

    recs = storage_socket.records.singlepoint.get(id1 + id2 + id3)

    # query for molecule
    meta, sp = snowflake_client.query_singlepoints(molecule_id=[recs[1]["molecule_id"]])
    assert meta.n_found == 1
    assert sp[0].raw_data.id == id2[0]

    # query for program
    meta, sp = snowflake_client.query_singlepoints(program="psi4")
    assert meta.n_found == 2

    # query for basis
    meta, sp = snowflake_client.query_singlepoints(basis="sTO-3g")
    assert meta.n_found == 1

    meta, sp = snowflake_client.query_singlepoints(basis=[None])
    assert meta.n_found == 1
    assert sp[0].raw_data.id == id3[0]

    meta, sp = snowflake_client.query_singlepoints(basis="")
    assert meta.n_found == 1
    assert sp[0].raw_data.id == id3[0]

    # query for method
    meta, sp = snowflake_client.query_singlepoints(method=["b3lyP"])
    assert meta.n_found == 2

    # keyword id
    meta, sp = snowflake_client.query_singlepoints(keywords_id=[recs[0]["specification"]["keywords_id"]])
    assert meta.n_found == 3  # All have empty keywords

    # driver
    meta, sp = snowflake_client.query_singlepoints(driver=[SinglepointDriver.energy])
    assert meta.n_found == 3

    # Some empty queries
    meta, sp = snowflake_client.query_singlepoints(driver=[SinglepointDriver.properties])
    assert meta.n_found == 0

    # Some empty queries
    meta, sp = snowflake_client.query_singlepoints(basis=["madeupbasis"])
    assert meta.n_found == 0

    # Query by default returns everything
    meta, sp = snowflake_client.query_singlepoints()
    assert meta.n_found == 3

    # Query by default (with a limit)
    meta, sp = snowflake_client.query_singlepoints(limit=1)
    assert meta.n_found == 3
    assert meta.n_returned == 1
