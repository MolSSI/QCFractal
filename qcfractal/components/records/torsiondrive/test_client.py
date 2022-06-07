from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data
from qcportal.records import PriorityEnum
from qcportal.records.optimization import (
    OptimizationSpecification,
)
from qcportal.records.singlepoint import QCSpecification
from qcportal.records.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveSpecification,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

from qcfractal.components.records.torsiondrive.testing_helpers import (
    compare_torsiondrive_specs,
    test_specs,
    submit_test_data,
)


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
