"""
Tests the torsiondrive record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.records import PriorityEnum
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQCInputSpecification,
)
from qcportal.records.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveInputSpecification,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient
    from typing import Optional


from .test_sockets import _test_specs, compare_torsiondrive_specs


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_torsiondrive_client_tag_priority_as_service(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    peroxide2 = load_molecule_data("peroxide2")
    meta1, id1 = snowflake_client.add_torsiondrives(
        [[peroxide2]],
        "torsiondrive",
        optimization_specification=OptimizationInputSpecification(
            program="geometric",
            qc_specification=OptimizationQCInputSpecification(program="psi4", method="hf", basis="sto-3g"),
        ),
        keywords=TorsiondriveKeywords(dihedrals=[(1, 2, 3, 4)], grid_spacing=[15], energy_upper_limit=0.04),
        priority=priority,
        tag=tag,
    )
    rec = snowflake_client.get_records(id1, include_service=True)
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", _test_specs)
def test_torsiondrive_client_add_get(snowflake_client: PortalClient, spec: TorsiondriveInputSpecification):
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

    recs = snowflake_client.get_torsiondrives(id, include_service=True, include_initial_molecules=True)
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
    spec = _test_specs[0]

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

    recs = snowflake_client.get_torsiondrives(id, include_initial_molecules=True)
    assert len(recs) == 2
    assert recs[0].raw_data.id == recs[1].raw_data.id

    rec_mols = {x.id for x in recs[0].raw_data.initial_molecules}
    _, mol_ids_2 = snowflake_client.add_molecules([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_torsiondrive_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("td_H2O2_psi4_b3lyp")
    input_spec_2, molecules_2, result_data_2 = load_procedure_data("td_H2O2_psi4_pbe")
    input_spec_3, molecules_3, result_data_3 = load_procedure_data("td_C9H11NO2_psi4_b3lyp-d3bj")
    input_spec_4, molecules_4, result_data_4 = load_procedure_data("td_H2O2_psi4_bp86")

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    meta_2, id_2 = storage_socket.records.torsiondrive.add(
        [molecules_2], input_spec_2, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    meta_3, id_3 = storage_socket.records.torsiondrive.add(
        [molecules_3], input_spec_3, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    meta_4, id_4 = storage_socket.records.torsiondrive.add(
        [molecules_4], input_spec_4, tag="*", priority=PriorityEnum.normal, as_service=True
    )
    assert meta_1.success and meta_2.success and meta_3.success and meta_4.success

    meta, td = snowflake_client.query_torsiondrives(qc_program=["psi4"])
    assert meta.n_found == 4

    meta, td = snowflake_client.query_torsiondrives(qc_program=["nothing"])
    assert meta.n_found == 0

    _, init_mol_id = storage_socket.molecules.add(molecules_1 + molecules_2 + molecules_3 + molecules_4)
    meta, td = snowflake_client.query_torsiondrives(initial_molecule_id=[init_mol_id[0], 9999])
    assert meta.n_found == 3

    # query for optimization program
    meta, td = snowflake_client.query_torsiondrives(optimization_program=["geometric"])
    assert meta.n_found == 4

    # query for optimization program
    meta, td = snowflake_client.query_torsiondrives(optimization_program=["geometric123"])
    assert meta.n_found == 0

    # query for basis
    meta, td = snowflake_client.query_torsiondrives(qc_basis=["sTO-3g"])
    assert meta.n_found == 3

    meta, td = snowflake_client.query_torsiondrives(qc_basis=[None])
    assert meta.n_found == 0

    meta, td = snowflake_client.query_torsiondrives(qc_basis=[""])
    assert meta.n_found == 0

    # query for method
    meta, td = snowflake_client.query_torsiondrives(qc_method=["b3lyP"])
    assert meta.n_found == 1

    kw_id = td[0].raw_data.specification.optimization_specification.qc_specification.keywords_id
    meta, td = snowflake_client.query_torsiondrives(qc_keywords_id=[kw_id])
    assert meta.n_found == 3

    # Query by default returns everything
    meta, td = snowflake_client.query_torsiondrives()
    assert meta.n_found == 4

    # Query by default (with a limit)
    meta, td = snowflake_client.query_torsiondrives(limit=1)
    assert meta.n_found == 4
    assert meta.n_returned == 1
