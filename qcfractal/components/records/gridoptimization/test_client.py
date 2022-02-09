"""
Tests the gridoptimization record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.records import PriorityEnum
from qcportal.records.gridoptimization import (
    GridoptimizationKeywords,
    GridoptimizationInputSpecification,
)
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQCInputSpecification,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient
    from typing import Optional


from .test_sockets import _test_specs, compare_gridoptimization_specs


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_gridoptimization_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    peroxide2 = load_molecule_data("peroxide2")
    meta1, id1 = snowflake_client.add_gridoptimizations(
        [peroxide2],
        "gridoptimization",
        optimization_specification=OptimizationInputSpecification(
            program="geometric",
            qc_specification=OptimizationQCInputSpecification(program="psi4", method="hf", basis="sto-3g"),
        ),
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        priority=priority,
        tag=tag,
    )
    rec = snowflake_client.get_records(id1, include_service=True)
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", _test_specs)
def test_gridoptimization_client_add_get(snowflake_client: PortalClient, spec: GridoptimizationInputSpecification):
    hooh = load_molecule_data("peroxide2")
    h3ns = load_molecule_data("go_H3NS")

    time_0 = datetime.utcnow()
    meta, id = snowflake_client.add_gridoptimizations(
        [hooh, h3ns],
        spec.program,
        spec.optimization_specification,
        spec.keywords,
        tag="tag1",
        priority=PriorityEnum.low,
    )
    time_1 = datetime.utcnow()
    assert meta.success

    recs = snowflake_client.get_gridoptimizations(id, include_service=True, include_initial_molecule=True)
    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "gridoptimization"
        assert r.raw_data.record_type == "gridoptimization"
        assert compare_gridoptimization_specs(spec, r.raw_data.specification)

        assert r.raw_data.service.tag == "tag1"
        assert r.raw_data.service.priority == PriorityEnum.low

        assert time_0 < r.raw_data.created_on < time_1
        assert time_0 < r.raw_data.modified_on < time_1
        assert time_0 < r.raw_data.service.created_on < time_1

    assert recs[0].raw_data.initial_molecule.identifiers.molecule_hash == hooh.get_hash()
    assert recs[1].raw_data.initial_molecule.identifiers.molecule_hash == h3ns.get_hash()


def test_gridoptimization_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = _test_specs[0]

    mol1 = load_molecule_data("go_H3NS")
    mol2 = load_molecule_data("peroxide2")

    # Add a molecule separately
    _, mol_ids = snowflake_client.add_molecules([mol2])

    # Now add records
    meta, id = snowflake_client.add_gridoptimizations(
        [mol1, mol2, mol2, mol1],
        "gridoptimization",
        keywords=spec.keywords,
        optimization_specification=spec.optimization_specification,
        tag="tag1",
        priority=PriorityEnum.low,
    )

    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 2

    recs = snowflake_client.get_gridoptimizations(id, include_initial_molecule=True)
    assert len(recs) == 4
    assert recs[0].raw_data.id == recs[3].raw_data.id
    assert recs[1].raw_data.id == recs[2].raw_data.id

    rec_mols = {x.raw_data.initial_molecule.id for x in recs}
    _, mol_ids_2 = snowflake_client.add_molecules([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_gridoptimization_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("go_H2O2_psi4_b3lyp")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("go_H2O2_psi4_pbe")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("go_C4H4N2OS_psi4_b3lyp-d3bj")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("go_H3NS_psi4_pbe")

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta_2, id_2 = storage_socket.records.gridoptimization.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta_3, id_3 = storage_socket.records.gridoptimization.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )
    meta_4, id_4 = storage_socket.records.gridoptimization.add(
        [molecule_4], input_spec_4, tag="*", priority=PriorityEnum.normal
    )
    assert meta_1.success and meta_2.success and meta_3.success and meta_4.success

    meta, td = snowflake_client.query_gridoptimizations(qc_program=["psi4"])
    assert meta.n_found == 4

    meta, td = snowflake_client.query_gridoptimizations(qc_program=["nothing"])
    assert meta.n_found == 0

    _, init_mol_id = storage_socket.molecules.add([molecule_1, molecule_2, molecule_3, molecule_4])
    meta, td = snowflake_client.query_gridoptimizations(initial_molecule_id=[init_mol_id[0], 9999])
    assert meta.n_found == 2

    # query for optimization program
    meta, td = snowflake_client.query_gridoptimizations(optimization_program=["geometric"])
    assert meta.n_found == 4

    # query for optimization program
    meta, td = snowflake_client.query_gridoptimizations(optimization_program=["geometric123"])
    assert meta.n_found == 0

    # query for basis
    meta, td = snowflake_client.query_gridoptimizations(qc_basis=["sTO-3g"])
    assert meta.n_found == 3

    meta, td = snowflake_client.query_gridoptimizations(qc_basis=[None])
    assert meta.n_found == 0

    meta, td = snowflake_client.query_gridoptimizations(qc_basis=[""])
    assert meta.n_found == 0

    # query for method
    meta, td = snowflake_client.query_gridoptimizations(qc_method=["b3lyP"])
    assert meta.n_found == 1

    kw_id = td[0].raw_data.specification.optimization_specification.qc_specification.keywords_id
    meta, td = snowflake_client.query_gridoptimizations(qc_keywords_id=[kw_id])
    assert meta.n_found == 3

    # Query by default returns everything
    meta, td = snowflake_client.query_gridoptimizations()
    assert meta.n_found == 4

    # Query by default (with a limit)
    meta, td = snowflake_client.query_gridoptimizations(limit=1)
    assert meta.n_found == 4
    assert meta.n_returned == 1
