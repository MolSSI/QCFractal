from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data
from qcportal.records import PriorityEnum
from qcportal.records.gridoptimization import (
    GridoptimizationKeywords,
    GridoptimizationSpecification,
)
from qcportal.records.optimization import (
    OptimizationSpecification,
)
from qcportal.records.singlepoint import QCSpecification

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient

from qcfractal.components.records.gridoptimization.testing_helpers import (
    compare_gridoptimization_specs,
    test_specs,
    submit_test_data,
)


@pytest.mark.parametrize("tag", ["*", "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_gridoptimization_client_tag_priority(snowflake_client: PortalClient, tag: str, priority: PriorityEnum):
    peroxide2 = load_molecule_data("peroxide2")
    meta1, id1 = snowflake_client.add_gridoptimizations(
        [peroxide2],
        "gridoptimization",
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(program="psi4", driver="deferred", method="hf", basis="sto-3g"),
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
    rec = snowflake_client.get_records(id1, include=["service"])
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", test_specs)
def test_gridoptimization_client_add_get(snowflake_client: PortalClient, spec: GridoptimizationSpecification):
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

    recs = snowflake_client.get_gridoptimizations(id, include=["service", "initial_molecule"])
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
    spec = test_specs[0]

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

    recs = snowflake_client.get_gridoptimizations(id, include=["initial_molecule"])
    assert len(recs) == 4
    assert recs[0].raw_data.id == recs[3].raw_data.id
    assert recs[1].raw_data.id == recs[2].raw_data.id

    rec_mols = {x.raw_data.initial_molecule.id for x in recs}
    _, mol_ids_2 = snowflake_client.add_molecules([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_gridoptimization_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
    id_1, _ = submit_test_data(storage_socket, "go_H2O2_psi4_b3lyp")
    id_2, _ = submit_test_data(storage_socket, "go_H2O2_psi4_pbe")
    id_3, _ = submit_test_data(storage_socket, "go_C4H4N2OS_psi4_b3lyp-d3bj")
    id_4, _ = submit_test_data(storage_socket, "go_H3NS_psi4_pbe")

    all_gos = snowflake_client.get_gridoptimizations([id_1, id_2, id_3, id_4])
    mol_ids = [x.initial_molecule_id for x in all_gos]

    query_res = snowflake_client.query_gridoptimizations(qc_program=["psi4"])
    assert query_res.current_meta.n_found == 4

    query_res = snowflake_client.query_gridoptimizations(qc_program=["nothing"])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_gridoptimizations(initial_molecule_id=[mol_ids[0], 9999])
    assert query_res.current_meta.n_found == 2

    # query for optimization program
    query_res = snowflake_client.query_gridoptimizations(optimization_program=["geometric"])
    assert query_res.current_meta.n_found == 4

    # query for optimization program
    query_res = snowflake_client.query_gridoptimizations(optimization_program=["geometric123"])
    assert query_res.current_meta.n_found == 0

    # query for basis
    query_res = snowflake_client.query_gridoptimizations(qc_basis=["sTO-3g"])
    assert query_res.current_meta.n_found == 3

    query_res = snowflake_client.query_gridoptimizations(qc_basis=[None])
    assert query_res.current_meta.n_found == 0

    query_res = snowflake_client.query_gridoptimizations(qc_basis=[""])
    assert query_res.current_meta.n_found == 0

    # query for method
    query_res = snowflake_client.query_gridoptimizations(qc_method=["b3lyP"])
    assert query_res.current_meta.n_found == 1

    # Query by default returns everything
    query_res = snowflake_client.query_gridoptimizations()
    assert query_res.current_meta.n_found == 4

    # Query by default (with a limit)
    query_res = snowflake_client.query_gridoptimizations(limit=1)
    assert query_res.current_meta.n_found == 4
