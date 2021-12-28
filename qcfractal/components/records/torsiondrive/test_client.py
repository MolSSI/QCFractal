"""
Tests the torsiondrive record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.records.optimization.db_models import OptimizationRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal.keywords import KeywordSet
from qcfractal.portal.molecules import Molecule
from qcfractal.portal.outputstore import OutputStore
from qcfractal.portal.records import RecordStatusEnum, PriorityEnum
from qcfractal.portal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQueryBody,
    OptimizationSinglepointInputSpecification,
)
from qcfractal.portal.records.singlepoint import (
    SinglepointDriver,
    SinglepointProtocols,
)
from qcfractal.portal.records.torsiondrive import (
    TorsiondriveKeywords,
    TorsiondriveSpecification,
    TorsiondriveInputSpecification,
)
from qcfractal.testing import load_molecule_data, load_procedure_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcfractal.portal import PortalClient
    from typing import Optional


from .test_sockets import _test_specs, compare_torsiondrive_specs


@pytest.mark.parametrize("tag", [None, "tag99"])
@pytest.mark.parametrize("priority", list(PriorityEnum))
def test_torsiondrive_client_tag_priority_as_service(
    snowflake_client: PortalClient, tag: Optional[str], priority: PriorityEnum
):
    peroxide2 = load_molecule_data("peroxide2")
    meta1, id1 = snowflake_client.add_torsiondrives(
        [[peroxide2]],
        "torsiondrive",
        optimization_specification=OptimizationInputSpecification(
            program="geometric",
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="psi4", method="hf", basis="sto-3g"
            ),
        ),
        keywords=TorsiondriveKeywords(dihedrals=[(1, 2, 3, 4)], grid_spacing=[15], energy_upper_limit=0.04),
        priority=priority,
        tag=tag,
    )
    rec = snowflake_client.get_records(id1, include_service=True)
    assert rec[0].raw_data.service.tag == tag
    assert rec[0].raw_data.service.priority == priority


# TODO
# @pytest.mark.parametrize("tag", [None, "tag99"])
# @pytest.mark.parametrize("priority", list(PriorityEnum))
# def test_torsiondrive_client_tag_priority_as_procedure(
#         snowflake_client: PortalClient, tag: Optional[str], priority: PriorityEnum
# ):
#    peroxide2 = load_molecule_data("peroxide2")
#    meta1, id1 = snowflake_client.add_torsiondrives(
#        [[peroxide2]],
#        "torsiondrive",
#        optimization_specification=OptimizationInputSpecification(
#            program="geometric",
#            singlepoint_specification=OptimizationSinglepointInputSpecification(
#                program="psi4", method="hf", basis="sto-3g"
#            ),
#        ),
#        keywords=TorsiondriveKeywords(dihedrals=[(1, 2, 3, 4)], grid_spacing=[15], energy_upper_limit=0.04),
#        priority=priority,
#        tag=tag,
#    )
#    rec = snowflake_client.get_records(id1, include_service=True)
#    assert rec[0].raw_data.service.tag == tag
#    assert rec[0].raw_data.service.priority == priority


@pytest.mark.parametrize("spec", _test_specs)
def test_torsiondrive_client_add_get(snowflake_client: PortalClient, spec: TorsiondriveInputSpecification):
    hooh = load_molecule_data("peroxide2")
    c8h6_1 = load_molecule_data("td_C8H6_1")
    c8h6_2 = load_molecule_data("td_C8H6_2")

    time_0 = datetime.utcnow()
    meta, id = snowflake_client.add_torsiondrives(
        [[hooh], [c8h6_1, c8h6_2]],
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
    assert {hash1, hash2} == {c8h6_1.get_hash(), c8h6_2.get_hash()}


def test_torsiondrive_client_add_existing_molecule(snowflake_client: PortalClient):
    spec = _test_specs[0]

    mol1 = load_molecule_data("td_C8H6_1")
    mol2 = load_molecule_data("td_C8H6_2")

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


# def test_torsiondrive_client_add_same_1(snowflake_client: PortalClient):
#    spec = OptimizationInputSpecification(
#        program="optprog1",
#        keywords={},
#        protocols={"trajectory": "initial_and_final"},
#        singlepoint_specification=OptimizationSinglepointInputSpecification(
#            program="prog1",
#            method="b3lyp",
#            basis="6-31G*",
#            keywords=KeywordSet(values={"k": "value"}),
#            protocols=SinglepointProtocols(wavefunction="all"),
#        ),
#    )
#
#    water = load_molecule_data("water_dimer_minima")
#    meta, id1 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec.program,
#        keywords=spec.keywords,
#        protocols=spec.protocols,
#        singlepoint_specification=spec.singlepoint_specification,
#    )
#    assert meta.n_inserted == 1
#    assert meta.inserted_idx == [0]
#
#    meta, id2 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec.program,
#        keywords=spec.keywords,
#        protocols=spec.protocols,
#        singlepoint_specification=spec.singlepoint_specification,
#    )
#    assert meta.n_inserted == 0
#    assert meta.n_existing == 1
#    assert meta.existing_idx == [0]
#    assert id1 == id2
#
#
# def test_torsiondrive_client_add_same_2(snowflake_client: PortalClient):
#    # Test case sensitivity
#    spec1 = OptimizationInputSpecification(
#        program="optprog1",
#        keywords={},
#        protocols={"trajectory": "initial_and_final"},
#        singlepoint_specification=OptimizationSinglepointInputSpecification(
#            program="prog1",
#            method="b3lyp",
#            basis="6-31G*",
#            keywords=KeywordSet(values={"k": "value"}),
#            protocols=SinglepointProtocols(wavefunction="all"),
#        ),
#    )
#
#    spec2 = OptimizationInputSpecification(
#        program="opTPROg1",
#        keywords={},
#        protocols={"trajectory": "initial_and_final"},
#        singlepoint_specification=OptimizationSinglepointInputSpecification(
#            program="prOG1",
#            method="b3LYp",
#            basis="6-31g*",
#            keywords=KeywordSet(values={"k": "value"}),
#            protocols=SinglepointProtocols(wavefunction="all"),
#        ),
#    )
#
#    water = load_molecule_data("water_dimer_minima")
#    meta, id1 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec1.program,
#        keywords=spec1.keywords,
#        protocols=spec1.protocols,
#        singlepoint_specification=spec1.singlepoint_specification,
#    )
#    assert meta.n_inserted == 1
#    assert meta.inserted_idx == [0]
#
#    meta, id2 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec2.program,
#        keywords=spec2.keywords,
#        protocols=spec2.protocols,
#        singlepoint_specification=spec2.singlepoint_specification,
#    )
#    assert meta.n_inserted == 0
#    assert meta.n_existing == 1
#    assert meta.existing_idx == [0]
#    assert id1 == id2
#
#
# def test_torsiondrive_client_add_same_3(snowflake_client: PortalClient):
#    # Test default keywords and protocols
#    spec1 = OptimizationInputSpecification(
#        program="optprog1",
#        keywords={},
#        protocols={},
#        singlepoint_specification=OptimizationSinglepointInputSpecification(
#            program="prog1",
#            method="b3lyp",
#            basis="6-31G*",
#            keywords=KeywordSet(values={"k": "value"}),
#            protocols=SinglepointProtocols(wavefunction="all"),
#        ),
#    )
#
#    spec2 = OptimizationInputSpecification(
#        program="optprog1",
#        singlepoint_specification=OptimizationSinglepointInputSpecification(
#            program="prog1",
#            method="b3lyp",
#            basis="6-31G*",
#            keywords=KeywordSet(values={"k": "value"}),
#            protocols=SinglepointProtocols(wavefunction="all"),
#        ),
#    )
#
#    water = load_molecule_data("water_dimer_minima")
#
#    meta, id1 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec1.program,
#        keywords=spec1.keywords,
#        protocols=spec1.protocols,
#        singlepoint_specification=spec1.singlepoint_specification,
#    )
#    assert meta.n_inserted == 1
#    assert meta.inserted_idx == [0]
#
#    meta, id2 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec2.program,
#        keywords=spec2.keywords,
#        protocols=spec2.protocols,
#        singlepoint_specification=spec2.singlepoint_specification,
#    )
#
#    assert meta.n_inserted == 0
#    assert meta.n_existing == 1
#    assert meta.existing_idx == [0]
#    assert id1 == id2
#
#
# def test_torsiondrive_client_add_same_4(snowflake_client: PortalClient):
#    # Test adding molecule by id
#
#    water = load_molecule_data("water_dimer_minima")
#    kw = KeywordSet(values={"a": "value"})
#    _, kw_ids = snowflake_client.add_keywords([kw])
#    _, mol_ids = snowflake_client.add_molecules([water])
#
#    spec1 = OptimizationInputSpecification(
#        program="optprog1",
#        singlepoint_specification=OptimizationSinglepointInputSpecification(
#            program="prog1",
#            method="b3lyp",
#            basis="6-31G*",
#        ),
#    )
#
#    meta, id1 = snowflake_client.add_torsiondrives(
#        initial_molecules=[water],
#        program=spec1.program,
#        keywords=spec1.keywords,
#        protocols=spec1.protocols,
#        singlepoint_specification=spec1.singlepoint_specification,
#    )
#    assert meta.n_inserted == 1
#    assert meta.inserted_idx == [0]
#
#    meta, id2 = snowflake_client.add_torsiondrives(
#        initial_molecules=mol_ids,
#        program=spec1.program,
#        keywords=spec1.keywords,
#        protocols=spec1.protocols,
#        singlepoint_specification=spec1.singlepoint_specification,
#    )
#    assert meta.n_inserted == 0
#    assert meta.n_existing == 1
#    assert meta.existing_idx == [0]
#    assert id1 == id2
#
#
# def test_torsiondrive_client_query(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket):
#    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
#    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
#    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_opt_sometraj")
#
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#    meta2, id2 = storage_socket.records.optimization.add(input_spec_2, [molecule_2])
#    meta3, id3 = storage_socket.records.optimization.add(input_spec_3, [molecule_3])
#
#    recs = snowflake_client.get_optimizations(id1 + id2 + id3)
#
#    # query for molecule
#    meta, opt = snowflake_client.query_optimizations(initial_molecule_id=[recs[1].raw_data.initial_molecule_id])
#    assert meta.n_found == 1
#    assert opt[0].raw_data.id == id2[0]
#
#    # query for program
#    meta, opt = snowflake_client.query_optimizations(program=["psi4"])
#    assert meta.n_found == 0
#
#    # query for program
#    meta, opt = snowflake_client.query_optimizations(program=["geometric"])
#    assert meta.n_found == 3
#
#    meta, opt = snowflake_client.query_optimizations(singlepoint_program=["psi4"])
#    assert meta.n_found == 3
#
#    # query for basis
#    meta, opt = snowflake_client.query_optimizations(singlepoint_basis=["sTO-3g"])
#    assert meta.n_found == 0
#
#    meta, opt = snowflake_client.query_optimizations(singlepoint_basis=[None])
#    assert meta.n_found == 0
#
#    meta, opt = snowflake_client.query_optimizations(singlepoint_basis=[""])
#    assert meta.n_found == 0
#
#    # query for method
#    meta, opt = snowflake_client.query_optimizations(singlepoint_method=["b3lyP"])
#    assert meta.n_found == 3
#
#    # keyword id
#    meta, opt = snowflake_client.query_optimizations(
#        singlepoint_keywords_id=[recs[0].raw_data.specification.singlepoint_specification.keywords_id]
#    )
#    assert meta.n_found == 2
#
#    # Some empty queries
#    meta, opt = snowflake_client.query_optimizations(program=["madeupprog"])
#    assert meta.n_found == 0
#
#    # Query by default returns everything
#    meta, opt = snowflake_client.query_optimizations()
#    assert meta.n_found == 3
#
#    # Query by default (with a limit)
#    meta, opt = snowflake_client.query_optimizations(limit=1)
#    assert meta.n_found == 3
#    assert meta.n_returned == 1
#
#
# @pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
# def test_torsiondrive_client_delete_1(snowflake_client: PortalClient, storage_socket: SQLAlchemySocket, opt_file: str):
#    # Deleting with deleting children
#    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#
#    with storage_socket.session_scope() as session:
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
#        storage_socket.records.update_completed_task(session, rec_orm, result_data_1, None)
#
#    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
#    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]
#
#    meta = snowflake_client.delete_records(id1, soft_delete=True, delete_children=True)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == len(child_ids)
#
#    child_recs = storage_socket.records.get(child_ids)
#    assert all(x["status"] == RecordStatusEnum.deleted for x in child_recs)
#
#    meta = snowflake_client.delete_records(id1, soft_delete=False, delete_children=True)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == len(child_ids)
#
#    recs = storage_socket.records.get(id1, missing_ok=True)
#    assert recs == [None]
#
#    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
#    assert all(x is None for x in child_recs)
