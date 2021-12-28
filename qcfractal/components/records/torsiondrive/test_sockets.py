"""
Tests the torsiondrive record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import json
import pytest

from qcfractal.components.records.optimization.db_models import OptimizationRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal.keywords import KeywordSet
from qcfractal.portal.molecules import Molecule
from qcfractal.portal.outputstore import OutputStore
from qcfractal.portal.managers import ManagerName
from qcfractal.portal.records import RecordStatusEnum, PriorityEnum
from qcfractal.portal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQueryBody,
    OptimizationSinglepointInputSpecification,
    OptimizationProtocols,
)
from qcfractal.portal.records.singlepoint import (
    SinglepointDriver,
    SinglepointProtocols,
)
from qcfractal.portal.records.torsiondrive import (
    TorsiondriveSpecification,
    TorsiondriveInputSpecification,
    TorsiondriveKeywords,
    TorsiondriveQueryBody,
)
from qcfractal.testing import load_molecule_data, load_procedure_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Dict, Any, Union


def compare_torsiondrive_specs(
    input_spec: Union[TorsiondriveInputSpecification, Dict[str, Any]],
    full_spec: Union[TorsiondriveSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = TorsiondriveInputSpecification(**input_spec)
    if isinstance(full_spec, TorsiondriveSpecification):
        full_spec = full_spec.dict()

    full_spec.pop("id")
    full_spec.pop("optimization_specification_id")
    full_spec["optimization_specification"].pop("id")
    full_spec["optimization_specification"].pop("singlepoint_specification_id")
    full_spec["optimization_specification"]["singlepoint_specification"].pop("id")
    full_spec["optimization_specification"]["singlepoint_specification"].pop("keywords_id")
    full_spec["optimization_specification"]["singlepoint_specification"]["keywords"].pop("id")
    trimmed_spec = TorsiondriveInputSpecification(**full_spec)
    return input_spec == trimmed_spec


_test_specs = [
    TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(1, 2, 3, 4)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    ),
    TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(7, 2, 9, 4), (5, 11, 3, 10)],
            grid_spacing=[30, 45],
            dihedral_ranges=[[-90, 90], [0, 180]],
            energy_decrease_thresh=1.0,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all", stdout=False),
            ),
        ),
    ),
]


@pytest.mark.parametrize("spec", _test_specs)
def test_torsiondrive_socket_add_get(storage_socket: SQLAlchemySocket, spec: TorsiondriveInputSpecification):
    hooh = load_molecule_data("peroxide2")
    c8h6_1 = load_molecule_data("td_C8H6_1")
    c8h6_2 = load_molecule_data("td_C8H6_2")

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.torsiondrive.add(
        spec, [[hooh], [c8h6_1, c8h6_2]], as_service=True, tag="tag1", priority=PriorityEnum.low
    )
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.torsiondrive.get(id, include=["*", "initial_molecules", "service"])

    assert len(recs) == 2
    for r in recs:
        assert r["record_type"] == "torsiondrive"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_torsiondrive_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    assert len(recs[0]["initial_molecules"]) == 1
    assert len(recs[1]["initial_molecules"]) == 2

    assert recs[0]["initial_molecules"][0]["identifiers"]["molecule_hash"] == hooh.get_hash()

    # Not necessarily in the input order
    hash1 = recs[1]["initial_molecules"][0]["identifiers"]["molecule_hash"]
    hash2 = recs[1]["initial_molecules"][1]["identifiers"]["molecule_hash"]
    assert {hash1, hash2} == {c8h6_1.get_hash(), c8h6_2.get_hash()}


def test_torsiondrive_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    mol1 = load_molecule_data("td_C8H6_1")
    mol2 = load_molecule_data("td_C8H6_2")

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([mol2])

    # Now add records
    meta, id = storage_socket.records.torsiondrive.add(spec, [[mol1, mol2], [mol2, mol1]], as_service=True)
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1

    recs = storage_socket.records.torsiondrive.get(id, include=["initial_molecules"])
    assert len(recs) == 2
    assert recs[0]["id"] == recs[1]["id"]

    rec_mols = {x["id"] for x in recs[0]["initial_molecules"]}
    _, mol_ids_2 = storage_socket.molecules.add([mol1])
    assert rec_mols == set(mol_ids + mol_ids_2)


def test_torsiondrive_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    hooh = load_molecule_data("peroxide2")
    meta, id1 = storage_socket.records.torsiondrive.add(spec, [[hooh]], as_service=True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(spec, [[hooh]], as_service=True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_torsiondrive_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # multiple molecule ordering, and duplicate molecules
    spec = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(spec, [[mol1, mol2, mol3]], as_service=True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(spec, [[mol2, mol3, mol1, mol2]], as_service=True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_torsiondrive_socket_add_same_3(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec2 = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optPROG1",
            keywords={"k": "value"},
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prOG2",
                method="b3LYP",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all", stdout=True),
            ),
        ),
    )

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(spec1, [[mol1, mol2, mol3]], as_service=True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(spec2, [[mol1, mol2, mol3]], as_service=True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


# TODO - enable when properly handling as_service=False
# def test_torsiondrive_socket_update(storage_socket: SQLAlchemySocket):
#    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
#    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
#    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_opt_sometraj")
#
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#    meta2, id2 = storage_socket.records.optimization.add(input_spec_2, [molecule_2])
#    meta3, id3 = storage_socket.records.optimization.add(input_spec_3, [molecule_3])
#
#    time_0 = datetime.utcnow()
#
#    with storage_socket.session_scope() as session:
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)
#
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id2[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_2, None)
#
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id3[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_3, None)
#
#    time_1 = datetime.utcnow()
#
#    all_results = [result_data_1, result_data_2, result_data_3]
#    recs = storage_socket.records.optimization.get(
#        id1 + id2 + id3,
#        include=[
#            "*",
#            "compute_history.*",
#            "compute_history.outputs",
#            "trajectory.*",
#            "trajectory.singlepoint_record.*",
#            "trajectory.singlepoint_record.molecule",
#        ],
#    )
#
#    for record, result in zip(recs, all_results):
#        assert record["status"] == RecordStatusEnum.complete
#        assert record["specification"]["program"] == result.provenance.creator.lower()
#
#        kw_no_prog = result.keywords.copy()
#        kw_no_prog["program"] = result.keywords["program"]
#        assert kw_no_prog == result.keywords
#
#        # The singlepoint spec
#        assert record["specification"]["singlepoint_specification"]["program"] == result.keywords["program"]
#        assert record["specification"]["singlepoint_specification"]["method"] == result.input_specification.model.method
#        assert record["specification"]["singlepoint_specification"]["basis"] == result.input_specification.model.basis
#        assert (
#            record["specification"]["singlepoint_specification"]["keywords"]["values"]
#            == result.input_specification.keywords
#        )
#        assert record["created_on"] < time_0
#        assert time_0 < record["modified_on"] < time_1
#
#        assert len(record["compute_history"]) == 1
#        assert record["compute_history"][0]["status"] == RecordStatusEnum.complete
#        assert time_0 < record["compute_history"][0]["modified_on"] < time_1
#        assert record["compute_history"][0]["provenance"] == result.provenance
#
#        outs = record["compute_history"][0]["outputs"]
#
#        avail_outputs = {x["output_type"] for x in outs}
#        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
#        assert avail_outputs == result_outputs
#
#        # NOTE - this only works for string outputs (not dicts)
#        # but those are used for errors, which aren't covered here
#        for o in outs:
#            out_obj = OutputStore(**o)
#            ro = getattr(result, o["output_type"])
#            assert out_obj.as_string == ro
#
#        # Test the trajectory
#        assert len(record["trajectory"]) == len(result.trajectory)
#        for db_traj, res_traj in zip(record["trajectory"], result.trajectory):
#            assert db_traj["singlepoint_record"]["specification"]["program"] == res_traj.provenance.creator.lower()
#            assert db_traj["singlepoint_record"]["specification"]["basis"] == res_traj.model.basis
#            assert (
#                db_traj["singlepoint_record"]["molecule"]["identifiers"]["molecule_hash"]
#                == res_traj.molecule.get_hash()
#            )


def test_torsiondrive_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("td_H2O2_psi4_b3lyp")

    meta_1, id_1 = storage_socket.records.torsiondrive.add(input_spec_1, [molecules_1], as_service=True)
    assert meta_1.success

    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(singlepoint_program=["psi4"]))
    assert meta.n_found == 1

    meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(singlepoint_program=["nothing"]))
    assert meta.n_found == 0

    _, init_mol_id = storage_socket.molecules.add(molecules_1)
    meta, td = storage_socket.records.torsiondrive.query(
        TorsiondriveQueryBody(initial_molecule_id=[init_mol_id[0]], include=["initial_molecules"])
    )
    assert meta.n_found == 1
    print(td)

    # _, init_mol_id = storage_socket.molecules.add(molecules_1)
    # meta, td = storage_socket.records.torsiondrive.query(TorsiondriveQueryBody(
    #    initial_molecule_id=[init_mol_id[0]+9999]
    # ))
    # assert meta.n_found == 0
    # assert meta.n_returned == 0
    # assert len(td) == 0


#
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#    meta2, id2 = storage_socket.records.optimization.add(input_spec_2, [molecule_2])
#    meta3, id3 = storage_socket.records.optimization.add(input_spec_3, [molecule_3])
#
#    recs = storage_socket.records.optimization.get(id1 + id2 + id3)
#
#    # query for molecule
#    meta, opt = storage_socket.records.optimization.query(
#        OptimizationQueryBody(initial_molecule_id=[recs[1]["initial_molecule_id"]])
#    )
#    assert meta.n_found == 1
#    assert opt[0]["id"] == id2[0]
#
#    # query for program
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(program=["psi4"]))
#    assert meta.n_found == 0
#
#    # query for program
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(program=["geometric"]))
#    assert meta.n_found == 3
#
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_program=["psi4"]))
#    assert meta.n_found == 3
#
#    # query for basis
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_basis=["sTO-3g"]))
#    assert meta.n_found == 0
#
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_basis=[None]))
#    assert meta.n_found == 0
#
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_basis=[""]))
#    assert meta.n_found == 0
#
#    # query for method
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_method=["b3lyP"]))
#    assert meta.n_found == 3
#
#    # keyword id
#    meta, opt = storage_socket.records.optimization.query(
#        OptimizationQueryBody(
#            singlepoint_keywords_id=[recs[0]["specification"]["singlepoint_specification"]["keywords_id"]]
#        )
#    )
#    assert meta.n_found == 2
#
#    # Some empty queries
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(program=["madeupprog"]))
#    assert meta.n_found == 0
#
#    # Query by default returns everything
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody())
#    assert meta.n_found == 3
#
#    # Query by default (with a limit)
#    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(limit=1))
#    assert meta.n_found == 3
#    assert meta.n_returned == 1


# @pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
# def test_torsiondrive_socket_delete_1(storage_socket: SQLAlchemySocket, opt_file: str):
#    # Deleting with deleting children
#    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#
#    with storage_socket.session_scope() as session:
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)
#
#    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
#    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]
#
#    meta = storage_socket.records.delete(id1, soft_delete=True, delete_children=True)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == len(child_ids)
#
#    child_recs = storage_socket.records.get(child_ids)
#    assert all(x["status"] == RecordStatusEnum.deleted for x in child_recs)
#
#    meta = storage_socket.records.delete(id1, soft_delete=False, delete_children=True)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == len(child_ids)
#
#    recs = storage_socket.records.get(id1, missing_ok=True)
#    assert recs == [None]
#
#    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
#    assert all(x is None for x in child_recs)
#
#
# @pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
# def test_torsiondrive_socket_delete_2(storage_socket: SQLAlchemySocket, opt_file: str):
#    # Deleting without deleting children
#    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#
#    with storage_socket.session_scope() as session:
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)
#
#    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
#    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]
#
#    meta = storage_socket.records.delete(id1, soft_delete=True, delete_children=False)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == 0
#
#    child_recs = storage_socket.records.get(child_ids)
#    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)
#
#    meta = storage_socket.records.delete(id1, soft_delete=False, delete_children=False)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == 0
#
#    recs = storage_socket.records.get(id1, missing_ok=True)
#    assert recs == [None]
#
#    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
#    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)
#
#
# @pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
# def test_torsiondrive_socket_undelete_1(storage_socket: SQLAlchemySocket, opt_file: str):
#    # Deleting with deleting children, then undeleting
#    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
#    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
#
#    with storage_socket.session_scope() as session:
#        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)
#
#    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
#    child_ids = [x["singlepoint_id"] for x in rec[0]["trajectory"]]
#
#    meta = storage_socket.records.delete(id1, soft_delete=True, delete_children=True)
#    assert meta.success
#    assert meta.deleted_idx == [0]
#    assert meta.n_children_deleted == len(child_ids)
#
#    meta = storage_socket.records.undelete(id1)
#    assert meta.success
#    assert meta.undeleted_idx == [0]
#
#    child_recs = storage_socket.records.get(child_ids)
#    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)
#


@pytest.mark.parametrize(
    "test_data_name",
    [
        "td_C8H6_psi4",
        "td_C9H11NO2_psi4",
        "td_H2O2_psi4_b3lyp-d3bj",
        "td_H2O2_psi4_b3lyp",
        "td_H2O2_psi4_blyp",
        "td_H2O2_psi4_bp86",
        "td_H2O2_psi4_hf",
        "td_H2O2_psi4_pbe0-d3bj",
        "td_H2O2_psi4_pbe0",
        "td_H2O2_psi4_pbe",
    ],
)
def test_torsiondrive_socket_run(storage_socket: SQLAlchemySocket, test_data_name: str):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data(test_data_name)

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        input_spec_1, [molecules_1], tag="test_tag", priority=PriorityEnum.low, as_service=True
    )
    assert meta_1.success
    rec = storage_socket.records.torsiondrive.get(id_1)
    assert rec[0]["status"] == RecordStatusEnum.waiting

    # A manager for completing the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={
            "geometric": None,
            "psi4": None,
        },
        tags=["*"],
    )

    time_0 = datetime.utcnow()
    r = storage_socket.services.iterate_services()
    time_1 = datetime.utcnow()

    while r > 0:
        rec = storage_socket.records.torsiondrive.get(
            id_1, include=["*", "service.*", "service.dependencies.*", "service.dependencies.record"]
        )
        assert rec[0]["status"] == RecordStatusEnum.running

        manager_tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=10)

        # Sometimes a task may be duplicated in the service dependencies.
        # The C8H6 test has this "feature"
        opt_ids = set(x["record_id"] for x in manager_tasks)
        opt_recs = storage_socket.records.optimization.get(opt_ids, include=["*", "initial_molecule", "task"])
        assert all(x["task"]["priority"] == PriorityEnum.low for x in opt_recs)
        assert all(x["task"]["tag"] == "test_tag" for x in opt_recs)

        manager_ret = {}
        for opt in opt_recs:
            # Find out info about what tasks the service spawned
            mol_hash = opt["initial_molecule"]["identifiers"]["molecule_hash"]
            constraints = opt["specification"]["keywords"]["constraints"]

            # This is the key in the dictionary of optimization results
            optresult_key = mol_hash + "|" + json.dumps(constraints, sort_keys=True)
            opt_data = result_data_1[optresult_key]
            manager_ret[opt["task"]["id"]] = opt_data

        rmeta = storage_socket.tasks.update_finished(mname1.fullname, manager_ret)
        assert rmeta.n_accepted == len(manager_tasks)

        time_0 = datetime.utcnow()
        # may or may not iterate - depends on if all tasks done
        r = storage_socket.services.iterate_services()
        time_1 = datetime.utcnow()

    rec = storage_socket.records.torsiondrive.get(
        id_1, include=["*", "compute_history.*", "compute_history.outputs", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.complete
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 1
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is None
    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"][0])
    assert "Job Finished" in out.as_string
