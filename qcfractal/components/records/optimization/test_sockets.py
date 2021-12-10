"""
Tests the singlepoint record socket
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
    OptimizationSinglePointInputSpecification,
)
from qcfractal.portal.records.singlepoint import (
    SinglePointDriver,
    SinglePointProtocols,
)
from qcfractal.testing import load_molecule_data, load_procedure_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

_test_specs = [
    OptimizationInputSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglePointProtocols(wavefunction="all"),
        ),
    ),
    OptimizationInputSpecification(
        program="optprog2",
        keywords={"k": "v"},
        protocols={"trajectory": "none"},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="Prog2",
            method="Hf",
            basis="def2-TZVP",
            keywords=KeywordSet(values={"k": "v"}),
        ),
    ),
    OptimizationInputSpecification(
        program="optPRog3",
        keywords={"k2": "v2"},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="Prog3",
            method="pbe0",
            basis="",
            keywords=KeywordSet(values={"o": 1, "v": 2.123}),
            protocols=SinglePointProtocols(stdout=False, wavefunction="orbitals_and_eigenvalues"),
        ),
    ),
    OptimizationInputSpecification(
        program="OPTPROG4",
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="ProG4",
            method="pbe",
            basis=None,
            protocols=SinglePointProtocols(stdout=False, wavefunction="return_results"),
        ),
    ),
]


@pytest.mark.parametrize("spec", _test_specs)
def test_optimization_socket_add_get(storage_socket: SQLAlchemySocket, spec: OptimizationInputSpecification):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.optimization.add(spec, all_mols, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.optimization.get(id, include=["*", "task", "initial_molecule"])

    assert len(recs) == 3
    for r in recs:
        assert r["record_type"] == "optimization"
        assert r["specification"]["program"] == spec.program.lower()
        assert r["specification"]["keywords"] == spec.keywords
        assert r["specification"]["protocols"] == spec.protocols.dict(exclude_defaults=True)

        # Test single point spec
        sp_spec = r["specification"]["singlepoint_specification"]
        assert sp_spec["driver"] == spec.singlepoint_specification.driver
        assert sp_spec["driver"] == SinglePointDriver.deferred
        assert sp_spec["method"] == spec.singlepoint_specification.method.lower()
        assert sp_spec["basis"] == (
            spec.singlepoint_specification.basis.lower() if spec.singlepoint_specification.basis is not None else ""
        )
        assert sp_spec["keywords"]["hash_index"] == spec.singlepoint_specification.keywords.hash_index
        assert sp_spec["protocols"] == spec.singlepoint_specification.protocols.dict(exclude_defaults=True)

        # Now the task stuff
        task_spec = r["task"]["spec"]["args"][0]
        assert r["task"]["spec"]["args"][1] == spec.program

        kw_with_prog = spec.keywords.copy()
        kw_with_prog["program"] = spec.singlepoint_specification.program

        assert task_spec["keywords"] == kw_with_prog
        assert task_spec["protocols"] == spec.protocols.dict(exclude_defaults=True)

        # Forced to gradient int he qcschema input
        assert task_spec["input_specification"]["driver"] == SinglePointDriver.gradient
        assert task_spec["input_specification"]["model"] == {
            "method": spec.singlepoint_specification.method,
            "basis": spec.singlepoint_specification.basis,
        }

        assert task_spec["input_specification"]["keywords"] == spec.singlepoint_specification.keywords.values

        assert r["task"]["tag"] == "tag1"
        assert r["task"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["task"]["created_on"] < time_1

    mol1 = storage_socket.molecules.get([recs[0]["initial_molecule_id"]])[0]
    mol2 = storage_socket.molecules.get([recs[1]["initial_molecule_id"]])[0]
    mol3 = storage_socket.molecules.get([recs[2]["initial_molecule_id"]])[0]
    assert mol1["identifiers"]["molecule_hash"] == water.get_hash()
    assert recs[0]["initial_molecule"]["identifiers"]["molecule_hash"] == water.get_hash()
    assert Molecule(**recs[0]["task"]["spec"]["args"][0]["initial_molecule"]) == water

    assert mol2["identifiers"]["molecule_hash"] == hooh.get_hash()
    assert recs[1]["initial_molecule"]["identifiers"]["molecule_hash"] == hooh.get_hash()
    assert Molecule(**recs[1]["task"]["spec"]["args"][0]["initial_molecule"]) == hooh

    assert mol3["identifiers"]["molecule_hash"] == ne4.get_hash()
    assert Molecule(**recs[2]["task"]["spec"]["args"][0]["initial_molecule"]) == ne4
    assert recs[2]["initial_molecule"]["identifiers"]["molecule_hash"] == ne4.get_hash()


def test_optimization_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([ne4])

    # Now add records
    meta, id = storage_socket.records.optimization.add(spec, all_mols)
    recs = storage_socket.records.optimization.get(id)

    assert len(recs) == 3
    assert recs[2]["initial_molecule_id"] == mol_ids[0]


def test_optimization_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = OptimizationInputSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglePointProtocols(wavefunction="all"),
        ),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.optimization.add(spec, [water])
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add(spec, [water])
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec1 = OptimizationInputSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglePointProtocols(wavefunction="all"),
        ),
    )

    spec2 = OptimizationInputSpecification(
        program="opTPROg1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prOG1",
            method="b3LYp",
            basis="6-31g*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglePointProtocols(wavefunction="all"),
        ),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.optimization.add(spec1, [water])
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add(spec2, [water])
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_add_same_3(storage_socket: SQLAlchemySocket):
    # Test default keywords and protocols
    spec1 = OptimizationInputSpecification(
        program="optprog1",
        keywords={},
        protocols={},
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglePointProtocols(wavefunction="all"),
        ),
    )

    spec2 = OptimizationInputSpecification(
        program="optprog1",
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglePointProtocols(wavefunction="all"),
        ),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.optimization.add(spec1, [water])
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add(spec2, [water])
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_add_same_4(storage_socket: SQLAlchemySocket):
    # Test adding molecule by id

    water = load_molecule_data("water_dimer_minima")
    kw = KeywordSet(values={"a": "value"})
    _, kw_ids = storage_socket.keywords.add([kw])
    _, mol_ids = storage_socket.molecules.add([water])

    spec1 = OptimizationInputSpecification(
        program="optprog1",
        singlepoint_specification=OptimizationSinglePointInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
        ),
    )

    meta, id1 = storage_socket.records.optimization.add(spec1, [water])
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add(spec1, mol_ids)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_update(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_opt_sometraj")

    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
    meta2, id2 = storage_socket.records.optimization.add(input_spec_2, [molecule_2])
    meta3, id3 = storage_socket.records.optimization.add(input_spec_3, [molecule_3])

    time_0 = datetime.utcnow()

    with storage_socket.session_scope() as session:
        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)

        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id2[0]).one()
        storage_socket.records.update_completed(session, rec_orm, result_data_2, None)

        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id3[0]).one()
        storage_socket.records.update_completed(session, rec_orm, result_data_3, None)

    time_1 = datetime.utcnow()

    all_results = [result_data_1, result_data_2, result_data_3]
    recs = storage_socket.records.optimization.get(
        id1 + id2 + id3,
        include=[
            "*",
            "compute_history.*",
            "compute_history.outputs",
            "trajectory.*",
            "trajectory.singlepoint_record.*",
            "trajectory.singlepoint_record.molecule",
        ],
    )

    for record, result in zip(recs, all_results):
        assert record["status"] == RecordStatusEnum.complete
        assert record["specification"]["program"] == result.provenance.creator.lower()

        kw_no_prog = result.keywords.copy()
        kw_no_prog["program"] = result.keywords["program"]
        assert kw_no_prog == result.keywords

        # The singlepoint spec
        assert record["specification"]["singlepoint_specification"]["program"] == result.keywords["program"]
        assert record["specification"]["singlepoint_specification"]["method"] == result.input_specification.model.method
        assert record["specification"]["singlepoint_specification"]["basis"] == result.input_specification.model.basis
        assert (
            record["specification"]["singlepoint_specification"]["keywords"]["values"]
            == result.input_specification.keywords
        )
        assert record["created_on"] < time_0
        assert time_0 < record["modified_on"] < time_1

        assert len(record["compute_history"]) == 1
        assert record["compute_history"][0]["status"] == RecordStatusEnum.complete
        assert time_0 < record["compute_history"][0]["modified_on"] < time_1
        assert record["compute_history"][0]["provenance"] == result.provenance

        outs = record["compute_history"][0]["outputs"]

        avail_outputs = {x["output_type"] for x in outs}
        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
        assert avail_outputs == result_outputs

        # NOTE - this only works for string outputs (not dicts)
        # but those are used for errors, which aren't covered here
        for o in outs:
            out_obj = OutputStore(**o)
            ro = getattr(result, o["output_type"])
            assert out_obj.get_string() == ro

        # Test the trajectory
        assert len(record["trajectory"]) == len(result.trajectory)
        for db_traj, res_traj in zip(record["trajectory"], result.trajectory):
            assert db_traj["singlepoint_record"]["specification"]["program"] == res_traj.provenance.creator.lower()
            assert db_traj["singlepoint_record"]["specification"]["basis"] == res_traj.model.basis
            assert (
                db_traj["singlepoint_record"]["molecule"]["identifiers"]["molecule_hash"]
                == res_traj.molecule.get_hash()
            )


#
#
# def test_optimization_socket_insert(storage_socket: SQLAlchemySocket):
#    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
#
#    meta2, id2 = storage_socket.records.optimization.add(input_spec_2, [molecule_2])
#
#    # Typical workflow
#    with storage_socket.session_scope() as session:
#        rec_orm = session.query(ResultORM).where(ResultORM.id == id2[0]).one()
#        storage_socket.records.update_completed(session, rec_orm, result_data_2, None)
#
#    # Actually insert the whole thing. This should end up being a duplicate
#    with storage_socket.session_scope() as session:
#        dup_id = storage_socket.records.insert_completed([result_data_2])
#
#    recs = storage_socket.records.optimization.get(
#        id2 + dup_id, include=["*", "wavefunction", "compute_history.*", "compute_history.outputs"]
#    )
#
#    assert recs[0]["id"] != recs[1]["id"]
#    assert recs[0]["status"] == RecordStatusEnum.complete == recs[1]["status"] == RecordStatusEnum.complete
#    assert recs[0]["specification"]["program"] == recs[1]["specification"]["program"]
#    assert recs[0]["specification"]["driver"] == recs[1]["specification"]["driver"]
#    assert recs[0]["specification"]["method"] == recs[1]["specification"]["method"]
#    assert recs[0]["specification"]["basis"] == recs[1]["specification"]["basis"]
#    assert recs[0]["specification"]["keywords"] == recs[1]["specification"]["keywords"]
#    assert recs[0]["specification"]["protocols"] == recs[1]["specification"]["protocols"]
#
#    assert len(recs[0]["compute_history"]) == 1
#    assert len(recs[1]["compute_history"]) == 1
#    assert recs[0]["compute_history"][0]["status"] == RecordStatusEnum.complete
#    assert recs[1]["compute_history"][0]["status"] == RecordStatusEnum.complete
#
#    assert recs[0]["compute_history"][0]["provenance"] == recs[1]["compute_history"][0]["provenance"]
#
#    assert recs[0]["return_result"] == recs[1]["return_result"]
#    arprop1 = AtomicResultProperties(**recs[0]["properties"])
#    arprop2 = AtomicResultProperties(**recs[1]["properties"])
#    assert arprop1.nuclear_repulsion_energy == arprop2.nuclear_repulsion_energy
#    assert arprop1.return_energy == arprop2.return_energy
#    assert arprop1.scf_iterations == arprop2.scf_iterations
#    assert arprop1.scf_total_energy == arprop2.scf_total_energy
#
#    wfn_model_1 = WavefunctionProperties(**recs[0]["wavefunction"])
#    wfn_model_2 = WavefunctionProperties(**recs[1]["wavefunction"])
#    assert_wfn_equal(wfn_model_1, wfn_model_2)
#
#    assert len(recs[0]["compute_history"][0]["outputs"]) == 1
#    assert len(recs[1]["compute_history"][0]["outputs"]) == 1
#    outs1 = OutputStore(**recs[0]["compute_history"][0]["outputs"][0])
#    outs2 = OutputStore(**recs[1]["compute_history"][0]["outputs"][0])
#    assert outs1.get_string() == outs2.get_string()


def test_optimization_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_opt_sometraj")

    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])
    meta2, id2 = storage_socket.records.optimization.add(input_spec_2, [molecule_2])
    meta3, id3 = storage_socket.records.optimization.add(input_spec_3, [molecule_3])

    recs = storage_socket.records.optimization.get(id1 + id2 + id3)

    # query for molecule
    meta, opt = storage_socket.records.optimization.query(
        OptimizationQueryBody(initial_molecule_id=[recs[1]["initial_molecule_id"]])
    )
    assert meta.n_found == 1
    assert opt[0]["id"] == id2[0]

    # query for program
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(program=["psi4"]))
    assert meta.n_found == 0

    # query for program
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(program=["geometric"]))
    assert meta.n_found == 3

    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_program=["psi4"]))
    assert meta.n_found == 3

    # query for basis
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_basis=["sTO-3g"]))
    assert meta.n_found == 0

    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_basis=[None]))
    assert meta.n_found == 0

    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_basis=[""]))
    assert meta.n_found == 0

    # query for method
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(singlepoint_method=["b3lyP"]))
    assert meta.n_found == 3

    # keyword id
    meta, opt = storage_socket.records.optimization.query(
        OptimizationQueryBody(
            singlepoint_keywords_id=[recs[0]["specification"]["singlepoint_specification"]["keywords_id"]]
        )
    )
    assert meta.n_found == 2

    # Some empty queries
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(program=["madeupprog"]))
    assert meta.n_found == 0

    # Query by default returns everything
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody())
    assert meta.n_found == 3

    # Query by default (with a limit)
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(limit=1))
    assert meta.n_found == 3
    assert meta.n_returned == 1


def test_optimization_socket_recreate_task(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])

    recs = storage_socket.records.optimization.get(id1, include=["task"])
    orig_task = recs[0]["task"]
    assert orig_task is not None

    # cancel, the verify the task is gone
    m = storage_socket.records.cancel(id1)
    assert m.n_updated == 1

    recs = storage_socket.records.optimization.get(id1, include=["task"])
    assert recs[0]["task"] is None

    # reset, and see that the task was recreated (and is the same)
    m = storage_socket.records.reset(id1)
    assert m.n_updated == 1

    recs = storage_socket.records.optimization.get(id1, include=["task"])
    new_task = recs[0]["task"]
    assert new_task is not None

    assert orig_task["required_programs"] == new_task["required_programs"]
    assert orig_task["spec"]["args"][1] == new_task["spec"]["args"][1]
    assert orig_task["spec"]["args"][0]["initial_molecule"]["identifiers"]["molecule_hash"] == molecule_1.get_hash()
    assert orig_task["spec"]["args"][0]["input_specification"] == new_task["spec"]["args"][0]["input_specification"]
    assert orig_task["spec"]["args"][0]["keywords"] == new_task["spec"]["args"][0]["keywords"]
    assert orig_task["spec"]["args"][0]["protocols"] == new_task["spec"]["args"][0]["protocols"]


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
def test_optimization_socket_delete_1(storage_socket: SQLAlchemySocket, opt_file: str):
    # Deleteing with deleting children
    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])

    with storage_socket.session_scope() as session:
        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)

    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
    child_ids = [x["singlepoint_record_id"] for x in rec[0]["trajectory"]]

    meta = storage_socket.records.delete(id1, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    child_recs = storage_socket.records.get(child_ids)
    assert all(x["status"] == RecordStatusEnum.deleted for x in child_recs)

    meta = storage_socket.records.delete(id1, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == len(child_ids)

    recs = storage_socket.records.get(id1, missing_ok=True)
    assert recs == [None]

    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
    assert all(x is None for x in child_recs)


@pytest.mark.parametrize("opt_file", ["psi4_benzene_opt", "psi4_fluoroethane_opt_notraj"])
def test_optimization_socket_delete_2(storage_socket: SQLAlchemySocket, opt_file: str):
    # Deleteing without deleting children
    input_spec_1, molecule_1, result_data_1 = load_procedure_data(opt_file)
    meta1, id1 = storage_socket.records.optimization.add(input_spec_1, [molecule_1])

    with storage_socket.session_scope() as session:
        rec_orm = session.query(OptimizationRecordORM).where(OptimizationRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed(session, rec_orm, result_data_1, None)

    rec = storage_socket.records.optimization.get(id1, include=["trajectory"])
    child_ids = [x["singlepoint_record_id"] for x in rec[0]["trajectory"]]

    meta = storage_socket.records.delete(id1, soft_delete=True, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    child_recs = storage_socket.records.get(child_ids)
    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)

    meta = storage_socket.records.delete(id1, soft_delete=False, delete_children=False)
    assert meta.success
    assert meta.deleted_idx == [0]
    assert meta.n_children_deleted == 0

    recs = storage_socket.records.get(id1, missing_ok=True)
    assert recs == [None]

    child_recs = storage_socket.records.get(child_ids, missing_ok=True)
    assert all(x["status"] == RecordStatusEnum.complete for x in child_recs)
