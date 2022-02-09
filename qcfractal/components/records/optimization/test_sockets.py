"""
Tests the singlepoint record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.keywords import KeywordSet
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.optimization import (
    OptimizationSpecification,
    OptimizationInputSpecification,
    OptimizationQueryBody,
    OptimizationQCInputSpecification,
)
from qcportal.records.singlepoint import (
    SinglepointDriver,
    SinglepointProtocols,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Dict, Any, Union


def compare_optimization_specs(
    input_spec: Union[OptimizationInputSpecification, Dict[str, Any]],
    full_spec: Union[OptimizationSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = OptimizationInputSpecification(**input_spec)
    if isinstance(full_spec, OptimizationSpecification):
        full_spec = full_spec.dict()

    full_spec.pop("id")
    full_spec.pop("qc_specification_id")
    full_spec["qc_specification"].pop("id")
    full_spec["qc_specification"].pop("keywords_id")
    full_spec["qc_specification"]["keywords"].pop("id")
    trimmed_spec = OptimizationInputSpecification(**full_spec)
    return input_spec == trimmed_spec


_test_specs = [
    OptimizationInputSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        qc_specification=OptimizationQCInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    ),
    OptimizationInputSpecification(
        program="optprog2",
        keywords={"k": "v"},
        protocols={"trajectory": "none"},
        qc_specification=OptimizationQCInputSpecification(
            program="Prog2",
            method="Hf",
            basis="def2-TZVP",
            keywords=KeywordSet(values={"k": "v"}),
        ),
    ),
    OptimizationInputSpecification(
        program="optPRog3",
        keywords={"k2": "v2"},
        qc_specification=OptimizationQCInputSpecification(
            program="Prog3",
            method="pbe0",
            basis="",
            keywords=KeywordSet(values={"o": 1, "v": 2.123}),
            protocols=SinglepointProtocols(stdout=False, wavefunction="orbitals_and_eigenvalues"),
        ),
    ),
    OptimizationInputSpecification(
        program="OPTPROG4",
        qc_specification=OptimizationQCInputSpecification(
            program="ProG4",
            method="pbe",
            basis=None,
            protocols=SinglepointProtocols(stdout=False, wavefunction="return_results"),
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
    meta, id = storage_socket.records.optimization.add(all_mols, spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.optimization.get(id, include=["*", "task", "initial_molecule"])

    assert len(recs) == 3
    for r in recs:
        assert r["record_type"] == "optimization"
        assert compare_optimization_specs(spec, r["specification"])

        assert r["task"]["tag"] == "tag1"
        assert r["task"]["priority"] == PriorityEnum.low
        assert spec.program in r["task"]["required_programs"]
        assert spec.qc_specification.program in r["task"]["required_programs"]

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["task"]["created_on"] < time_1

    mol1 = storage_socket.molecules.get([recs[0]["initial_molecule_id"]])[0]
    mol2 = storage_socket.molecules.get([recs[1]["initial_molecule_id"]])[0]
    mol3 = storage_socket.molecules.get([recs[2]["initial_molecule_id"]])[0]
    assert mol1["identifiers"]["molecule_hash"] == water.get_hash()
    assert recs[0]["initial_molecule"]["identifiers"]["molecule_hash"] == water.get_hash()

    assert mol2["identifiers"]["molecule_hash"] == hooh.get_hash()
    assert recs[1]["initial_molecule"]["identifiers"]["molecule_hash"] == hooh.get_hash()

    assert mol3["identifiers"]["molecule_hash"] == ne4.get_hash()
    assert recs[2]["initial_molecule"]["identifiers"]["molecule_hash"] == ne4.get_hash()


@pytest.mark.parametrize("spec", _test_specs)
def test_optimization_socket_task_spec(storage_socket: SQLAlchemySocket, spec: OptimizationInputSpecification):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.optimization.add(all_mols, spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={
            "optprog1": None,
            "optprog2": None,
            "optprog3": None,
            "optprog4": None,
            "prog1": None,
            "prog2": "v3.0",
            "prog3": None,
            "prog4": None,
        },
        tags=["*"],
    )
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    assert len(tasks) == 3
    for t in tasks:
        task_spec = t["spec"]["args"][0]
        assert t["spec"]["args"][1] == spec.program

        kw_with_prog = spec.keywords.copy()
        kw_with_prog["program"] = spec.qc_specification.program

        assert task_spec["keywords"] == kw_with_prog
        assert task_spec["protocols"] == spec.protocols.dict(exclude_defaults=True)

        # Forced to gradient in the qcschema input
        assert task_spec["input_specification"]["driver"] == SinglepointDriver.gradient
        assert task_spec["input_specification"]["model"] == {
            "method": spec.qc_specification.method,
            "basis": spec.qc_specification.basis,
        }

        assert task_spec["input_specification"]["keywords"] == spec.qc_specification.keywords.values

        assert t["tag"] == "tag1"
        assert t["priority"] == PriorityEnum.low

        assert time_0 < t["created_on"] < time_1

    rec_id_mol_map = {
        id[0]: all_mols[0],
        id[1]: all_mols[1],
        id[2]: all_mols[2],
    }

    assert Molecule(**tasks[0]["spec"]["args"][0]["initial_molecule"]) == rec_id_mol_map[tasks[0]["record_id"]]
    assert Molecule(**tasks[1]["spec"]["args"][0]["initial_molecule"]) == rec_id_mol_map[tasks[1]["record_id"]]
    assert Molecule(**tasks[2]["spec"]["args"][0]["initial_molecule"]) == rec_id_mol_map[tasks[2]["record_id"]]


def test_optimization_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([ne4])

    # Now add records
    meta, id = storage_socket.records.optimization.add(all_mols, spec, tag="*", priority=PriorityEnum.normal)
    recs = storage_socket.records.optimization.get(id)

    assert len(recs) == 3
    assert recs[2]["initial_molecule_id"] == mol_ids[0]


def test_optimization_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = OptimizationInputSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        qc_specification=OptimizationQCInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.optimization.add([water], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add([water], spec, tag="*", priority=PriorityEnum.normal)
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
        qc_specification=OptimizationQCInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = OptimizationInputSpecification(
        program="opTPROg1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        qc_specification=OptimizationQCInputSpecification(
            program="prOG1",
            method="b3LYp",
            basis="6-31g*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.optimization.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add([water], spec2, tag="*", priority=PriorityEnum.normal)
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
        qc_specification=OptimizationQCInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = OptimizationInputSpecification(
        program="optprog1",
        qc_specification=OptimizationQCInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
            keywords=KeywordSet(values={"k": "value"}),
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.optimization.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add([water], spec2, tag="*", priority=PriorityEnum.normal)
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
        qc_specification=OptimizationQCInputSpecification(
            program="prog1",
            method="b3lyp",
            basis="6-31G*",
        ),
    )

    meta, id1 = storage_socket.records.optimization.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add(mol_ids, spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_run(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_opt_sometraj")

    meta1, id1 = storage_socket.records.optimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta2, id2 = storage_socket.records.optimization.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta3, id3 = storage_socket.records.optimization.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )

    result_map = {id1[0]: result_data_1, id2[0]: result_data_2, id3[0]: result_data_3}

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

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=100)
    assert len(tasks) == 3

    time_0 = datetime.utcnow()
    rmeta = storage_socket.tasks.update_finished(
        mname1.fullname,
        {
            tasks[0]["id"]: result_map[tasks[0]["record_id"]],
            tasks[1]["id"]: result_map[tasks[1]["record_id"]],
            tasks[2]["id"]: result_map[tasks[2]["record_id"]],
        },
    )
    time_1 = datetime.utcnow()
    assert rmeta.n_accepted == 3

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
        assert record["specification"]["qc_specification"]["program"] == result.keywords["program"]
        assert record["specification"]["qc_specification"]["method"] == result.input_specification.model.method
        assert record["specification"]["qc_specification"]["basis"] == result.input_specification.model.basis
        assert record["specification"]["qc_specification"]["keywords"]["values"] == result.input_specification.keywords
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
            assert out_obj.as_string == ro

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
#        rec_orm = session.query(SinglepointRecordORM).where(SinglepointRecordORM.id == id2[0]).one()
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
#    assert outs1.as_string == outs2.as_string


def test_optimization_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_fluoroethane_opt_notraj")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_opt_sometraj")

    meta1, id1 = storage_socket.records.optimization.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta2, id2 = storage_socket.records.optimization.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta3, id3 = storage_socket.records.optimization.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )

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

    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(qc_program=["psi4"]))
    assert meta.n_found == 3

    # query for basis
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(qc_basis=["sTO-3g"]))
    assert meta.n_found == 0

    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(qc_basis=[None]))
    assert meta.n_found == 0

    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(qc_basis=[""]))
    assert meta.n_found == 0

    # query for method
    meta, opt = storage_socket.records.optimization.query(OptimizationQueryBody(qc_method=["b3lyP"]))
    assert meta.n_found == 3

    # keyword id
    meta, opt = storage_socket.records.optimization.query(
        OptimizationQueryBody(qc_keywords_id=[recs[0]["specification"]["qc_specification"]["keywords_id"]])
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
