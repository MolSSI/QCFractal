from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.records.optimization.testing_helpers import test_specs, load_test_data
from qcfractal.db_socket import SQLAlchemySocket
from qcfractaltesting import load_molecule_data
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.optimization import (
    OptimizationSpecification,
)
from qcportal.records.singlepoint import (
    QCSpecification,
    SinglepointDriver,
    SinglepointProtocols,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


@pytest.mark.parametrize("spec", test_specs)
def test_optimization_socket_task_spec(storage_socket: SQLAlchemySocket, spec: OptimizationSpecification):
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
        username="bill",
        programs={
            "qcengine": None,
            "qcengine": None,
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

        assert task_spec["input_specification"]["keywords"] == spec.qc_specification.keywords

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


def test_optimization_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = OptimizationSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        qc_specification=QCSpecification(
            program="prog1",
            driver="deferred",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
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
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        qc_specification=QCSpecification(
            program="prog1",
            driver="deferred",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = OptimizationSpecification(
        program="opTPROg1",
        keywords={},
        protocols={"trajectory": "initial_and_final"},
        qc_specification=QCSpecification(
            program="prOG1",
            driver="deferred",
            method="b3LYp",
            basis="6-31g*",
            keywords={"k": "value"},
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
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={},
        protocols={},
        qc_specification=QCSpecification(
            program="prog1",
            driver="deferred",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = OptimizationSpecification(
        program="optprog1",
        qc_specification=QCSpecification(
            program="prog1",
            driver="deferred",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
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
    kw = {"a": "value"}
    _, mol_ids = storage_socket.molecules.add([water])

    spec1 = OptimizationSpecification(
        program="optprog1",
        qc_specification=QCSpecification(
            program="prog1",
            driver="deferred",
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
    input_spec_1, molecule_1, result_data_1 = load_test_data("opt_psi4_fluoroethane_notraj")
    input_spec_2, molecule_2, result_data_2 = load_test_data("opt_psi4_benzene")
    input_spec_3, molecule_3, result_data_3 = load_test_data("opt_psi4_methane_sometraj")

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
        username="bill",
        programs={
            "qcengine": None,
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
        assert record["specification"]["qc_specification"]["keywords"] == result.input_specification.keywords
        assert record["created_on"] < time_0
        assert time_0 < record["modified_on"] < time_1

        assert len(record["compute_history"]) == 1
        assert record["compute_history"][0]["status"] == RecordStatusEnum.complete
        assert time_0 < record["compute_history"][0]["modified_on"] < time_1
        assert record["compute_history"][0]["provenance"] == result.provenance

        outs = record["compute_history"][0]["outputs"]

        avail_outputs = set(outs.keys())
        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
        assert avail_outputs == result_outputs

        # NOTE - this only works for string outputs (not dicts)
        # but those are used for errors, which aren't covered here
        for o in outs.values():
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
