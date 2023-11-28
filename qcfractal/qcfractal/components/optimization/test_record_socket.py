from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM
from qcfractal.components.optimization.testing_helpers import test_specs, load_test_data, run_test_data
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.compression import decompress
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.optimization import (
    OptimizationSpecification,
)
from qcportal.record_models import RecordStatusEnum, PriorityEnum, RecordTask
from qcportal.singlepoint import (
    QCSpecification,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session
    from typing import List, Dict


@pytest.mark.parametrize("spec", test_specs)
def test_optimization_socket_task_spec(
    storage_socket: SQLAlchemySocket,
    spec: OptimizationSpecification,
    activated_manager_name: ManagerName,
    activated_manager_programs: Dict[str, List[str]],
):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = now_at_utc()
    meta, id = storage_socket.records.optimization.add(all_mols, spec, "tag1", PriorityEnum.low, None, None, True)
    time_1 = now_at_utc()
    assert meta.success

    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname, activated_manager_programs, ["*"])
    tasks = [RecordTask(**t) for t in tasks]

    assert len(tasks) == 3
    for t in tasks:
        assert t.function == "qcengine.compute_procedure"
        assert t.function_kwargs["procedure"] == spec.program

        kw_with_prog = spec.keywords.copy()
        kw_with_prog["program"] = spec.qc_specification.program

        task_input = t.function_kwargs["input_data"]
        assert task_input["keywords"] == kw_with_prog
        assert task_input["protocols"] == spec.protocols.dict(exclude_defaults=True)

        # Forced to gradient in the qcschema input
        assert task_input["input_specification"]["driver"] == SinglepointDriver.gradient
        assert task_input["input_specification"]["model"] == {
            "method": spec.qc_specification.method,
            "basis": spec.qc_specification.basis,
        }

        assert task_input["input_specification"]["keywords"] == spec.qc_specification.keywords

        assert t.tag == "tag1"
        assert t.priority == PriorityEnum.low

    rec_id_mol_map = {
        id[0]: all_mols[0],
        id[1]: all_mols[1],
        id[2]: all_mols[2],
    }

    assert Molecule(**tasks[0].function_kwargs["input_data"]["initial_molecule"]) == rec_id_mol_map[tasks[0].record_id]
    assert Molecule(**tasks[1].function_kwargs["input_data"]["initial_molecule"]) == rec_id_mol_map[tasks[1].record_id]
    assert Molecule(**tasks[2].function_kwargs["input_data"]["initial_molecule"]) == rec_id_mol_map[tasks[2].record_id]


def test_optimization_socket_find_existing_1(storage_socket: SQLAlchemySocket):
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
    meta, id1 = storage_socket.records.optimization.add([water], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add([water], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_find_existing_2(storage_socket: SQLAlchemySocket):
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
    meta, id1 = storage_socket.records.optimization.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add([water], spec2, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_find_existing_3(storage_socket: SQLAlchemySocket):
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
    meta, id1 = storage_socket.records.optimization.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add([water], spec2, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_find_existing_4(storage_socket: SQLAlchemySocket):
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

    meta, id1 = storage_socket.records.optimization.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.optimization.add(mol_ids, spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_optimization_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName
):
    test_names = [
        "opt_psi4_fluoroethane_notraj",
        "opt_psi4_benzene",
        "opt_psi4_methane_sometraj",
    ]

    all_results = []
    all_id = []

    for test_name in test_names:
        _, _, result_data = load_test_data(test_name)
        record_id = run_test_data(storage_socket, activated_manager_name, test_name)
        all_results.append(result_data)
        all_id.append(record_id)

    for rec_id, result in zip(all_id, all_results):
        record = session.get(OptimizationRecordORM, rec_id)
        assert record.status == RecordStatusEnum.complete
        assert record.specification.program == result.provenance.creator.lower()

        kw_no_prog = result.keywords.copy()
        kw_no_prog["program"] = result.keywords["program"]
        assert kw_no_prog == result.keywords

        # The singlepoint spec
        assert record.specification.qc_specification.program == result.keywords["program"]
        assert record.specification.qc_specification.method == result.input_specification.model.method
        assert record.specification.qc_specification.basis == result.input_specification.model.basis
        assert record.specification.qc_specification.keywords == result.input_specification.keywords

        assert len(record.compute_history) == 1
        assert record.compute_history[0].status == RecordStatusEnum.complete
        assert record.compute_history[0].provenance == result.provenance

        desc_info = storage_socket.records.get_short_descriptions([rec_id])[0]
        short_desc = desc_info["description"]
        assert desc_info["record_type"] == record.record_type
        assert desc_info["created_on"] == record.created_on
        assert record.specification.program in short_desc
        assert record.specification.qc_specification.program in short_desc
        assert record.specification.qc_specification.method in short_desc

        outs = record.compute_history[0].outputs

        avail_outputs = set(outs.keys())
        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
        compressed_outputs = result.extras.get("_qcfractal_compressed_outputs", {})
        result_outputs |= set(compressed_outputs.keys())
        assert avail_outputs == result_outputs

        # NOTE - this only works for string outputs (not dicts)
        # but those are used for errors, which aren't covered here
        for out in outs.values():
            o_str = out.get_output()
            co = result.extras["_qcfractal_compressed_outputs"][out.output_type]
            ro = decompress(co["data"], co["compression_type"])
            assert o_str == ro

        # Test the trajectory
        assert len(record.trajectory) == len(result.trajectory)
        for db_traj, res_traj in zip(record.trajectory, result.trajectory):
            assert db_traj.singlepoint_record.specification.program == res_traj.provenance.creator.lower()
            assert db_traj.singlepoint_record.specification.basis == res_traj.model.basis
            assert db_traj.singlepoint_record.molecule.identifiers["molecule_hash"] == res_traj.molecule.get_hash()
