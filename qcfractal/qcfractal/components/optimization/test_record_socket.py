from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM
from qcfractal.components.optimization.testing_helpers import test_specs, load_test_data, run_test_data
from qcfractal.components.testing_helpers import convert_to_plain_qcschema_result
from qcfractal.db_socket import SQLAlchemySocket
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
from ..record_utils import build_extras_properties

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session
    from typing import List, Dict


def _compare_record_with_schema(record_orm, result_schema):
    assert record_orm.status == RecordStatusEnum.complete
    assert record_orm.specification.program == result_schema.provenance.creator.lower()

    kw_no_prog = result_schema.keywords.copy()
    kw_no_prog["program"] = result_schema.keywords["program"]
    assert kw_no_prog == result_schema.keywords

    # The singlepoint spec
    assert record_orm.specification.qc_specification.program == result_schema.keywords["program"]
    assert record_orm.specification.qc_specification.method == result_schema.input_specification.model.method
    assert record_orm.specification.qc_specification.basis == result_schema.input_specification.model.basis
    assert record_orm.specification.qc_specification.keywords == result_schema.input_specification.keywords

    assert len(record_orm.compute_history) == 1
    assert record_orm.compute_history[0].status == RecordStatusEnum.complete
    assert record_orm.compute_history[0].provenance == result_schema.provenance

    # Test the trajectory
    assert len(record_orm.trajectory) == len(result_schema.trajectory)
    for db_traj, res_traj in zip(record_orm.trajectory, result_schema.trajectory):
        assert db_traj.singlepoint_record.specification.program == res_traj.provenance.creator.lower()
        assert db_traj.singlepoint_record.specification.basis == res_traj.model.basis
        assert db_traj.singlepoint_record.molecule.identifiers["molecule_hash"] == res_traj.molecule.get_hash()

    # Use plain schema, where compressed stuff is removed
    new_extras, new_properties = build_extras_properties(result_schema.copy(deep=True))
    assert record_orm.properties == new_properties
    assert record_orm.extras == new_extras

    # TODO - eventually schema may have these
    assert record_orm.native_files == {}

    for k in ("stdout", "stderr", "error"):
        plain_output = getattr(result_schema, k)
        if plain_output is not None:
            out_str = record_orm.compute_history[0].outputs[k].get_output()
            assert out_str == plain_output
        else:
            assert k not in record_orm.compute_history[0].outputs


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

        assert t.compute_tag == "tag1"
        assert t.compute_priority == PriorityEnum.low

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

        plain_result = convert_to_plain_qcschema_result(result)
        _compare_record_with_schema(record, plain_result)


def test_optimization_socket_insert_complete_schema_v1(storage_socket: SQLAlchemySocket, session: Session):
    test_names = [
        "opt_psi4_benzene",
        "opt_psi4_fluoroethane_notraj",
        "opt_psi4_methane",
        "opt_psi4_methane_sometraj",
    ]

    all_ids = []

    for test_name in test_names:
        _, _, result_schema = load_test_data(test_name)

        plain_schema = convert_to_plain_qcschema_result(result_schema)

        # Need a full copy of results - they can get mutated
        with storage_socket.session_scope() as session2:
            ins_ids_1 = storage_socket.records.insert_complete_schema_v1(session2, [result_schema.copy(deep=True)])
            ins_ids_2 = storage_socket.records.insert_complete_schema_v1(session2, [plain_schema.copy(deep=True)])

        ins_id_1 = ins_ids_1[0]
        ins_id_2 = ins_ids_2[0]

        # insert_complete_schema always inserts
        assert ins_id_1 != ins_id_2
        assert ins_id_1 not in all_ids
        assert ins_id_2 not in all_ids
        all_ids.extend([ins_id_1, ins_id_2])

        rec_1 = session.get(OptimizationRecordORM, ins_id_1)
        rec_2 = session.get(OptimizationRecordORM, ins_id_2)

        _compare_record_with_schema(rec_1, plain_schema)
        _compare_record_with_schema(rec_2, plain_schema)
