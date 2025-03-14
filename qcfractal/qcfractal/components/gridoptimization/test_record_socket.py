from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.gridoptimization.record_db_models import GridoptimizationRecordORM
from qcfractal.components.optimization.record_db_models import OptimizationRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.auth import UserInfo, GroupInfo
from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification, SinglepointProtocols
from qcportal.utils import now_at_utc
from .testing_helpers import compare_gridoptimization_specs, test_specs, load_test_data, generate_task_key

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName
    from sqlalchemy.orm.session import Session


@pytest.mark.parametrize("spec", test_specs)
def test_gridoptimization_socket_add_get(
    storage_socket: SQLAlchemySocket, session: Session, spec: GridoptimizationSpecification
):
    hooh = load_molecule_data("peroxide2")
    h3ns = load_molecule_data("go_H3NS")

    time_0 = now_at_utc()
    meta, ids = storage_socket.records.gridoptimization.add(
        [hooh, h3ns], spec, "tag1", PriorityEnum.low, None, None, True
    )
    time_1 = now_at_utc()
    assert meta.success

    recs = [session.get(GridoptimizationRecordORM, i) for i in ids]

    assert len(recs) == 2
    for r in recs:
        assert r.record_type == "gridoptimization"
        assert r.status == RecordStatusEnum.waiting
        assert compare_gridoptimization_specs(spec, r.specification.model_dict())

        # Service queue entry should exist with the proper tag and priority
        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert recs[0].initial_molecule.identifiers["molecule_hash"] == hooh.get_hash()
    assert recs[1].initial_molecule.identifiers["molecule_hash"] == h3ns.get_hash()


def test_gridoptimization_socket_find_existing_1(storage_socket: SQLAlchemySocket):
    spec = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=QCSpecification(
                program="prog2",
                driver="deferred",
                method="b3lyp",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    hooh = load_molecule_data("peroxide2")
    meta, id1 = storage_socket.records.gridoptimization.add([hooh], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.gridoptimization.add([hooh], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_gridoptimization_socket_find_existing_2(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=True,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=QCSpecification(
                program="prog2",
                driver="deferred",
                method="b3lyp",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(wavefunction="none"),
            ),
        ),
    )

    spec2 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationSpecification(
            program="optPROG1",
            keywords={"k": "value"},
            qc_specification=QCSpecification(
                program="prOG2",
                driver="deferred",
                method="b3LYP",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(stdout=True),
            ),
        ),
    )

    mol1 = load_molecule_data("go_H3NS")
    mol2 = load_molecule_data("peroxide2")
    meta, id1 = storage_socket.records.gridoptimization.add(
        [mol1, mol2], spec1, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.gridoptimization.add(
        [mol1, mol2], spec2, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id1 == id2


@pytest.mark.parametrize(
    "test_data_name",
    [
        "go_C4H4N2OS_mopac_pm6",
        "go_H2O2_psi4_b3lyp",
        "go_H3NS_psi4_pbe",
    ],
)
def test_gridoptimization_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, molecules_1, result_data_1 = load_test_data(test_data_name)

    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.users.add(UserInfo(username="submit_user", role="submit", groups=["group1"], enabled=True))

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, "submit_user", "group1", True
    )
    id_1 = id_1[0]
    assert meta_1.success

    time_0 = now_at_utc()
    finished, n_optimizations = run_service(
        storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 100
    )
    time_1 = now_at_utc()

    assert finished is True

    rec = session.get(GridoptimizationRecordORM, id_1)

    assert rec.status == RecordStatusEnum.complete
    assert time_0 < rec.modified_on < time_1
    assert len(rec.compute_history) == 1
    assert len(rec.compute_history[-1].outputs) == 1
    assert rec.compute_history[-1].status == RecordStatusEnum.complete
    assert time_0 < rec.compute_history[-1].modified_on < time_1
    assert rec.service is None

    desc_info = storage_socket.records.get_short_descriptions([id_1])[0]
    short_desc = desc_info["description"]
    assert desc_info["record_type"] == rec.record_type
    assert desc_info["created_on"] == rec.created_on
    assert rec.specification.program in short_desc
    assert rec.specification.optimization_specification.program in short_desc
    assert rec.specification.optimization_specification.qc_specification.program in short_desc
    assert rec.specification.optimization_specification.qc_specification.method in short_desc

    out = rec.compute_history[-1].outputs["stdout"].get_output()
    assert "Grid optimization finished successfully!" in out

    assert len(rec.optimizations) == n_optimizations

    for o in rec.optimizations:
        optr = session.get(OptimizationRecordORM, o.optimization_id)
        assert optr.energies[-1] == o.energy


def test_gridoptimization_socket_run_duplicate(
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):
    input_spec_1, molecules_1, result_data_1 = load_test_data("go_H2O2_psi4_b3lyp")

    meta_1, id_1 = storage_socket.records.gridoptimization.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, None, True
    )
    id_1 = id_1[0]
    assert meta_1.success

    run_service(storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 100)

    rec_1 = session.get(GridoptimizationRecordORM, id_1)
    assert rec_1.status == RecordStatusEnum.complete
    opt_ids_1 = [x.optimization_id for x in rec_1.optimizations]

    # Submit again, without duplicate checking
    meta_2, id_2 = storage_socket.records.gridoptimization.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, None, False
    )
    id_2 = id_2[0]
    assert meta_2.success
    assert id_2 != id_1

    run_service(storage_socket, activated_manager_name, id_2, generate_task_key, result_data_1, 1000)

    rec_2 = session.get(GridoptimizationRecordORM, id_2)
    assert rec_2.status == RecordStatusEnum.complete
    opt_ids_2 = [x.optimization_id for x in rec_2.optimizations]

    assert set(opt_ids_1).isdisjoint(opt_ids_2)
