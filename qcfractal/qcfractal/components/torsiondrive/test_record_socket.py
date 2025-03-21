from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.torsiondrive.record_db_models import TorsiondriveRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.auth import UserInfo, GroupInfo
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification, SinglepointProtocols
from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords
from qcportal.utils import now_at_utc
from .testing_helpers import compare_torsiondrive_specs, test_specs, load_test_data, generate_task_key

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName
    from sqlalchemy.orm.session import Session


@pytest.mark.parametrize("spec", test_specs)
def test_torsiondrive_socket_add_get(
    storage_socket: SQLAlchemySocket, session: Session, spec: TorsiondriveSpecification
):
    hooh = load_molecule_data("peroxide2")
    td_mol_1 = load_molecule_data("td_C9H11NO2_1")
    td_mol_2 = load_molecule_data("td_C9H11NO2_2")

    time_0 = now_at_utc()
    meta, ids = storage_socket.records.torsiondrive.add(
        [[hooh], [td_mol_1, td_mol_2]], spec, True, "tag1", PriorityEnum.low, None, None, True
    )
    time_1 = now_at_utc()
    assert meta.success

    recs = [session.get(TorsiondriveRecordORM, i) for i in ids]

    assert len(recs) == 2
    for r in recs:
        assert r.record_type == "torsiondrive"
        assert r.status == RecordStatusEnum.waiting
        assert compare_torsiondrive_specs(spec, r.specification.model_dict())

        # Service queue entry should exist with the proper tag and priority
        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert len(recs[0].initial_molecules) == 1
    assert len(recs[1].initial_molecules) == 2

    assert recs[0].initial_molecules[0].molecule.identifiers["molecule_hash"] == hooh.get_hash()

    # Not necessarily in the input order
    hash1 = recs[1].initial_molecules[0].molecule.identifiers["molecule_hash"]
    hash2 = recs[1].initial_molecules[1].molecule.identifiers["molecule_hash"]
    assert {hash1, hash2} == {td_mol_1.get_hash(), td_mol_2.get_hash()}


def test_torsiondrive_socket_find_existing_1(storage_socket: SQLAlchemySocket):
    spec = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[hooh]], spec, True, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[hooh]], spec, True, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_torsiondrive_socket_find_existing_2(storage_socket: SQLAlchemySocket):
    # multiple molecule ordering, and duplicate molecules
    spec = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec, True, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[mol2, mol3, mol1, mol2], [mol3, mol2, mol1, mol1]], spec, True, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id2 == [id1[0], id1[0]]


def test_torsiondrive_socket_find_existing_3(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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

    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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
                protocols=SinglepointProtocols(wavefunction="all", stdout=True),
            ),
        ),
    )

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec1, True, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]], spec2, True, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_torsiondrive_socket_add_different_1(storage_socket: SQLAlchemySocket):
    # Molecules are a subset of another
    spec = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
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

    mol1 = load_molecule_data("td_C9H11NO2_0")
    mol2 = load_molecule_data("td_C9H11NO2_1")
    mol3 = load_molecule_data("td_C9H11NO2_2")
    meta, id1 = storage_socket.records.torsiondrive.add(
        [[mol1, mol2, mol3]],
        spec,
        True,
        "*",
        PriorityEnum.normal,
        None,
        None,
        True,
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.torsiondrive.add(
        [[mol1], [mol3, mol2], [mol2, mol3, mol1]],
        spec,
        True,
        "*",
        PriorityEnum.normal,
        None,
        None,
        True,
    )
    assert meta.n_inserted == 2
    assert meta.n_existing == 1
    assert meta.existing_idx == [2]
    assert meta.inserted_idx == [0, 1]
    assert id1[0] == id2[2]


@pytest.mark.parametrize(
    "test_data_name",
    [
        "td_C9H11NO2_mopac_pm6",
        "td_H2O2_mopac_pm6",
        "td_H2O2_psi4_pbe",
    ],
)
def test_torsiondrive_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, molecules_1, result_data_1 = load_test_data(test_data_name)

    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.users.add(UserInfo(username="submit_user", role="submit", groups=["group1"], enabled=True))

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, True, "test_tag", PriorityEnum.low, "submit_user", "group1", True
    )
    id_1 = id_1[0]
    assert meta_1.success

    time_0 = now_at_utc()
    finished, n_optimizations = run_service(
        storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 1000
    )
    time_1 = now_at_utc()

    rec = session.get(TorsiondriveRecordORM, id_1)

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
    assert "Job Finished" in out

    assert len(rec.optimizations) == n_optimizations


def test_torsiondrive_socket_run_duplicate(
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):
    input_spec_1, molecules_1, result_data_1 = load_test_data("td_H2O2_mopac_pm6")

    meta_1, id_1 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, True, "test_tag", PriorityEnum.low, None, None, True
    )
    id_1 = id_1[0]
    assert meta_1.success

    run_service(storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 1000)

    rec_1 = session.get(TorsiondriveRecordORM, id_1)
    assert rec_1.status == RecordStatusEnum.complete
    opt_ids_1 = [x.optimization_id for x in rec_1.optimizations]

    # Submit again, without duplicate checking
    meta_2, id_2 = storage_socket.records.torsiondrive.add(
        [molecules_1], input_spec_1, True, "test_tag", PriorityEnum.low, None, None, False
    )
    id_2 = id_2[0]
    assert meta_2.success
    assert id_2 != id_1

    run_service(storage_socket, activated_manager_name, id_2, generate_task_key, result_data_1, 1000)

    rec_2 = session.get(TorsiondriveRecordORM, id_2)
    assert rec_2.status == RecordStatusEnum.complete
    opt_ids_2 = [x.optimization_id for x in rec_2.optimizations]

    assert set(opt_ids_1).isdisjoint(opt_ids_2)
