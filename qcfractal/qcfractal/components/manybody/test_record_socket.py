from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data, test_users
from qcfractal.components.manybody.record_db_models import ManybodyRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.auth import UserInfo
from qcportal.manybody import ManybodySpecification, compare_manybody_records
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import SinglepointProtocols, QCSpecification
from qcportal.utils import now_at_utc
from .testing_helpers import (
    compare_manybody_specs,
    test_specs,
    load_procedure_data,
    generate_task_key,
    load_record_data,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcarchivetesting.testing_classes import QCATestingSnowflake
    from qcportal.managers import ManagerName
    from sqlalchemy.orm.session import Session


@pytest.mark.parametrize("spec", test_specs)
def test_manybody_socket_add_get(storage_socket: SQLAlchemySocket, session: Session, spec: ManybodySpecification):
    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")

    time_0 = now_at_utc()
    meta, ids = storage_socket.records.manybody.add([water2, water4], spec, "tag1", PriorityEnum.low, None, True)
    time_1 = now_at_utc()
    assert meta.success

    recs = [session.get(ManybodyRecordORM, i) for i in ids]

    assert len(recs) == 2

    for r in recs:
        assert r.record_type == "manybody"
        assert r.status == RecordStatusEnum.waiting
        assert compare_manybody_specs(spec, r.specification.model_dict())

        # Service queue entry should exist with the proper tag and priority
        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert recs[0].initial_molecule.identifiers["molecule_hash"] == water2.get_hash()
    assert recs[1].initial_molecule.identifiers["molecule_hash"] == water4.get_hash()


def test_manybody_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = ManybodySpecification(
        program="qcmanybody",
        levels={
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")

    meta, id1 = storage_socket.records.manybody.add([water2, water4], spec, "*", PriorityEnum.normal, None, True)
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.manybody.add([water4, water2], spec, "*", PriorityEnum.normal, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id1[0] == id2[1]
    assert id1[1] == id2[0]


@pytest.mark.parametrize(
    "test_data_name",
    [
        "mb_cp_he4_psi4_mp2",
        "mb_all_he4_psi4_multi",
        "mb_all_he4_psi4_multiss",
    ],
)
def test_manybody_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data(test_data_name)

    storage_socket.users.add(UserInfo(username="submit_user", role="submit", enabled=True))

    meta_1, id_1 = storage_socket.records.manybody.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, "submit_user", True
    )
    id_1 = id_1[0]
    assert meta_1.success

    time_0 = now_at_utc()
    finished, n_singlepoints = run_service(
        storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 100
    )
    time_1 = now_at_utc()

    assert finished is True

    rec = session.get(ManybodyRecordORM, id_1)

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

    out = rec.compute_history[-1].outputs["stdout"].get_output()
    assert "All manybody singlepoint computations are complete" in out

    unique_sp = set(x.singlepoint_id for x in rec.clusters)
    assert len(unique_sp) == n_singlepoints


def test_manybody_socket_run_duplicate(
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):
    input_spec_1, molecules_1, result_data_1 = load_procedure_data("mb_cp_he4_psi4_mp2")

    meta_1, id_1 = storage_socket.records.manybody.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, True
    )
    id_1 = id_1[0]
    assert meta_1.success

    run_service(storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 1000)

    rec_1 = session.get(ManybodyRecordORM, id_1)
    assert rec_1.status == RecordStatusEnum.complete
    sp_ids_1 = [x.singlepoint_id for x in rec_1.clusters]

    # Submit again, without duplicate checking
    meta_2, id_2 = storage_socket.records.manybody.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, False
    )
    id_2 = id_2[0]
    assert meta_2.success
    assert id_2 != id_1

    run_service(storage_socket, activated_manager_name, id_2, generate_task_key, result_data_1, 1000)

    rec_2 = session.get(ManybodyRecordORM, id_2)
    assert rec_2.status == RecordStatusEnum.complete
    sp_ids_2 = [x.singlepoint_id for x in rec_2.clusters]

    assert set(sp_ids_1).isdisjoint(sp_ids_2)


def test_manybody_socket_insert_full_qcportal_record(secure_snowflake: QCATestingSnowflake):
    test_names = [
        "mb_all_he4_psi4_multi",
        "mb_all_he4_psi4_multiss",
        "mb_cp_he4_psi4_mp2",
    ]

    storage_socket = secure_snowflake.get_storage_socket()
    client = secure_snowflake.client("submit_user", test_users["submit_user"]["pw"])
    user_id = client.get_user().id

    for test_name in test_names:
        initial_record = load_record_data(test_name)
        initial_record_copy = initial_record.copy(deep=True)

        # Need a full copy of results - they can get mutated
        with storage_socket.session_scope() as session:
            ins_ids = storage_socket.records.insert_full_qcportal_records(session, [initial_record_copy], user_id)

        rec_1 = client.get_manybodys(ins_ids[0], include=["**"])
        rec_1.fetch_children(include=["**"], force_fetch=True)
        compare_manybody_records(rec_1, initial_record)

        assert rec_1.creator_user == "submit_user"
        for cl in rec_1.cluster_records_:
            assert cl.singlepoint_record.creator_user == "submit_user"
