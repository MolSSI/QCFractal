from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.neb.record_db_models import NEBRecordORM
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service
from qcportal.auth import UserInfo, GroupInfo
from qcportal.neb import (
    NEBSpecification,
    NEBKeywords,
)
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import (
    QCSpecification,
    SinglepointProtocols,
)
from qcportal.utils import now_at_utc
from .testing_helpers import compare_neb_specs, test_specs, load_test_data, generate_task_key

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName
    from sqlalchemy.orm.session import Session


@pytest.mark.parametrize("spec", test_specs)
def test_neb_socket_add_get(storage_socket: SQLAlchemySocket, session: Session, spec: NEBSpecification):
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)]

    time_0 = now_at_utc()
    meta, ids = storage_socket.records.neb.add([chain1, chain2], spec, "tag1", PriorityEnum.low, None, None, True)
    time_1 = now_at_utc()
    assert meta.success

    recs = [session.get(NEBRecordORM, i) for i in ids]

    assert len(recs) == 2
    for r in recs:
        assert r.record_type == "neb"
        assert r.status == RecordStatusEnum.waiting
        assert compare_neb_specs(spec, r.specification.model_dict())

        # Service queue entry should exist with the proper tag and priority
        assert r.service.compute_tag == "tag1"
        assert r.service.compute_priority == PriorityEnum.low

        assert time_0 < r.created_on < time_1
        assert time_0 < r.modified_on < time_1

    assert len(recs[0].initial_chain) == spec.keywords.images
    assert len(recs[1].initial_chain) == spec.keywords.images


def test_neb_socket_add_same_chains_diff_order(storage_socket: SQLAlchemySocket, session: Session):
    # Flipping the order of molecules in a chain generates different initial chain.
    spec = test_specs[0]
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = chain1[::-1]

    # Now add records
    meta, ids = storage_socket.records.neb.add([chain1, chain2], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 0

    recs = [session.get(NEBRecordORM, i) for i in ids]
    assert len(recs) == 2
    assert recs[0].id != recs[1].id


def test_neb_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            spring_type=0,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]

    meta, id1 = storage_socket.records.neb.add([chain1], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.neb.add([chain1], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_neb_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            spring_type=1,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            spring_type=1,
        ),
        singlepoint_specification=QCSpecification(
            program="PSI4",
            keywords={"k": "value"},
            driver="gradient",
            method="ccsd(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all", stdout=True),
        ),
    )

    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]

    meta, id1 = storage_socket.records.neb.add([chain1], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.neb.add(
        [chain1, chain1, chain1], spec2, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 3
    assert meta.existing_idx == [0, 1, 2]
    assert id1[0] == id2[0]


def test_neb_socket_add_different_1(storage_socket: SQLAlchemySocket):
    # Molecules are a subset of another
    spec = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            spring_type=0,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )
    chain1 = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)]
    meta, id1 = storage_socket.records.neb.add([chain1, chain2], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.neb.add(
        [chain1, chain2, chain2[::-1]], spec, "*", PriorityEnum.normal, None, None, True
    )
    assert meta.n_inserted == 1
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert meta.inserted_idx == [2]
    assert id1[0] == id2[0]
    assert id1[1] == id2[1]


@pytest.mark.parametrize(
    "test_data_name",
    [
        "neb_HCN_psi4_pbe",
        "neb_HCN_psi4_pbe0_opt1",
        "neb_HCN_psi4_pbe_opt2",
        "neb_HCN_psi4_b3lyp_opt3",
        "neb_HCN_psi4_pbe_opt_diff",
    ],
)
def test_neb_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, initial_chain_1, result_data_1 = load_test_data(test_data_name)

    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.users.add(UserInfo(username="submit_user", role="submit", groups=["group1"], enabled=True))

    meta_1, id_1 = storage_socket.records.neb.add(
        [initial_chain_1],
        input_spec_1,
        "test_tag",
        PriorityEnum.low,
        "submit_user",
        "group1",
        True,
    )
    assert meta_1.success

    time_0 = now_at_utc()
    finished, n_spopt = run_service(
        storage_socket, activated_manager_name, id_1[0], generate_task_key, result_data_1, 150
    )
    time_1 = now_at_utc()

    rec = session.get(NEBRecordORM, id_1)

    assert rec.status == RecordStatusEnum.complete
    assert time_0 < rec.modified_on < time_1
    assert len(rec.compute_history) == 1
    assert len(rec.compute_history[-1].outputs) == 1
    assert rec.compute_history[-1].status == RecordStatusEnum.complete
    assert time_0 < rec.compute_history[-1].modified_on < time_1
    assert rec.service is None

    desc_info = storage_socket.records.get_short_descriptions(id_1)[0]
    short_desc = desc_info["description"]
    assert desc_info["record_type"] == rec.record_type
    assert desc_info["created_on"] == rec.created_on
    assert rec.specification.program in short_desc
    assert rec.specification.singlepoint_specification.program in short_desc
    assert rec.specification.singlepoint_specification.method in short_desc

    out = rec.compute_history[-1].outputs["stdout"].get_output()
    assert "NEB calculation is completed" in out


def test_neb_socket_run_duplicate(
    storage_socket: SQLAlchemySocket,
    session: Session,
    activated_manager_name: ManagerName,
):
    input_spec_1, molecules_1, result_data_1 = load_test_data("neb_HCN_psi4_pbe_opt_diff")

    meta_1, id_1 = storage_socket.records.neb.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, None, True
    )
    id_1 = id_1[0]
    assert meta_1.success

    run_service(storage_socket, activated_manager_name, id_1, generate_task_key, result_data_1, 100)

    rec_1 = session.get(NEBRecordORM, id_1)
    assert rec_1.status == RecordStatusEnum.complete
    sp_ids_1 = [x.singlepoint_id for x in rec_1.singlepoints]
    opt_ids_1 = [x.optimization_id for x in rec_1.optimizations]

    # Submit again, without duplicate checking
    meta_2, id_2 = storage_socket.records.neb.add(
        [molecules_1], input_spec_1, "test_tag", PriorityEnum.low, None, None, False
    )
    id_2 = id_2[0]
    assert meta_2.success
    assert id_2 != id_1

    run_service(storage_socket, activated_manager_name, id_2, generate_task_key, result_data_1, 1000)

    rec_2 = session.get(NEBRecordORM, id_2)
    assert rec_2.status == RecordStatusEnum.complete
    sp_ids_2 = [x.singlepoint_id for x in rec_2.singlepoints]
    opt_ids_2 = [x.optimization_id for x in rec_2.optimizations]

    assert set(sp_ids_1 + opt_ids_1).isdisjoint(sp_ids_2 + opt_ids_2)
