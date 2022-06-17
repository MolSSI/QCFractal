from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_simple
from qcfractaltesting import load_molecule_data
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.manybody import ManybodySpecification
from qcportal.records.manybody.models import ManybodyKeywords
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification
from .testing_helpers import compare_manybody_specs, test_specs, load_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


@pytest.mark.parametrize("spec", test_specs[:1])
def test_manybody_socket_add_get(storage_socket: SQLAlchemySocket, spec: ManybodySpecification):
    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.manybody.add([water2, water4], spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.manybody.get(id, include=["*", "initial_molecule", "service"])

    assert len(recs) == 2

    for r in recs:
        assert r["record_type"] == "manybody"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_manybody_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    assert recs[0]["initial_molecule"]["identifiers"]["molecule_hash"] == water2.get_hash()
    assert recs[1]["initial_molecule"]["identifiers"]["molecule_hash"] == water4.get_hash()


def test_manybody_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    water2 = load_molecule_data("water_dimer_minima")
    water4 = load_molecule_data("water_stacked")

    meta, id1 = storage_socket.records.manybody.add([water2, water4], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.manybody.add([water4, water2], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert id1[0] == id2[1]
    assert id1[1] == id2[0]


@pytest.mark.parametrize(
    "test_data_name",
    [
        "mb_none_he4_psi4_mp2",
        "mb_cp_he4_psi4_mp2",
    ],
)
def test_manybody_socket_run(
    storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, test_data_name: str
):
    input_spec_1, molecules_1, result_data_1 = load_test_data(test_data_name)

    meta_1, id_1 = storage_socket.records.manybody.add(
        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low
    )
    assert meta_1.success

    time_0 = datetime.utcnow()
    finished, n_singlepoints = run_service_simple(storage_socket, activated_manager_name, id_1[0], result_data_1, 100)
    time_1 = datetime.utcnow()

    assert finished is True

    rec = storage_socket.records.manybody.get(
        id_1, include=["*", "compute_history.*", "compute_history.outputs", "clusters", "service"]
    )

    assert rec[0]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["modified_on"] < time_1
    assert len(rec[0]["compute_history"]) == 1
    assert len(rec[0]["compute_history"][-1]["outputs"]) == 1
    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.complete
    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
    assert rec[0]["service"] is None
    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"]["stdout"])
    assert "All manybody singlepoint computations are complete" in out.as_string

    assert len(rec[0]["clusters"]) == n_singlepoints
