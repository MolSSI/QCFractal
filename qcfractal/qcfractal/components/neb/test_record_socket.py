"""
Tests the neb record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.neb.testing_helpers import (
    compare_neb_specs,
    test_specs,
)
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.neb import (
    NEBSpecification,
    NEBKeywords,
)
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import (
    QCSpecification,
    SinglepointProtocols,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

pytestmark = pytest.mark.xfail


@pytest.mark.parametrize("spec", test_specs)
def test_neb_socket_add_get(storage_socket: SQLAlchemySocket, spec: NEBSpecification):
    chain1 = [load_molecule_data("neb/neb_NCH_%i" % i) for i in range(43)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(60)]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.neb.add([chain1, chain2], spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.neb.get(id, include=["*", "initial_chain", "initial_chain.molecule", "service"])

    assert len(recs) == 2
    for r in recs:
        assert r["record_type"] == "neb"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_neb_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    assert len(recs[0]["initial_chain"]) == spec.keywords.images
    assert len(recs[1]["initial_chain"]) == spec.keywords.images


def test_neb_socket_add_same_chains_diff_order(storage_socket: SQLAlchemySocket):
    # Flipping the order of molecules in a chain generates different initial chain.
    spec = test_specs[0]

    chain1 = [load_molecule_data("neb/neb_NCH_%i" % i) for i in range(43)]
    chain2 = chain1[::-1]

    # Now add records
    meta, id = storage_socket.records.neb.add([chain1, chain2], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 0

    recs = storage_socket.records.neb.get(id, include=["initial_chains"])
    assert len(recs) == 2
    assert recs[0]["id"] != recs[1]["id"]


def test_neb_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            energy_weighted=20,
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
    chain1 = [load_molecule_data("neb/neb_NCH_%i" % i) for i in range(43)]

    meta, id1 = storage_socket.records.neb.add([chain1], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.neb.add([chain1], spec, tag="*", priority=PriorityEnum.normal)
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
            energy_weighted=20,
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
            energy_weighted=20,
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

    chain1 = [load_molecule_data("neb/neb_NCH_%i" % i) for i in range(43)]

    meta, id1 = storage_socket.records.neb.add([chain1], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.neb.add([chain1, chain1, chain1], spec2, tag="*", priority=PriorityEnum.normal)
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
            energy_weighted=20,
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
    chain1 = [load_molecule_data("neb/neb_NCH_%i" % i) for i in range(43)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(60)]
    meta, id1 = storage_socket.records.neb.add([chain1, chain2], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 2
    assert meta.inserted_idx == [0, 1]

    meta, id2 = storage_socket.records.neb.add(
        [chain1, chain2, chain2[::-1]], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 1
    assert meta.n_existing == 2
    assert meta.existing_idx == [0, 1]
    assert meta.inserted_idx == [2]
    assert id1[0] == id2[0]
    assert id1[1] == id2[1]
