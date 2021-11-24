"""
Tests the wavefunction store socket
"""

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.exceptions import MissingDataError
from qcfractal.portal.components.wavefunctions.models import WavefunctionProperties
from qcfractal.testing import load_wavefunction_data
from qcfractal.portal.components.records.singlepoint import (
    SinglePointSpecification,
    SinglePointDriver,
    AtomicResultProtocols,
)
from qcfractal.portal.components.keywords import KeywordSet


def test_singlepointrecord_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=AtomicResultProtocols(wavefunction="all"),
    )

    spec2 = SinglePointSpecification(
        program="prog2",
        driver=SinglePointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=AtomicResultProtocols(wavefunction="all"),
    )

    spec3 = SinglePointSpecification(
        program="prog2",
        driver=SinglePointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=AtomicResultProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta1, id1 = storage_socket.records.singlepoint.add_specification(spec1)
    meta2, id2 = storage_socket.records.singlepoint.add_specification(spec2)
    meta3, id3 = storage_socket.records.singlepoint.add_specification(spec3)
    assert meta1.success
    assert meta2.success
    assert meta3.success
    assert meta1.inserted_idx == [0]
    assert meta2.inserted_idx == [0]
    assert meta3.inserted_idx == [0]
    assert meta1.existing_idx == []
    assert meta2.existing_idx == []
    assert meta3.existing_idx == []

    sp1 = storage_socket.records.singlepoint.get_specification(id1)
    sp2 = storage_socket.records.singlepoint.get_specification(id2)
    sp3 = storage_socket.records.singlepoint.get_specification(id3)

    # Keywords now have id. Remove those for comparison
    sp1["keywords"].pop("id")
    sp2["keywords"].pop("id")
    sp3["keywords"].pop("id")
    assert SinglePointSpecification(**sp1) == spec1
    assert SinglePointSpecification(**sp2) == spec2
    assert SinglePointSpecification(**sp3) == spec3


def test_singlepointrecord_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=AtomicResultProtocols(wavefunction="all"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    # Try inserting again
    meta, id2 = storage_socket.records.singlepoint.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2

    # Change keywords
    spec1 = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value2"}),
        protocols=AtomicResultProtocols(wavefunction="all"),
    )

    meta, id3 = storage_socket.records.singlepoint.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id3 != id


def test_singlepointrecord_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=AtomicResultProtocols(wavefunction="all"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointSpecification(
        program="Prog1",
        driver=SinglePointDriver.energy,
        method="b3LYP",
        basis="6-31g*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=AtomicResultProtocols(wavefunction="all"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepointrecord_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Test keywords defaults
    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={}),
        protocols=AtomicResultProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=AtomicResultProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepointrecord_socket_add_specification_same_3(storage_socket: SQLAlchemySocket):
    # Test protocols defaults
    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=AtomicResultProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepointrecord_socket_add_specification_same_4(storage_socket: SQLAlchemySocket):
    # Test protocols defaults (due to exclude_defaults)
    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=AtomicResultProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": {}}
        ),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepointrecord_socket_add_specification_same_5(storage_socket: SQLAlchemySocket):
    # Test protocols defaults (due to exclude_defaults)
    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=AtomicResultProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": None}
        ),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepointrecord_socket_add_specification_same_6(storage_socket: SQLAlchemySocket):
    # Test basis none, empty string
    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=AtomicResultProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="",
        protocols=AtomicResultProtocols(),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2
