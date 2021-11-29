"""
Tests the wavefunction store socket
"""

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal.records.singlepoint import (
    SinglePointInputSpecification,
    SinglePointDriver,
    SinglePointProtocols,
)
from qcfractal.portal.keywords import KeywordSet


def test_singlepoint_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglePointProtocols(wavefunction="all"),
    )

    spec2 = SinglePointInputSpecification(
        program="prog2",
        driver=SinglePointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglePointProtocols(wavefunction="all"),
    )

    spec3 = SinglePointInputSpecification(
        program="prog2",
        driver=SinglePointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglePointProtocols(wavefunction="orbitals_and_eigenvalues"),
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

    # Remove some ids that aren't in the input spec
    for sp in [sp1, sp2, sp3]:
        assert sp["keywords_id"] == sp["keywords"]["id"]
        sp["keywords"].pop("id")
        sp.pop("keywords_id")
        sp.pop("id")

    assert SinglePointInputSpecification(**sp1) == spec1
    assert SinglePointInputSpecification(**sp2) == spec2
    assert SinglePointInputSpecification(**sp3) == spec3


def test_singlepoint_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglePointProtocols(wavefunction="all"),
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
    spec1 = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value2"}),
        protocols=SinglePointProtocols(wavefunction="all"),
    )

    meta, id3 = storage_socket.records.singlepoint.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id3 != id


def test_singlepoint_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglePointProtocols(wavefunction="all"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointInputSpecification(
        program="Prog1",
        driver=SinglePointDriver.energy,
        method="b3LYP",
        basis="6-31g*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglePointProtocols(wavefunction="all"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Test keywords defaults
    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={}),
        protocols=SinglePointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglePointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_3(storage_socket: SQLAlchemySocket):
    # Test protocols defaults
    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglePointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_4(storage_socket: SQLAlchemySocket):
    # Test protocols defaults (due to exclude_defaults)
    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglePointProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": {}}
        ),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_5(storage_socket: SQLAlchemySocket):
    # Test protocols defaults (due to exclude_defaults)
    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglePointProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": None}
        ),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_6(storage_socket: SQLAlchemySocket):
    # Test basis none, empty string
    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglePointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglePointInputSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="",
        protocols=SinglePointProtocols(),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2
