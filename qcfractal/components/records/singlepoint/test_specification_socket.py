"""
Tests the wavefunction store socket
"""

from qcfractal.db_socket import SQLAlchemySocket
from qcportal.records.singlepoint import (
    SinglepointInputSpecification,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.keywords import KeywordSet


def test_singlepoint_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec2 = SinglepointInputSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec3 = SinglepointInputSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
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

    assert SinglepointInputSpecification(**sp1) == spec1
    assert SinglepointInputSpecification(**sp2) == spec2
    assert SinglepointInputSpecification(**sp3) == spec3


def test_singlepoint_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
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
    spec1 = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value2"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    meta, id3 = storage_socket.records.singlepoint.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id3 != id


def test_singlepoint_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="Prog1",
        driver=SinglepointDriver.energy,
        method="b3LYP",
        basis="6-31g*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Test keywords defaults
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={}),
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_3(storage_socket: SQLAlchemySocket):
    # Test protocols defaults
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_4(storage_socket: SQLAlchemySocket):
    # Test protocols defaults (due to exclude_defaults)
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": {}}
        ),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_5(storage_socket: SQLAlchemySocket):
    # Test protocols defaults (due to exclude_defaults)
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": None}
        ),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_6(storage_socket: SQLAlchemySocket):
    # Test basis none, empty string
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
        protocols=SinglepointProtocols(),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_diff_1(storage_socket: SQLAlchemySocket):
    # Test different protocols
    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = SinglepointInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
        protocols=SinglepointProtocols(stdout=False),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2
