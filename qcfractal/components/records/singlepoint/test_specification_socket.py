from qcfractal.db_socket import SQLAlchemySocket
from qcportal.records.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols


def test_singlepoint_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec2 = QCSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec3 = QCSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords={"k": "value"},
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


def test_singlepoint_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
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
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value2"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    meta, id3 = storage_socket.records.singlepoint.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id3 != id


def test_singlepoint_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = QCSpecification(
        program="Prog1",
        driver=SinglepointDriver.energy,
        method="b3LYP",
        basis="6-31g*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    meta, id2 = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.existing_idx == [0]
    assert id == id2


def test_singlepoint_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Test keywords defaults
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={},
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = QCSpecification(
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
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = QCSpecification(
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
    spec = QCSpecification(
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

    spec = QCSpecification(
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
    spec = QCSpecification(
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

    spec = QCSpecification(
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
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = QCSpecification(
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
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    meta, id = storage_socket.records.singlepoint.add_specification(spec)
    assert meta.inserted_idx == [0]

    spec = QCSpecification(
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
