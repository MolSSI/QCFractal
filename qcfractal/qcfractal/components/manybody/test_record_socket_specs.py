from qcfractal.db_socket import SQLAlchemySocket
from qcportal.manybody import ManybodySpecification
from qcportal.singlepoint import SinglepointProtocols, QCSpecification


def test_manybody_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    # Try inserting again
    meta, id2 = storage_socket.records.manybody.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_manybody_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prOg1",
                driver="energy",
                method="b3LYP",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.existing_idx == [0]


def test_manybody_socket_add_specification_same_3(storage_socket: SQLAlchemySocket):
    # Test supersystem
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            "supersystem": QCSpecification(
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

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.existing_idx == [0]


def test_manybody_socket_add_specification_same_4(storage_socket: SQLAlchemySocket):
    # Test ordering
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["nocp"],
        keywords={},
    )

    spec2 = ManybodySpecification(
        program="manybody",
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
        keywords={},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.existing_idx == [0]


def test_manybody_socket_add_specification_diff_1(storage_socket: SQLAlchemySocket):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_2(storage_socket: SQLAlchemySocket):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["vmfc"],
        keywords={"return_total_data": True},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_3(storage_socket: SQLAlchemySocket):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": False},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_4(storage_socket: SQLAlchemySocket):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_5(storage_socket: SQLAlchemySocket):
    # Test different levels
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_5(storage_socket: SQLAlchemySocket):
    # Test different levels
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="ccsd",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_6(storage_socket: SQLAlchemySocket):
    # Test different levels
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="none"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_specification_diff_7(storage_socket: SQLAlchemySocket):
    # Test different levels
    spec1 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="ccsd",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
    )

    spec2 = ManybodySpecification(
        program="manybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            "supersystem": QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]
