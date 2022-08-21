from qcfractal.db_socket import SQLAlchemySocket
from qcportal.records.manybody import ManybodySpecification
from qcportal.records.manybody.models import ManybodyKeywords
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification


def test_manybody_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):
    spec1 = ManybodySpecification(
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


def test_manybody_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec1 = ManybodySpecification(
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

    spec2 = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prOg1",
            driver="energy",
            method="b3LYP",
            basis="6-31g*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.existing_idx == [0]


def test_manybody_socket_add_diff_1(storage_socket: SQLAlchemySocket):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=4, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prOg1",
            driver="energy",
            method="b3LYP",
            basis="6-31g*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_diff_2(storage_socket: SQLAlchemySocket):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="cp"),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prOg1",
            driver="energy",
            method="b3LYP",
            basis="6-31g*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]


def test_manybody_socket_add_diff_3(storage_socket: SQLAlchemySocket):
    # Test different qc spec
    spec1 = ManybodySpecification(
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

    spec2 = ManybodySpecification(
        program="manybody",
        keywords=ManybodyKeywords(max_nbody=None, bsse_correction="none"),
        singlepoint_specification=QCSpecification(
            program="prOg1",
            driver="energy",
            method="bhlyp",
            basis="6-31g*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    meta, id = storage_socket.records.manybody.add_specification(spec1)
    assert meta.inserted_idx == [0]

    meta, id = storage_socket.records.manybody.add_specification(spec2)
    assert meta.inserted_idx == [0]
