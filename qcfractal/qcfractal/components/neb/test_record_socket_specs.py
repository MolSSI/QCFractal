from qcfractal.db_socket import SQLAlchemySocket
from qcportal.neb import NEBSpecification, NEBKeywords
from qcportal.singlepoint import (
    QCSpecification,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.optimization import OptimizationSpecification


def test_neb_socket_basic_specification(storage_socket: SQLAlchemySocket):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.0,
            spring_type=2,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            method="b3lyp",
            basis="6-31g",
            keywords={"k1": "values1"},
            driver="gradient",
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=1.0,
            spring_type=0,
        ),
        singlepoint_specification=QCSpecification(
            program="qchem",
            method="CCSD(T)",
            basis="6-31g",
            keywords={"k1": "values2"},
            driver=SinglepointDriver.gradient,
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
    )

    spec3 = NEBSpecification(
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.0,
            spring_type=1,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            method="CCSD(T)",
            basis="def2-tzvp",
            keywords={"k1": "values1"},
            driver=SinglepointDriver.hessian,
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="mopac",
                method="pm7",
                basis="",
                keywords={"k1": "values1"},
                driver=SinglepointDriver.deferred,
            ),
            protocols={},
        ),
    )

    meta1, id1 = storage_socket.records.neb.add_specification(spec1)
    meta2, id2 = storage_socket.records.neb.add_specification(spec2)
    meta3, id3 = storage_socket.records.neb.add_specification(spec3)
    assert meta1.success
    assert meta2.success
    assert meta3.success
    assert meta1.inserted_idx == [0]
    assert meta2.inserted_idx == [0]
    assert meta3.inserted_idx == [0]
    assert meta1.existing_idx == []
    assert meta2.existing_idx == []
    assert meta3.existing_idx == []


common_sp_spec = QCSpecification(
    program="psi4",
    method="CCSD(T)",
    basis="def2-tzvp",
    keywords={"k1": "values1"},
    driver=SinglepointDriver.gradient,
    protocols=SinglepointProtocols(wavefunction="all"),
)


def test_neb_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(images=11, spring_constant=1.0, optimize_ts=True, optimize_endpoints=True),
        singlepoint_specification=common_sp_spec,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    # Try inserting again
    meta, id2 = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2

    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(images=11, spring_constant=2.0, optimize_ts=True, optimize_endpoints=True),
        singlepoint_specification=common_sp_spec,
    )

    meta, id3 = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id3 != id


def test_neb_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    # model handling defaults
    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_neb_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Test keywords defaults
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords={},
        singlepoint_specification=common_sp_spec,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_neb_socket_add_specification_same_3(storage_socket: SQLAlchemySocket):
    # Test protocol defaults

    sp_spec_1 = QCSpecification(
        program="psi4",
        method="CCSD(T)",
        basis="def2-tzvp",
        keywords={"k1": "values1"},
        driver=SinglepointDriver.gradient,
        protocols=SinglepointProtocols(),
    )

    sp_spec_2 = QCSpecification(
        program="psi4",
        method="CCSD(T)",
        basis="def2-tzvp",
        keywords={"k1": "values1"},
        driver=SinglepointDriver.gradient,
    )

    spec1 = NEBSpecification(
        program="geometric",
        keywords={},
        singlepoint_specification=sp_spec_1,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords={},
        singlepoint_specification=sp_spec_2,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_neb_socket_add_specification_same_4(storage_socket: SQLAlchemySocket):
    # Test protocol defaults

    sp_spec_1 = QCSpecification(
        program="psi4",
        method="CCSD(T)",
        basis="def2-tzvp",
        keywords={"k1": "values1"},
        driver=SinglepointDriver.gradient,
        protocols=SinglepointProtocols(),
    )

    sp_spec_2 = QCSpecification(
        program="psi4",
        method="CCSD(T)",
        basis="def2-tzvp",
        keywords={"k1": "values1"},
        driver=SinglepointDriver.gradient,
    )

    spec1 = NEBSpecification(
        program="geometric",
        keywords={},
        singlepoint_specification=sp_spec_1,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords={},
        singlepoint_specification=sp_spec_2,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_neb_socket_add_specification_diff_1(storage_socket: SQLAlchemySocket):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=1.0,
            spring_type=1,
        ),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
        ),
        singlepoint_specification=common_sp_spec,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_neb_socket_add_specification_diff_2(storage_socket: SQLAlchemySocket):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=True,
        ),
        singlepoint_specification=common_sp_spec,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_neb_socket_add_specification_diff_3(storage_socket: SQLAlchemySocket):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=True,
            hessian_reset=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=True,
        ),
        singlepoint_specification=common_sp_spec,
    )

    meta, id = storage_socket.records.neb.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.neb.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2
