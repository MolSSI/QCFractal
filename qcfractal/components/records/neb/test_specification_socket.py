from qcfractal.db_socket import SQLAlchemySocket

from qcportal.records.singlepoint import (
    QCSpecification,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.records.neb import (
    NEBSpecification,
    NEBKeywords,
    NEBInitialchain
)


def test_neb_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
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
            images=31,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            method="CCSD(T)",
            basis="6-31g**",
            keywords={"k1": "values1"},
            driver=SinglepointDriver.gradient,
            protocols=SinglepointProtocols(wavefunction="all"),
            ),
    )

    spec3 = NEBSpecification(
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            method="CCSD(T)",
            basis="def2-tzvp",
            keywords={"k1": "values1"},
            driver=SinglepointDriver.gradient,
            protocols=SinglepointProtocols(wavefunction="all"),
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
        keywords=NEBKeywords(
        ),
        qc_specification=common_sp_spec,
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


def test_neb_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):

    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
    )

    # model handling defaults
    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.0,
        ),
        qc_specification=common_sp_spec,
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
    # some changes to the qc spec
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
    )
    # model handling defaults
    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            method="CCSD(T)",
            basis="def2-tzvp",
            keywords={"k1": "values1"},
            driver=SinglepointDriver.gradient,
            protocols=SinglepointProtocols(wavefunction="all"),
            ),
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
    #  turning energy weighted on and off
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.0,
            energy_weighted=True,
        ),
        qc_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
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
    #  changing spring constant
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.0,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
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
    #  number of images
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=1.5,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
    )
    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=31,
            spring_constant=1.5,
            energy_weighted=False,
        ),
        qc_specification=common_sp_spec,
    
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

