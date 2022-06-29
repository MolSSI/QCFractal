from qcfractal.db_socket import SQLAlchemySocket
from qcportal.records.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.records.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols
from qcportal.records.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords


def test_torsiondrive_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=QCSpecification(
                program="prog2",
                driver="deferred",
                method="b3lyp",
                basis="6-31g",
                keywords={"k2": "values2"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 14)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=QCSpecification(
                program="prog2",
                driver=SinglepointDriver.hessian,
                method="hf",
                basis="def2-tzvp",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec3 = TorsiondriveSpecification(
        # Not putting in program
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[5],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationSpecification(
            program="optprog2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(trajectory="none"),
            qc_specification=QCSpecification(
                program="prog2",
                driver=SinglepointDriver.hessian,
                method="hf",
                basis="def2-tzvp",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
            ),
        ),
    )

    meta1, id1 = storage_socket.records.torsiondrive.add_specification(spec1)
    meta2, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    meta3, id3 = storage_socket.records.torsiondrive.add_specification(spec3)
    assert meta1.success
    assert meta2.success
    assert meta3.success
    assert meta1.inserted_idx == [0]
    assert meta2.inserted_idx == [0]
    assert meta3.inserted_idx == [0]
    assert meta1.existing_idx == []
    assert meta2.existing_idx == []
    assert meta3.existing_idx == []


common_opt_spec = OptimizationSpecification(
    program="optprog2",
    keywords={"k": "value"},
    protocols=OptimizationProtocols(trajectory="none"),
    qc_specification=QCSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    ),
)


def test_torsiondrive_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    # Try inserting again
    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_torsiondrive_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):

    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    # model handling defaults
    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_torsiondrive_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # some changes to the opt spec
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    # model handling defaults
    spec2 = TorsiondriveSpecification(
        program="torSIOndrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationSpecification(
            program="optpROg2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(trajectory="none"),
            qc_specification=QCSpecification(
                program="PROG2",
                driver=SinglepointDriver.gradient,
                method="hf",
                basis="def2-tZVp",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
            ),
        ),
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_torsiondrive_socket_add_specification_diff_1(storage_socket: SQLAlchemySocket):
    #  changing energy upper limit
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            energy_upper_limit=0.06,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_torsiondrive_socket_add_specification_diff_2(storage_socket: SQLAlchemySocket):
    #  ordering of dihedrals
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = TorsiondriveSpecification(
        program="torsionndrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 13, 15)],
            grid_spacing=[15],
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_torsiondrive_socket_add_specification_diff_3(storage_socket: SQLAlchemySocket):
    #  grid spacing
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = TorsiondriveSpecification(
        program="torsionndrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[5],
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_torsiondrive_socket_add_specification_diff_4(storage_socket: SQLAlchemySocket):
    #  grid spacing
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.1,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_torsiondrive_socket_add_specification_diff_5(storage_socket: SQLAlchemySocket):
    #  grid spacing
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.2,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.1,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.torsiondrive.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.torsiondrive.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2
