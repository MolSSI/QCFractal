from qcfractal.db_socket import SQLAlchemySocket
from qcportal.records.singlepoint import (
    SinglepointInputSpecification,
    SinglepointDriver,
    SinglepointProtocols,
)
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationSinglepointInputSpecification,
    OptimizationProtocols,
)

from qcportal.records.torsiondrive import (
    TorsiondriveSpecification,
    TorsiondriveInputSpecification,
    TorsiondriveAddBody,
    TorsiondriveKeywords,
)
from qcportal.keywords import KeywordSet


def test_torsiondrive_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec2 = TorsiondriveInputSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 14)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                driver=SinglepointDriver.hessian,
                method="hf",
                basis="def2-tzvp",
                keywords=KeywordSet(values={"k": "value"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec3 = TorsiondriveInputSpecification(
        # Not putting in program
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[5],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(trajectory="none"),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="prog2",
                driver=SinglepointDriver.hessian,
                method="hf",
                basis="def2-tzvp",
                keywords=KeywordSet(values={"k": "value"}),
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

    sp1 = storage_socket.records.torsiondrive.get_specification(id1)
    sp2 = storage_socket.records.torsiondrive.get_specification(id2)
    sp3 = storage_socket.records.torsiondrive.get_specification(id3)

    for sp in [sp1, sp2, sp3]:
        assert sp["program"] == "torsiondrive"
        assert sp["optimization_specification_id"] == sp["optimization_specification"]["id"]

    assert TorsiondriveKeywords(**sp1["keywords"]) == spec1.keywords
    assert TorsiondriveKeywords(**sp2["keywords"]) == spec2.keywords
    assert TorsiondriveKeywords(**sp3["keywords"]) == spec3.keywords


common_opt_spec = OptimizationInputSpecification(
    program="optprog2",
    keywords={"k": "value"},
    protocols=OptimizationProtocols(trajectory="none"),
    singlepoint_specification=OptimizationSinglepointInputSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    ),
)


def test_torsiondrive_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = TorsiondriveInputSpecification(
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

    spec1 = TorsiondriveInputSpecification(
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
    spec2 = TorsiondriveInputSpecification(
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
    spec1 = TorsiondriveInputSpecification(
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
    spec2 = TorsiondriveInputSpecification(
        program="torSIOndrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optpROg2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(trajectory="none"),
            singlepoint_specification=OptimizationSinglepointInputSpecification(
                program="PROG2",
                driver=SinglepointDriver.gradient,
                method="hf",
                basis="def2-tZVp",
                keywords=KeywordSet(values={"k": "value"}),
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
    spec1 = TorsiondriveInputSpecification(
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

    spec2 = TorsiondriveInputSpecification(
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
    spec1 = TorsiondriveInputSpecification(
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

    spec2 = TorsiondriveInputSpecification(
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
    spec1 = TorsiondriveInputSpecification(
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

    spec2 = TorsiondriveInputSpecification(
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
