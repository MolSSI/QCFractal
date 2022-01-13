from qcfractal.db_socket import SQLAlchemySocket
from qcportal.keywords import KeywordSet
from qcportal.records.gridoptimization import (
    GridoptimizationInputSpecification,
    GridoptimizationKeywords,
)
from qcportal.records.optimization import (
    OptimizationInputSpecification,
    OptimizationQCInputSpecification,
    OptimizationProtocols,
)
from qcportal.records.singlepoint import (
    SinglepointDriver,
    SinglepointProtocols,
)


def test_gridoptimization_socket_basic_specification(storage_socket: SQLAlchemySocket):

    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog1",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                method="b3lyp",
                basis="6-31g",
                keywords=KeywordSet(values={"k2": "values2"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0, 90], "step_type": "absolute"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                driver=SinglepointDriver.hessian,
                method="hf",
                basis="def2-tzvp",
                keywords=KeywordSet(values={"k": "value"}),
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    )

    spec3 = GridoptimizationInputSpecification(
        # Not putting in program
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=OptimizationInputSpecification(
            program="optprog2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(trajectory="none"),
            qc_specification=OptimizationQCInputSpecification(
                program="prog2",
                driver=SinglepointDriver.hessian,
                method="hf",
                basis="def2-tzvp",
                keywords=KeywordSet(values={"k": "value"}),
                protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
            ),
        ),
    )

    meta1, id1 = storage_socket.records.gridoptimization.add_specification(spec1)
    meta2, id2 = storage_socket.records.gridoptimization.add_specification(spec2)
    meta3, id3 = storage_socket.records.gridoptimization.add_specification(spec3)
    assert meta1.success
    assert meta2.success
    assert meta3.success
    assert meta1.inserted_idx == [0]
    assert meta2.inserted_idx == [0]
    assert meta3.inserted_idx == [0]
    assert meta1.existing_idx == []
    assert meta2.existing_idx == []
    assert meta3.existing_idx == []

    sp1 = storage_socket.records.gridoptimization.get_specification(id1)
    sp2 = storage_socket.records.gridoptimization.get_specification(id2)
    sp3 = storage_socket.records.gridoptimization.get_specification(id3)

    for sp in [sp1, sp2, sp3]:
        assert sp["program"] == "gridoptimization"
        assert sp["optimization_specification_id"] == sp["optimization_specification"]["id"]

    assert GridoptimizationKeywords(**sp1["keywords"]) == spec1.keywords
    assert GridoptimizationKeywords(**sp2["keywords"]) == spec2.keywords
    assert GridoptimizationKeywords(**sp3["keywords"]) == spec3.keywords


common_opt_spec = OptimizationInputSpecification(
    program="optprog2",
    keywords={"k": "value"},
    protocols=OptimizationProtocols(trajectory="none"),
    qc_specification=OptimizationQCInputSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    ),
)


def test_gridoptimization_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):

    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    # Try inserting again
    meta, id2 = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_gridoptimization_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # capitalization should be ignored
    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridopTIMIZatiON",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.gridoptimization.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_gridoptimization_socket_add_specification_diff_1(storage_socket: SQLAlchemySocket):
    # different indices
    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 4], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.gridoptimization.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_gridoptimization_socket_add_specification_diff_2(storage_socket: SQLAlchemySocket):
    # different steps
    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.1], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.gridoptimization.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_gridoptimization_socket_add_specification_diff_3(storage_socket: SQLAlchemySocket):
    # absolute vs relative
    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "absolute"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.gridoptimization.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2


def test_gridoptimization_socket_add_specification_diff_4(storage_socket: SQLAlchemySocket):
    # absolute vs relative
    spec1 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationInputSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=True,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    meta, id = storage_socket.records.gridoptimization.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.gridoptimization.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id != id2
