from qcfractal.components.testing_fixtures import spec_test_runner
from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols

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


def test_gridoptimization_socket_add_specification_same_1(spec_test_runner):
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("gridoptimization", spec1, spec1, True)


def test_gridoptimization_socket_add_specification_same_2(spec_test_runner):
    # capitalization should be ignored
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationSpecification(
        program="gridopTIMIZatiON",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("gridoptimization", spec1, spec2, True)


def test_gridoptimization_socket_add_specification_diff_1(spec_test_runner):
    # different indices
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(
        update={"scans": [{"type": "distance", "indices": [0, 4], "steps": [2.0, 3.0, 4.0], "step_type": "relative"}]}
    )
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_2(spec_test_runner):
    # different steps
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(
        update={"scans": [{"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.1], "step_type": "relative"}]}
    )
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_3(spec_test_runner):
    # absolute vs relative
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(
        update={"scans": [{"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "absolute"}]}
    )
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_4(spec_test_runner):
    # preoptimization
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(update={"preoptimization": True})
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_5(spec_test_runner):
    # basis set
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"basis": "def2-qzvp"})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_6(spec_test_runner):
    # qc keywords
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"keywords": {"z": 1.0 - 10}})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("gridoptimization", spec1, spec2, False)
