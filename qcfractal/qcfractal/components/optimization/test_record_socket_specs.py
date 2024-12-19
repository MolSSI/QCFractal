from qcfractal.components.testing_fixtures import spec_test_runner
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols

common_qc_spec = QCSpecification(
    program="prog1",
    driver=SinglepointDriver.energy,
    method="b3lyp",
    basis="6-31G*",
    keywords={"k": 1.0e-8},
    protocols=SinglepointProtocols(),
)


def test_optimization_socket_add_specification_same_0(spec_test_runner):
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": "value"},
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    spec_test_runner("optimization", spec1, spec1, True)


def test_optimization_socket_add_specification_same_1(spec_test_runner):
    # Test case sensitivity
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": "value"},
        protocols=OptimizationProtocols(),
        qc_specification=QCSpecification(
            program="prog1",
            driver=SinglepointDriver.energy,
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": 1.0e-8},
            protocols=SinglepointProtocols(),
        ),
    )

    spec2 = OptimizationSpecification(
        program="optPRog1",
        keywords={"k": "value"},
        protocols=OptimizationProtocols(),
        qc_specification=QCSpecification(
            program="PROG1",
            driver=SinglepointDriver.energy,
            method="b3LYP",
            basis="6-31g*",
            keywords={"k": 1.0e-8},
            protocols=SinglepointProtocols(),
        ),
    )

    spec_test_runner("optimization", spec1, spec2, True)


def test_optimization_socket_add_specification_same_2(spec_test_runner):
    # Test keywords defaults
    spec1 = OptimizationSpecification(
        program="optprog1", keywords={}, protocols=OptimizationProtocols(), qc_specification=common_qc_spec
    )

    spec2 = OptimizationSpecification(
        program="optprog1", protocols=OptimizationProtocols(), qc_specification=common_qc_spec
    )

    spec_test_runner("optimization", spec1, spec2, True)


def test_optimization_socket_add_specification_same_3(spec_test_runner):
    # Test protocols defaults
    spec1 = OptimizationSpecification(program="optprog1", keywords={}, qc_specification=common_qc_spec)

    spec2 = OptimizationSpecification(
        program="optprog1", keywords={}, protocols=OptimizationProtocols(), qc_specification=common_qc_spec
    )

    spec_test_runner("optimization", spec1, spec2, True)


def test_optimization_socket_add_specification_same_4(spec_test_runner):
    # Test protocols defaults (due to exclude_defaults)
    spec1 = OptimizationSpecification(program="optprog1", keywords={}, qc_specification=common_qc_spec)

    spec2 = OptimizationSpecification(
        program="optprog1",
        keywords={},
        protocols=OptimizationProtocols(trajectory="all"),
        qc_specification=common_qc_spec,
    )

    spec_test_runner("optimization", spec1, spec2, True)


def test_optimization_socket_add_specification_same_5(spec_test_runner):
    # Test keyword ordering
    spec1 = OptimizationSpecification(
        program="optprog1", keywords={"a": 10, "b": "str"}, qc_specification=common_qc_spec
    )
    spec2 = OptimizationSpecification(
        program="optprog1", keywords={"b": "str", "a": 10}, qc_specification=common_qc_spec
    )

    spec_test_runner("optimization", spec1, spec2, True)


def test_optimization_socket_add_specification_diff_1(spec_test_runner):
    # Test different keywords
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": "value"},
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    spec2 = spec1.copy(update={"keywords": {"k2": "value2"}})

    spec_test_runner("optimization", spec1, spec2, False)


def test_optimization_socket_add_specification_diff_2(spec_test_runner):
    # Test different keywords
    spec1 = OptimizationSpecification(
        program="optprog1",
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    spec2 = spec1.copy(update={"keywords": {"k2": "value2"}})

    spec_test_runner("optimization", spec1, spec2, False)


def test_optimization_socket_add_specification_diff_3(spec_test_runner):
    # Test different keywords
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": 1.0e-8},
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    spec2 = spec1.copy(update={"keywords": {"k": 1.0e-9}})

    spec_test_runner("optimization", spec1, spec2, False)


def test_optimization_socket_add_specification_diff_4(spec_test_runner):
    # Test qc spec changes
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": 1.0e-8},
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    qc_spec = common_qc_spec.copy(update={"keywords": {"a": 1}})
    spec2 = spec1.copy(update={"qc_specification": qc_spec})

    spec_test_runner("optimization", spec1, spec2, False)


def test_optimization_socket_add_specification_diff_5(spec_test_runner):
    # Test qc spec changes
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": 1.0e-8},
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    qc_spec = common_qc_spec.copy(update={"protocols": SinglepointProtocols(stdout=False)})
    spec2 = spec1.copy(update={"qc_specification": qc_spec})

    spec_test_runner("optimization", spec1, spec2, False)


def test_optimization_socket_add_specification_diff_6(spec_test_runner):
    # Test different protocols
    spec1 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": "value"},
        protocols=OptimizationProtocols(),
        qc_specification=common_qc_spec,
    )

    spec2 = OptimizationSpecification(
        program="optprog1",
        keywords={"k": "value"},
        protocols=OptimizationProtocols(trajectory="initial_and_final"),
        qc_specification=common_qc_spec,
    )

    spec_test_runner("optimization", spec1, spec2, False)
