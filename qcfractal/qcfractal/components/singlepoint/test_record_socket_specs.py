from qcfractal.components.testing_fixtures import spec_test_runner
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols


def test_singlepoint_socket_add_specification_same_1(spec_test_runner):
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec_test_runner("singlepoint", spec1, spec1, True)


def test_singlepoint_socket_add_specification_same_2(spec_test_runner):
    # Test case sensitivity
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec2 = QCSpecification(
        program="Prog1",
        driver=SinglepointDriver.energy,
        method="b3LYP",
        basis="6-31g*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_same_3(spec_test_runner):
    # Test keywords defaults
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={},
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_same_4(spec_test_runner):
    # Test protocols defaults
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(),
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_same_5(spec_test_runner):
    # Test protocols defaults
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": {}}
        ),
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_same_6(spec_test_runner):
    # Test protocols defaults
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        protocols=SinglepointProtocols(
            wavefunction="none", stdout=True, error_correction={"default_policy": True, "policies": None}
        ),
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_same_7(spec_test_runner):
    # Test basis none vs empty string
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
        protocols=SinglepointProtocols(),
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_same_8(spec_test_runner):
    # keyword ordering
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="sto-3g",
        protocols=SinglepointProtocols(),
        keywords={"a": 10, "b": "str"},
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="sto-3g",
        protocols=SinglepointProtocols(),
        keywords={"b": "str", "a": 10},
    )

    spec_test_runner("singlepoint", spec1, spec2, True)


def test_singlepoint_socket_add_specification_diff_1(spec_test_runner):
    # Test different programs
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="sto-3g",
    )

    spec2 = spec1.copy(update={"program": "prog2"})

    spec_test_runner("singlepoint", spec1, spec2, False)


def test_singlepoint_socket_add_specification_diff_2(spec_test_runner):
    # Test different driver
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.gradient,
        method="b3lyp",
        basis=None,
    )

    spec2 = spec1.copy(update={"driver": SinglepointDriver.energy})

    spec_test_runner("singlepoint", spec1, spec2, False)


def test_singlepoint_socket_add_specification_diff_3(spec_test_runner):
    # Test different keywords
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
    )

    spec2 = spec1.copy(update={"keywords": {"k": "value"}})

    spec_test_runner("singlepoint", spec1, spec2, False)


def test_singlepoint_socket_add_specification_diff_4(spec_test_runner):
    # Test different keywords
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="sto-3g",
        keywords={"k": 1.0e-8},
    )

    spec2 = spec1.copy(update={"keywords": {"k": 1.0e-9}})

    spec_test_runner("singlepoint", spec1, spec2, False)


def test_singlepoint_socket_add_specification_diff_5(spec_test_runner):
    # Test different keywords
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="sto-3g",
        keywords={"k": "value"},
    )

    spec2 = spec1.copy(update={"keywords": {"k": "value", "k2": "value2"}})

    spec_test_runner("singlepoint", spec1, spec2, False)


def test_singlepoint_socket_add_specification_diff_6(spec_test_runner):
    # Test different protocols
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    spec2 = spec1.copy(update={"protocols": SinglepointProtocols(stdout=False)})

    spec_test_runner("singlepoint", spec1, spec2, False)


def test_singlepoint_socket_add_specification_diff_7(spec_test_runner):
    # Test different protocols
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
        protocols=SinglepointProtocols(),
    )

    spec2 = spec1.copy(update={"protocols": SinglepointProtocols(wavefunction="orbitals_and_eigenvalues")})

    spec_test_runner("singlepoint", spec1, spec2, False)
