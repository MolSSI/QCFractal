from .models import QCInputSpecification, SinglepointDriver


def test_singlepoint_models_lowercase():
    s = QCInputSpecification(
        program="pROg1",
        driver=SinglepointDriver.energy,
        method="b3LYP",
        basis="def2-TZVP",
    )

    assert s.program == "prog1"
    assert s.method == "b3lyp"
    assert s.basis == "def2-tzvp"


def test_singlepoint_models_basis_convert():
    s = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
    )

    assert s.basis is None

    s = QCInputSpecification(
        program="prog1",
        driver="energy",
        method="b3lyp",
        basis=None,
    )

    assert s.basis is None
