import pytest
import json
from .models import SinglePointSpecification, SinglePointDriver, AtomicResultProtocols


def test_singlepoint_models_lowercase():
    s = SinglePointSpecification(
        program="pROg1",
        driver=SinglePointDriver.energy,
        method="b3LYP",
        basis="def2-TZVP",
    )

    assert s.program == "prog1"
    assert s.method == "b3lyp"
    assert s.basis == "def2-tzvp"


def test_singlepoint_models_basis_convert():
    s = SinglePointSpecification(
        program="prog1",
        driver=SinglePointDriver.energy,
        method="b3lyp",
        basis="",
    )

    assert s.basis is None

    s = SinglePointSpecification(
        program="prog1",
        driver="energy",
        method="b3lyp",
        basis=None,
    )

    assert s.basis is None
