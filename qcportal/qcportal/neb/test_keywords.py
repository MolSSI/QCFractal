"""
Tests validation of NEBKeywords numerical controls.

Pure unit tests (no server/snowflake needed) guarding issue #1069:
non-finite or non-positive numerical controls must be rejected at
construction time, while valid finite settings keep working.
"""

import math

import pytest
from pydantic import ValidationError

from qcportal.neb.record_models import NEBKeywords


@pytest.mark.parametrize(
    "kwargs",
    [
        {"spring_constant": math.nan},
        {"spring_constant": math.inf},
        {"spring_constant": -math.inf},
        {"spring_constant": -1.0},
        {"spring_constant": 0.0},
        {"maximum_force": math.nan},
        {"maximum_force": math.inf},
        {"maximum_force": -1.0},
        {"maximum_force": 0.0},
        {"average_force": math.nan},
        {"average_force": math.inf},
        {"average_force": -1.0},
        {"average_force": 0.0},
        {"maximum_cycle": -1},
        {"maximum_cycle": 0},
        {"epsilon": math.nan},
        {"epsilon": math.inf},
        {"epsilon": -1.0},
        {"epsilon": 0.0},
    ],
)
def test_neb_keywords_rejects_invalid_controls(kwargs):
    with pytest.raises(ValidationError):
        NEBKeywords(**kwargs)


def test_neb_keywords_accepts_valid_controls():
    # Defaults
    keywords = NEBKeywords()
    assert keywords.spring_constant == 1.0
    assert keywords.maximum_force == 0.05
    assert keywords.average_force == 0.025
    assert keywords.maximum_cycle == 100
    assert keywords.epsilon == 1e-5

    # Explicit finite, positive settings
    keywords = NEBKeywords(
        spring_constant=2.5,
        maximum_force=0.01,
        average_force=0.01,
        maximum_cycle=50,
        epsilon=1e-6,
    )
    assert keywords.spring_constant == 2.5
    assert keywords.maximum_force == 0.01
    assert keywords.average_force == 0.01
    assert keywords.maximum_cycle == 50
    assert keywords.epsilon == 1e-6
