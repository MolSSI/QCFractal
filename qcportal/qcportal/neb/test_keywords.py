"""
Tests for NEBKeywords numerical-control validation, scoped to new submissions.

Per issue #1069 and the review discussion on PR #1073, the finite/positive
checks live on the submission models (NEBInput / NEBMultiInput, and
NEBAddBody through inheritance), so new submissions with malformed
controls are rejected early. NEBKeywords itself stays permissive, so
already-stored records (e.g. with epsilon=0) keep loading.

Pure unit tests (no server/snowflake needed).
"""

import math
from datetime import datetime

import pytest
from pydantic import ValidationError

from qcportal.neb.record_models import (
    NEBAddBody,
    NEBInput,
    NEBKeywords,
    NEBMultiInput,
    NEBRecord,
    NEBSpecification,
)
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.singlepoint.record_models import QCSpecification


def _valid_qc_spec():
    return QCSpecification(
        program="psi4", driver="energy", method="b3lyp", basis="sto-3g"
    )


def _spec_with_keywords(**kwargs):
    return NEBSpecification(
        singlepoint_specification=_valid_qc_spec(), keywords=NEBKeywords(**kwargs)
    )


def _neb_input(**kwargs):
    return NEBInput(specification=_spec_with_keywords(**kwargs), initial_chain=[1, 2])


def _neb_multi_input(**kwargs):
    return NEBMultiInput(
        specification=_spec_with_keywords(**kwargs), initial_chains=[[1, 2]]
    )


def _neb_add_body(**kwargs):
    return NEBAddBody(
        specification=_spec_with_keywords(**kwargs),
        initial_chains=[[1, 2]],
        compute_tag="test",
        compute_priority=PriorityEnum.normal,
    )


def _stored_neb_record(**kwargs):
    """A record as loaded from storage, with possibly out-of-range controls."""
    return NEBRecord(
        id=1,
        record_type="neb",
        is_service=False,
        properties={},
        status=RecordStatusEnum.complete,
        manager_name=None,
        created_on=datetime(2024, 1, 1),
        modified_on=datetime(2024, 1, 1),
        owner_user=None,
        specification=_spec_with_keywords(**kwargs),
    )


INVALID_CONTROLS = [
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
]


@pytest.mark.parametrize("kwargs", INVALID_CONTROLS)
def test_neb_input_rejects_invalid_controls(kwargs):
    with pytest.raises(ValidationError):
        _neb_input(**kwargs)


@pytest.mark.parametrize(
    "kwargs", [{"epsilon": 0.0}, {"maximum_cycle": 0}, {"spring_constant": math.nan}]
)
def test_neb_multi_input_rejects_invalid_controls(kwargs):
    with pytest.raises(ValidationError):
        _neb_multi_input(**kwargs)


def test_neb_add_body_rejects_invalid_controls():
    with pytest.raises(ValidationError):
        _neb_add_body(epsilon=0.0)


@pytest.mark.parametrize("kwargs", INVALID_CONTROLS)
def test_neb_keywords_accepts_invalid_controls(kwargs):
    # NEBKeywords stays permissive: stored/loaded values are not rejected here.
    keywords = NEBKeywords(**kwargs)
    field, expected = next(iter(kwargs.items()))
    actual = getattr(keywords, field)
    if isinstance(expected, float) and math.isnan(expected):
        assert math.isnan(actual)
    else:
        assert actual == expected


@pytest.mark.parametrize("kwargs", [{"epsilon": 0.0}, {"maximum_cycle": 0}])
def test_neb_record_loads_stored_invalid_controls(kwargs):
    # A stored record with out-of-range controls must still load.
    record = _stored_neb_record(**kwargs)
    field, expected = next(iter(kwargs.items()))
    assert getattr(record.specification.keywords, field) == expected


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


def test_neb_input_accepts_valid_controls():
    neb_input = _neb_input(
        spring_constant=2.5,
        maximum_force=0.01,
        average_force=0.01,
        maximum_cycle=50,
        epsilon=1e-6,
    )
    assert neb_input.specification.keywords.spring_constant == 2.5
    assert neb_input.specification.keywords.maximum_cycle == 50
