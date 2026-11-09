"""
Tests for TorsiondriveKeywords scan-domain validation, scoped to new submissions.

Per issue #1070 and the review discussion on PR #1073 (Kamran's preference),
the scan-domain checks live on the submission models (TorsiondriveInput /
TorsiondriveMultiInput, and TorsiondriveAddBody through inheritance), so new
submissions with malformed scan domains are rejected early.
TorsiondriveKeywords itself stays permissive, so already-stored records keep
loading.

Pure unit tests (no server/snowflake needed).
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from qcportal.optimization.record_models import OptimizationSpecification
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.singlepoint.record_models import QCSpecification
from qcportal.torsiondrive.record_models import (
    TorsiondriveAddBody,
    TorsiondriveInput,
    TorsiondriveKeywords,
    TorsiondriveMultiInput,
    TorsiondriveRecord,
    TorsiondriveSpecification,
)


def _valid_qc_spec():
    return QCSpecification(
        program="psi4", driver="energy", method="b3lyp", basis="sto-3g"
    )


def _spec_with_keywords(**kwargs):
    return TorsiondriveSpecification(
        optimization_specification=OptimizationSpecification(
            program="geometric", qc_specification=_valid_qc_spec()
        ),
        keywords=TorsiondriveKeywords(**kwargs),
    )


def _td_input(**kwargs):
    return TorsiondriveInput(
        specification=_spec_with_keywords(**kwargs),
        initial_molecules=[1, 2],
        as_service=False,
    )


def _td_multi_input(**kwargs):
    return TorsiondriveMultiInput(
        specification=_spec_with_keywords(**kwargs),
        initial_molecules=[[1, 2]],
        as_service=False,
    )


def _td_add_body(**kwargs):
    return TorsiondriveAddBody(
        specification=_spec_with_keywords(**kwargs),
        initial_molecules=[[1, 2]],
        as_service=False,
        compute_tag="test",
        compute_priority=PriorityEnum.normal,
    )


def _stored_td_record(**kwargs):
    """A record as loaded from storage, with possibly out-of-range keywords."""
    return TorsiondriveRecord(
        id=1,
        record_type="torsiondrive",
        is_service=False,
        properties={},
        status=RecordStatusEnum.complete,
        manager_name=None,
        created_on=datetime(2024, 1, 1),
        modified_on=datetime(2024, 1, 1),
        owner_user=None,
        specification=_spec_with_keywords(**kwargs),
    )


# The five invalid inputs from issue #1070.
INVALID_KEYWORDS = [
    {"grid_spacing": [0]},
    {"grid_spacing": [-15]},
    {"dihedral_ranges": [(10, 0)]},
    {"dihedral_ranges": [(0, 0)]},
    {"dihedrals": [(0, 1, 2, 2)]},
]


@pytest.mark.parametrize("kwargs", INVALID_KEYWORDS)
def test_td_input_rejects_invalid_keywords(kwargs):
    with pytest.raises(ValidationError):
        _td_input(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"grid_spacing": [0]},
        {"dihedral_ranges": [(10, 0)]},
        {"dihedrals": [(0, 1, 2, 2)]},
    ],
)
def test_td_multi_input_rejects_invalid_keywords(kwargs):
    with pytest.raises(ValidationError):
        _td_multi_input(**kwargs)


def test_td_add_body_rejects_invalid_keywords():
    with pytest.raises(ValidationError):
        _td_add_body(grid_spacing=[-15])


@pytest.mark.parametrize("kwargs", INVALID_KEYWORDS)
def test_td_keywords_accepts_invalid_keywords(kwargs):
    # TorsiondriveKeywords stays permissive: stored/loaded values are not rejected here.
    keywords = TorsiondriveKeywords(**kwargs)
    field, expected = next(iter(kwargs.items()))
    assert getattr(keywords, field) == expected


@pytest.mark.parametrize(
    "kwargs",
    [
        {"grid_spacing": [0]},
        {"dihedral_ranges": [(0, 0)]},
        {"dihedrals": [(0, 1, 2, 2)]},
    ],
)
def test_td_record_loads_stored_invalid_keywords(kwargs):
    # A stored record with out-of-range keywords must still load.
    record = _stored_td_record(**kwargs)
    field, expected = next(iter(kwargs.items()))
    assert getattr(record.specification.keywords, field) == expected


def test_td_keywords_accepts_valid_keywords():
    # Defaults
    keywords = TorsiondriveKeywords()
    assert keywords.grid_spacing == []
    assert keywords.dihedral_ranges is None
    assert keywords.dihedrals == []

    # Explicit valid settings (the control case from issue #1070)
    keywords = TorsiondriveKeywords(
        grid_spacing=[15],
        dihedral_ranges=[(-180, 180)],
        dihedrals=[(0, 1, 2, 3)],
    )
    assert keywords.grid_spacing == [15]
    assert keywords.dihedral_ranges == [(-180, 180)]
    assert keywords.dihedrals == [(0, 1, 2, 3)]


def test_td_input_accepts_valid_keywords():
    td_input = _td_input(
        grid_spacing=[15],
        dihedral_ranges=[(-180, 180)],
        dihedrals=[(0, 1, 2, 3)],
    )
    assert td_input.specification.keywords.grid_spacing == [15]
    assert td_input.specification.keywords.dihedral_ranges == [(-180, 180)]
