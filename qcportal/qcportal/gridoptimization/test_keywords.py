"""
Tests validation of ScanDimension steps and atom indices.

Pure unit tests (no server/snowflake needed) guarding issue #1071:
empty step lists and repeated/negative atom indices must be rejected at
construction time, while valid monotonic scan definitions keep working.
"""

import pytest
from pydantic import ValidationError

from qcportal.gridoptimization.record_models import ScanDimension


@pytest.mark.parametrize(
    "kwargs",
    [
        # Empty steps: would produce an empty grid
        {"type": "distance", "indices": [0, 1], "steps": [], "step_type": "absolute"},
        # Repeated atom indices do not identify a valid coordinate
        {
            "type": "distance",
            "indices": [0, 0],
            "steps": [1.0, 2.0],
            "step_type": "absolute",
        },
        {
            "type": "angle",
            "indices": [0, 1, 1],
            "steps": [1.0, 2.0],
            "step_type": "absolute",
        },
        {
            "type": "dihedral",
            "indices": [0, 1, 2, 2],
            "steps": [1.0, 2.0],
            "step_type": "absolute",
        },
        # Negative atom indices do not identify a valid coordinate
        {
            "type": "distance",
            "indices": [-1, 1],
            "steps": [1.0, 2.0],
            "step_type": "absolute",
        },
    ],
)
def test_scan_dimension_rejects_invalid_steps_and_indices(kwargs):
    with pytest.raises(ValidationError):
        ScanDimension(**kwargs)


def test_scan_dimension_accepts_valid_scans():
    # Increasing absolute steps
    dim = ScanDimension(
        type="distance", indices=[0, 1], steps=[1.0, 2.0], step_type="absolute"
    )
    assert dim.indices == [0, 1]
    assert dim.steps == [1.0, 2.0]

    # Decreasing steps are still strictly monotonic and therefore valid
    dim = ScanDimension(
        type="distance", indices=[0, 1], steps=[2.0, 1.0], step_type="absolute"
    )
    assert dim.steps == [2.0, 1.0]

    # Relative steps may decrease as well
    dim = ScanDimension(
        type="angle", indices=[0, 1, 2], steps=[5.0, -5.0], step_type="relative"
    )
    assert dim.steps == [5.0, -5.0]

    # A single step is trivially monotonic
    dim = ScanDimension(
        type="dihedral", indices=[0, 1, 2, 3], steps=[10.0], step_type="absolute"
    )
    assert dim.steps == [10.0]
