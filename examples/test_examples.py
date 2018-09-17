"""
Test the examples
"""

import re
import pytest
import subprocess as sp
from qcfractal import testing


@testing.using_fireworks
@testing.using_unix
@pytest.mark.slow
def test_fireworks_example():
    """Make sure the Fireworks example works as intended"""
    output = sp.check_output(["bash", "run_fireworks_example.sh"], shell=False)
    output_lines = output.decode().split('\n')  # Split up the output
    expected = {
        'Water Dimer': -1.392710,
        'Water Dimer Stretch': 0.037144,
        'Helium Dimer': -0.003148
    }
    for index, line in enumerate(output_lines):
        try:
            split = re.split(r"([-.0-9]+$)", line)  # Split the line around what we expect
            expected_value = expected[split[0].strip()]  # use first column as key
            assert expected_value == float(split[1])  # Check that output is expected value
        except (KeyError, IndexError):
            # Skip over lines which are not in the right format
            pass
