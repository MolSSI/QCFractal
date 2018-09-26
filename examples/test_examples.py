"""
Test the examples
"""

import os
import pytest
import subprocess as sp
from qcfractal import testing


def _run_command(folder, script):
    # Get the examples director
    root = os.path.abspath(os.path.dirname(__file__))
    example_path = os.path.join(root, folder)
    os.chdir(example_path)

    error = False
    try:
        output = sp.check_output(["bash", script], shell=False)
    except sp.CalledProcessError as e:
        output = e.output
        error = True

    os.chdir(root)
    if error:
        msg = "Example {} failed. Output as follows\n\n".format(folder)
        msg += output.decode()
        raise SystemError(msg)

    return not error

@testing.using_psi4
@testing.using_fireworks
@testing.using_unix
@pytest.mark.example
def test_fireworks_server_example():
    """Make sure the Fireworks example works as intended"""

    assert _run_command("fireworks_server", "run_fireworks_example.sh")


