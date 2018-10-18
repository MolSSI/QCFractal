"""
Test the examples
"""

import os
import pytest
import time
import subprocess as sp
from qcfractal import testing

_pwd = os.path.abspath(os.path.dirname(__file__))


@testing.using_psi4
@testing.using_fireworks
@testing.using_unix
@pytest.mark.example
def test_fireworks_server_example():
    """Make sure the Fireworks example works as intended"""

    os.chdir(os.path.join(_pwd, "fireworks_server"))
    testing.check_active_mongo_server()

    kwargs = {"dump_stdout": True}

    with testing.popen(["python", "server.py"], **kwargs) as server:
        time.sleep(5) # Boot up server

        assert testing.run_process(["python", "build_database.py"], **kwargs)
        assert testing.run_process(["python", "compute_database.py"], **kwargs)
        assert testing.run_process(["rlaunch", "-l", "fw_lpad.yaml", "rapidfire"], **kwargs, append_prefix=True)

        time.sleep(3) # Ensure all tasks are gathered
        assert testing.run_process(["python", "query_database.py"], **kwargs)

    os.chdir(_pwd)
