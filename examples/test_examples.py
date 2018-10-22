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
@testing.mark_example
def test_fireworks_server_example():
    """Make sure the Fireworks example works as intended"""

    testing.check_active_mongo_server()

    kwargs = {"dump_stdout": True}

    with testing.preserve_cwd():
        os.chdir(os.path.join(_pwd, "fireworks_server"))
        server_args = ["qcfractal-server", "qca_fw_testing", "--fireworks-manager"]
        with testing.popen(server_args, **kwargs) as server:
            time.sleep(5) # Boot up server

            assert testing.run_process(["python", "build_database.py"], **kwargs)
            assert testing.run_process(["python", "compute_database.py"], **kwargs)

            time.sleep(3) # Ensure tasks are pushed to QueueManager
            assert testing.run_process(["rlaunch", "-l", "fw_lpad.yaml", "rapidfire"], **kwargs)

            time.sleep(3) # Ensure all tasks are gathered
            assert testing.run_process(["python", "query_database.py"], **kwargs)
