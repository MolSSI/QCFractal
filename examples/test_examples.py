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
        os.chdir(os.path.join(_pwd, "fireworks_dataset"))
        server_args = ["qcfractal-server", "qca_fw_testing", "--fireworks-manager"]
        with testing.popen(server_args, **kwargs) as server:
            time.sleep(5)  # Boot up server

            assert testing.run_process(["python", "build_database.py"], **kwargs)
            assert testing.run_process(["python", "compute_database.py"], **kwargs)

            time.sleep(3)  # Ensure tasks are pushed to QueueManager
            assert testing.run_process(["rlaunch", "-l", "fw_lpad.yaml", "rapidfire"], **kwargs)

            time.sleep(3)  # Ensure all tasks are gathered
            assert testing.run_process(["python", "query_database.py"], **kwargs)


@testing.using_geometric
@testing.using_torsiondrive
@testing.using_rdkit
@testing.using_unix
@testing.mark_example
def test_parsl_server_example():
    """Make sure the Parsl example works as intended"""

    testing.check_active_mongo_server()

    kwargs = {"dump_stdout": True}

    with testing.preserve_cwd():
        os.chdir(os.path.join(_pwd, "parsl_torsiondrive"))
        server_args = ["qcfractal-server", "qca_parsl_testing"]
        with testing.popen(server_args, **kwargs) as server:
            time.sleep(5)  # Boot up server

            manager_args = ["python", "parsl_manager.py"]
            with testing.popen(manager_args, **kwargs) as manager:

                assert testing.run_process(["python", "compute_torsion.py"], **kwargs)
                time.sleep(30)  # Ensure all tasks are gathered

                assert testing.run_process(["python", "query_torsion.py"], **kwargs)