"""
Test the examples
"""

import os
import time
from qcfractal import testing

_pwd = os.path.abspath(os.path.dirname(__file__))


@testing.using_psi4
@testing.using_unix
@testing.mark_example
def test_local_server_example():
    """Make sure the Fireworks example works as intended"""

    testing.check_active_mongo_server()

    kwargs = {"dump_stdout": True}

    with testing.preserve_cwd():
        os.chdir(os.path.join(_pwd, "local_dataset"))
        server_args = ["qcfractal-server", "qca_local_testing", "--local-manager"]
        with testing.popen(server_args, **kwargs) as server:
            time.sleep(5)  # Boot up server

            assert testing.run_process(["python", "build_database.py"], **kwargs)
            assert testing.run_process(["python", "compute_database.py"], **kwargs)

            time.sleep(8)  # Ensure all tasks are completed
            assert testing.run_process(["python", "query_database.py"], **kwargs)


@testing.using_parsl
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