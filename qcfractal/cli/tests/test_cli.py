"""
Tests for QCFractals CLI
"""
import argparse
import os
import pytest
import time

import qcfractal
from qcfractal import testing

#def _run_tests()
_options = {"coverage": True, "dump_stdout": True}
_pwd = os.path.dirname(os.path.abspath(__file__))

@testing.mark_slow
def test_cli_server_boot():
    port = "--port=" + str(testing.find_open_port())
    assert testing.run_process(["qcfractal-server", "mydb", port], terminate_after=2, **_options)

@testing.mark_slow
def test_cli_server_fireworks_boot():
    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "mydb", "--fireworks-manager", port]
    assert testing.run_process(args, terminate_after=2, **_options)

@testing.mark_slow
def test_cli_server_dask_boot():
    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "mydb", "--dask-manager", port]
    assert testing.run_process(args, terminate_after=5, **_options)


@pytest.fixture(scope="module")
def active_server(request):
    port = str(testing.find_open_port())
    args = ["qcfractal-server", "mydb", "--port=" + port]
    with testing.popen(args, **_options) as server:
        time.sleep(2)

        server.test_uri_cli = "--fractal-uri=localhost:" + port
        yield server

@testing.mark_slow
def test_cli_server_dask_manager_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "dask", "--local-cluster"]
    assert testing.run_process(args, terminate_after=5, **_options)

@testing.mark_slow
def test_manager_fireworks_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "fireworks"]
    assert testing.run_process(args, terminate_after=2, **_options)

@testing.mark_slow
def test_manager_fireworks_config_boot(active_server):
    config_path = os.path.join(_pwd, "fw_config_boot.yaml")
    args = ["qcfractal-manager", active_server.test_uri_cli, "--rapidfire", "--config-file=" + config_path, "fireworks"]
    assert testing.run_process(args, **_options)
