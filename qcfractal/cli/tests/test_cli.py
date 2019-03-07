"""
Tests for QCFractals CLI
"""
import ast
import os
import time

import pytest

from qcfractal import testing

# def _run_tests()
_options = {"coverage": True, "dump_stdout": True}
_pwd = os.path.dirname(os.path.abspath(__file__))


@testing.mark_slow
def test_cli_server_boot():
    port = "--port=" + str(testing.find_open_port())
    assert testing.run_process(["qcfractal-server", "mydb", port], interupt_after=10, **_options)


@testing.mark_slow
def test_cli_server_local_boot():
    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "mydb", "--local-manager", port]
    assert testing.run_process(args, interupt_after=10, **_options)


@pytest.fixture(scope="module")
def active_server(request):
    port = str(testing.find_open_port())
    args = ["qcfractal-server", "mydb", "--port=" + port]
    with testing.popen(args, **_options) as server:
        time.sleep(2)

        server.test_uri_cli = "--fractal-uri=localhost:" + port
        yield server


@testing.mark_slow
def test_manager_local_testing_process():
    assert testing.run_process(["qcfractal-manager", "--test", "--ntasks=2"], **_options)


@testing.mark_slow
def test_manager_executor_manager_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "--ntasks=1"]
    assert testing.run_process(args, interupt_after=7, **_options)


@testing.mark_slow
@pytest.mark.parametrize(
    "adapter",
    [
        "dask",
        "parsl",
        # pytest.param("fireworks", marks=pytest.mark.xfail),
        # pytest.param("executor", marks=pytest.mark.xfail)
    ])
@pytest.mark.parametrize("scheduler",
                         ["slurm", "pbs", "torque", "lsf",
                          pytest.param("garbage", marks=pytest.mark.xfail)])
def test_cli_template_generator(adapter, scheduler, tmp_path):
    if adapter == "parsl" and scheduler == "lsf":
        pytest.xfail("Parsl has no LSF implementation")

    tmpl_path = tmp_path / "tmp_template.py"
    args = ["qcfractal-template", adapter, scheduler, "--test", "-o", str(tmpl_path)]
    testing.run_process(args)

    with open(tmpl_path, 'r') as handle:
        data = handle.read()

    # Will throw a syntax error if incorrect
    c = ast.parse(data)
