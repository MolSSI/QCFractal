"""
Tests for QCFractals CLI
"""
import ast
import os
import time

import pytest

from qcfractal import testing
from qcfractal.cli.cli_utils import read_config_file
import yaml

# def _run_tests()
_options = {"coverage": True, "dump_stdout": True}
_pwd = os.path.dirname(os.path.abspath(__file__))


@testing.mark_slow
def test_cli_server_boot():
    port = "--port=" + str(testing.find_open_port())
    assert testing.run_process(["qcfractal-server", "mydb", port], interupt_after=10, **_options)


@pytest.mark.skip(reason="Odd travis issue. TODO")
@testing.mark_slow
def test_cli_server_local_boot():
    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "mydb", "--local-manager=2", port]
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
    assert testing.run_process(["qcfractal-manager", "--adapter=pool", "--test", "--tasks_per_worker=2"], **_options)


@testing.mark_slow
def test_manager_executor_manager_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "--adapter=pool", "--tasks_per_worker=2", "--verify=False"]
    assert testing.run_process(args, interupt_after=7, **_options)


@testing.mark_slow
def test_manager_executor_manager_boot_from_file(active_server, tmp_path):

    yaml_file = """
    common:
        adapter: pool
        tasks_per_worker: 4
        cores_per_worker: 4
    server:
        fractal_uri: {}
        verify: False
    """.format(active_server.test_uri_cli.split("=")[1])

    p = tmp_path / "config.yaml"
    p.write_text(yaml_file)

    args = ["qcfractal-manager", "--config-file={}".format(p)]
    assert testing.run_process(args, interupt_after=7, **_options)


def cli_manager_runs(config_data, tmp_path):
    temp_config = tmp_path / "temp_config.yaml"
    with open(temp_config, 'w') as config:
        config.write(yaml.dump(config_data))
    args = ["qcfractal-manager", f"--config-file={temp_config}", "--test"]
    try:
        assert testing.run_process(args)
    except AssertionError:
        # Dump stdout when slow to better debug
        assert testing.run_process(args, **_options)


@testing.mark_slow
@testing.using_dask_jobqueue
@testing.using_parsl
@pytest.mark.parametrize("adapter,scheduler", [
    ("pool", "slurm"),
    ("dask", "slurm"),
    ("dask", "PBS"),
    ("dask", "MoAb"),
    ("dask", "SGE"),
    ("dask", "lSf"),
    ("parsl", "slurm"),
    ("parsl", "PBS"),
    ("parsl", "MoAb"),
    ("parsl", "SGE"),
    pytest.param("parsl", "lSf", marks=pytest.mark.xfail),
    pytest.param("NotAParser", "slurm", marks=pytest.mark.xfail),
    pytest.param("dask", "NotAScheduler", marks=pytest.mark.xfail),
])
def test_cli_managers(adapter, scheduler, tmp_path):
    """Test that multiple adapter/scheduler combinations at least can boot up in Managers"""
    config = read_config_file(os.path.join(_pwd, "manager_boot_template.yaml"))
    config["common"]["adapter"] = adapter
    config["cluster"]["scheduler"] = scheduler
    # Make sure this runs
    cli_manager_runs(config, tmp_path)
    # Try removing the scheduler block
    config_present = config.pop(adapter, None)
    cli_manager_runs(config, tmp_path)
    # Finally, try setting scheduler block to None to check a corner case
    if config_present is not None:
        config[adapter] = None


def test_cli_managers_quick_exits():
    """Test that --help and --schema correctly work"""
    args = ["qcfractal-manager", "--help"]
    testing.run_process(args)
    args = ["qcfractal-manager", "--schema"]
    testing.run_process(args)
