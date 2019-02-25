"""
Tests for QCFractals CLI
"""
import os
import time

import pytest

from qcfractal import testing

from tempfile import TemporaryDirectory


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
    assert testing.run_process(["qcfractal-manager", "--test", "executor"], **_options)


@testing.mark_slow
def test_manager_executor_manager_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "executor", "--nprocs=1"]
    assert testing.run_process(args, interupt_after=7, **_options)


@testing.mark_slow
@testing.using_dask
def test_manager_dask_manager_local_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "dask", "--local-cluster", "--local-workers=1"]
    assert testing.run_process(args, interupt_after=7, **_options)


@testing.mark_slow
@testing.using_fireworks
def test_manager_fireworks_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "fireworks"]
    assert testing.run_process(args, interupt_after=5, **_options)


@testing.mark_slow
@testing.using_fireworks
def test_manager_fireworks_config_boot(active_server):
    config_path = os.path.join(_pwd, "fw_config_boot.yaml")
    args = [
        "qcfractal-manager", active_server.test_uri_cli, "--rapidfire", "--config-file=" + config_path, "fireworks"
    ]
    assert testing.run_process(args, **_options)


@pytest.mark.parametrize("adapter", [
    "dask",
    "parsl",
    pytest.param("fireworks", marks=pytest.mark.xfail),
    pytest.param("executor", marks=pytest.mark.xfail)])
@pytest.mark.parametrize("scheduler", [
    "slurm",
    "pbs",
    "torque",
    "lsf",
    pytest.param("garbage", marks=pytest.mark.xfail)
])
def test_cli_template_generator(adapter, scheduler):

    def ensure_test_is_set_and_exit_head(text: str):
        match_string = "TEST_RUN = True"
        split_lines = text.splitlines()
        for counter, line in enumerate(split_lines):
            if match_string in line:
                # This exits early, before imports and will ensure at least basic syntax is correct
                base = "\n".join(split_lines[:counter+1])
                base += "\nimport sys\nsys.exit()\n"
                base += "\n".join(split_lines[counter:])
                return base
        # Should only reach this if no line found
        raise AssertionError('String "{}" not found in file'.format(match_string))

    def parse_template(path):
        with open(path, 'r') as throwaway:
            new_throw = ensure_test_is_set_and_exit_head(throwaway.read())
        return new_throw

    with TemporaryDirectory() as td:
        throwaway_path = os.path.join(td, "throwaway.py")
        args = ["qcfractal-template", adapter, scheduler, "--test", "-o", throwaway_path]
        try:
            testing.run_process(args, **_options)
        except ValueError:
            return  # Certain not implemented items
        # Ensure bottom of file is valid
        # Known bad combos
        if adapter == "parsl" and scheduler == "lsf":
            pytest.xfail("Parsl has no LSF implementation")
            with pytest.raises(FileNotFoundError):
                new_throw = parse_template(throwaway_path)
        else:
            new_throw = parse_template(throwaway_path)
        new_throw_path = os.path.join(td, "new_throw.py")
        with open(new_throw_path, 'w') as f:
            f.write(new_throw)
        new_args = ["python", new_throw_path]
        assert testing.run_process(new_args, **_options)
