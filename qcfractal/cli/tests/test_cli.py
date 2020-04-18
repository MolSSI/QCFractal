"""
Tests for QCFractals CLI
"""
import os
import tempfile
import time
from typing import Any, Dict

import pytest
import yaml

import qcfractal
from qcfractal import testing
from qcfractal.cli.cli_utils import read_config_file

_options = {"coverage": True, "dump_stdout": True}
_pwd = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="module")
def qcfractal_base_init():

    storage = qcfractal.TemporaryPostgres()
    tmpdir = tempfile.TemporaryDirectory()

    args = [
        "qcfractal-server",
        "init",
        "--base-folder",
        str(tmpdir.name),
        "--db-own=False",
        "--clear-database",
        f"--db-port={storage.config.database.port}",
    ]
    assert testing.run_process(args, **_options)

    yield f"--base-folder={tmpdir.name}"


@pytest.mark.slow
def test_cli_server_boot(qcfractal_base_init):
    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "start", qcfractal_base_init, port]
    assert testing.run_process(args, interupt_after=10, **_options)


@pytest.mark.slow
def test_cli_upgrade(qcfractal_base_init):
    args = ["qcfractal-server", "upgrade", qcfractal_base_init]
    assert testing.run_process(args, interupt_after=10, **_options)


@pytest.mark.slow
def test_cli_user_add(qcfractal_base_init):
    args = ["qcfractal-server", "user", qcfractal_base_init, "add", "test_user_add_1", "--permissions", "admin"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "add", "test_user_add_1", "--permissions", "admin"]
    assert testing.run_process(args, **_options) is False

    args = [
        "qcfractal-server",
        "user",
        qcfractal_base_init,
        "add",
        "test_user_add_2",
        "--password",
        "foo",
        "--permissions",
        "admin",
    ]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "add", "test_user_add_3"]
    assert testing.run_process(args, **_options) is False


@pytest.mark.slow
def test_cli_user_show(qcfractal_base_init):
    args = ["qcfractal-server", "user", qcfractal_base_init, "add", "test_user_show", "--permissions", "admin"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "info", "test_user_show"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "info", "badname_1234"]
    assert testing.run_process(args, **_options) is False


@pytest.mark.slow
def test_cli_user_modify(qcfractal_base_init):
    args = ["qcfractal-server", "user", qcfractal_base_init, "add", "test_user_modify", "--permissions", "read"]
    assert testing.run_process(args, **_options)

    args = [
        "qcfractal-server",
        "user",
        qcfractal_base_init,
        "modify",
        "test_user_modify",
        "--permissions",
        "read",
        "write",
        "--reset-password",
    ]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "modify", "test_user_modify", "--password", "foopass"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "modify", "test_user_modify", "--permissions", "read"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "modify", "badname_1234"]
    assert testing.run_process(args, **_options) is False


@pytest.mark.slow
def test_cli_user_remove(qcfractal_base_init):
    args = ["qcfractal-server", "user", qcfractal_base_init, "add", "test_user_remove", "--permissions", "admin"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "remove", "test_user_remove"]
    assert testing.run_process(args, **_options)

    args = ["qcfractal-server", "user", qcfractal_base_init, "remove", "badname_1234"]
    assert testing.run_process(args, **_options) is False


@pytest.mark.xfail(reason="Failing on Travis for unknown reasons.")
@pytest.mark.slow
def test_cli_server_local_boot(qcfractal_base_init):
    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "start", "--local-manager=1", port, qcfractal_base_init]
    assert testing.run_process(args, interupt_after=10, **_options)


@pytest.fixture(scope="module")
def active_server(request, qcfractal_base_init):
    port = str(testing.find_open_port())
    args = ["qcfractal-server", "start", qcfractal_base_init, f"--port={port}"]
    assert testing.run_process(args, interupt_after=10, **_options)
    with testing.popen(args, **_options) as server:
        time.sleep(2)

        server.test_uri_cli = "--fractal-uri=localhost:" + port
        yield server


@pytest.mark.slow
@pytest.mark.parametrize("log_apis", [0, 1])
def test_with_api_logging(postgres_server, log_apis):

    tmpdir = tempfile.TemporaryDirectory()

    args = [
        "qcfractal-server",
        "init",
        "--base-folder",
        str(tmpdir.name),
        "--db-own=False",
        "--clear-database",
        f"--db-port={postgres_server.config.database.port}",
        f"--log-apis={log_apis}",
    ]
    assert testing.run_process(args, **_options)

    port = "--port=" + str(testing.find_open_port())
    args = ["qcfractal-server", "start", f"--base-folder={tmpdir.name}", port]
    assert testing.run_process(args, interupt_after=10, **_options)


@pytest.mark.slow
def test_manager_local_testing_process():
    assert testing.run_process(["qcfractal-manager", "--adapter=pool", "--test", "--tasks-per-worker=2"], **_options)


@pytest.mark.slow
def test_manager_executor_manager_boot(active_server):
    args = ["qcfractal-manager", active_server.test_uri_cli, "--adapter=pool", "--tasks-per-worker=2", "--verify=False"]
    assert testing.run_process(args, interupt_after=7, **_options)


@pytest.mark.slow
def test_manager_executor_manager_boot_from_file(active_server, tmp_path):

    yaml_file = """
    common:
        adapter: pool
        tasks_per_worker: 4
        cores_per_worker: 4
    server:
        fractal_uri: {}
        verify: False
    """.format(
        active_server.test_uri_cli.split("=")[1]
    )

    p = tmp_path / "config.yaml"
    p.write_text(yaml_file)

    args = ["qcfractal-manager", "--config-file={}".format(p)]
    assert testing.run_process(args, interupt_after=7, **_options)


@pytest.mark.slow
def cli_manager_runs(config_data, tmp_path):
    temp_config = tmp_path / "temp_config.yaml"
    temp_config.write_text(yaml.dump(config_data))
    args = ["qcfractal-manager", f"--config-file={temp_config}", "--test"]
    assert testing.run_process(args, **_options)


@pytest.mark.slow
def load_manager_config(adapter, scheduler) -> Dict[str, Any]:
    config = read_config_file(os.path.join(_pwd, "manager_boot_template.yaml"))
    config["common"]["adapter"] = adapter
    config["cluster"]["scheduler"] = scheduler
    return config


@pytest.mark.slow
@pytest.mark.parametrize(
    "adapter,scheduler",
    [
        ("pool", "slurm"),
        pytest.param("dask", "slurm", marks=testing.using_dask_jobqueue),
        pytest.param("dask", "PBS", marks=testing.using_dask_jobqueue),
        pytest.param("dask", "MoAb", marks=testing.using_dask_jobqueue),
        pytest.param("dask", "SGE", marks=testing.using_dask_jobqueue),
        pytest.param("dask", "lSf", marks=testing.using_dask_jobqueue),
        pytest.param("parsl", "slurm", marks=testing.using_parsl),
        pytest.param("parsl", "PBS", marks=testing.using_parsl),
        pytest.param("parsl", "MoAb", marks=testing.using_parsl),
        pytest.param("parsl", "SGE", marks=testing.using_parsl),
        pytest.param("parsl", "lSf", marks=[testing.using_parsl, pytest.mark.xfail]),  # Invalid combination
        pytest.param("NotAParser", "slurm", marks=pytest.mark.xfail),  # Invalid Parser
        pytest.param("pool", "NotAScheduler", marks=pytest.mark.xfail),  # Invalid Scheduler
    ],
)
def test_cli_managers(adapter, scheduler, tmp_path):
    """Test that multiple adapter/scheduler combinations at least can boot up in Managers"""
    config = load_manager_config(adapter, scheduler)
    cli_manager_runs(config, tmp_path)


@pytest.mark.slow
@testing.using_parsl
def test_cli_manager_parsl_launchers(tmp_path):
    config = load_manager_config("parsl", "slurm")
    config["parsl"]["provider"].update({"launcher": {"launcher_class": "singleNODELauncher"}})
    cli_manager_runs(config, tmp_path)


@pytest.mark.slow
@pytest.mark.parametrize(
    "adapter",
    [pytest.param("dask", marks=testing.using_dask_jobqueue), pytest.param("parsl", marks=testing.using_parsl)],
)
def test_cli_managers_missing(adapter, tmp_path):
    """Test that the manager block missing correctly sets defaults"""
    config = load_manager_config(adapter, "slurm")
    config.pop(adapter, None)
    cli_manager_runs(config, tmp_path)


@pytest.mark.slow
@pytest.mark.parametrize(
    "adapter",
    [pytest.param("dask", marks=testing.using_dask_jobqueue), pytest.param("parsl", marks=testing.using_parsl)],
)
def test_cli_managers_none(adapter, tmp_path):
    """Test that manager block set to None correctly assigns the defaults"""
    config = load_manager_config(adapter, "slurm")
    config[adapter] = None
    cli_manager_runs(config, tmp_path)


@pytest.mark.slow
def test_cli_managers_help():
    """Test that qcfractal_manager --help works"""
    args = ["qcfractal-manager", "--help"]
    testing.run_process(args, **_options)


@pytest.mark.slow
def test_cli_managers_schema():
    """Test that qcfractal_manager --schema works"""
    args = ["qcfractal-manager", "--schema"]
    testing.run_process(args, **_options)


@pytest.mark.slow
def test_cli_managers_skel(tmp_path):
    """Test that qcfractal_manager --skeleton works"""
    config = tmp_path / "config.yaml"
    args = ["qcfractal-manager", "--skel", config.as_posix()]
    testing.run_process(args, **_options)


@testing.using_parsl
def test_nodeparallel_tasks(tmp_path):
    """Make sure that it boots up properly"""
    config = load_manager_config("parsl", "cobalt")
    config["common"]["nodes_per_task"] = 2
    config["common"]["nodes_per_job"] = 2
    config["common"]["cores_per_rank"] = 2
    cli_manager_runs(config, tmp_path)
