import hashlib
import os
import shutil
import subprocess
import time
from typing import List, Optional

import pytest
import yaml

from qcarchivetesting import testconfig_path, migrationdata_path
from qcfractal.config import read_configuration
from qcfractal.port_util import find_open_port

config_file = os.path.join(testconfig_path, "qcf_basic.yaml")
old_config_file = os.path.join(migrationdata_path, "qcfractal_config_v0.15.8.yaml")
old_db_dump = os.path.join(migrationdata_path, "qcfractal_config_v0.15.8.yaml")

pytestmark = pytest.mark.slow


@pytest.fixture(scope="function")
def unique_db_name(request):
    # Use a hash of the node (which is unique in pytest)
    # Prefix with qcf_ to make sure it is a valid dbname
    yield f"qcf_{hashlib.sha1(request.node.nodeid.encode()).hexdigest()}"


@pytest.fixture(scope="function")
def tmp_config(tmp_path, unique_db_name):
    full_config_path = os.path.join(tmp_path, os.path.basename(config_file))

    # inject the unique database name (unique per test)
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    config["database"]["database_name"] = unique_db_name
    config["database"]["port"] = find_open_port()
    config["api"]["port"] = find_open_port()

    with open(full_config_path, "w") as f:
        yaml.dump(config, f)

    yield full_config_path


# Test with both owning the database and not owning it
@pytest.fixture(scope="function", params=[True, False])
def cli_runner_core(postgres_server, tmp_config, request, unique_db_name):

    pg_harness = None
    own_db = request.param

    if not own_db:
        # Unique database for each test
        # This is needed when running tests in parallel
        pg_harness = postgres_server.get_new_harness(unique_db_name)

        # Don't actually use the database - we just want a placeholder
        pg_harness.delete_database()

        config = read_configuration([tmp_config])
        config.database = pg_harness.config.copy(deep=True)
        config.database.own = False

        with open(tmp_config, "w") as f:
            yaml.dump(config.dict(), f)

    # Use a functor so we can get own_db (and maybe other info in the future)
    class run_qcfractal_cli:
        def __init__(self):
            self.own_db = own_db
            self.db_name = unique_db_name
            self.config_path = tmp_config

        def __call__(
            self,
            cmd_args: List[str],
            *,
            fail_expected=False,
            timeout_expected=False,
            stdin: Optional[str] = None,
            timeout: Optional[int] = None,
        ):
            full_cmd = ["qcfractal-server", "-v", "--config", self.config_path] + cmd_args
            try:
                ret = subprocess.run(full_cmd, capture_output=True, text=True, input=stdin, timeout=timeout)
            except subprocess.TimeoutExpired as err:
                if timeout_expected:
                    return err.output
                else:
                    raise

            if ret.returncode == 0 and not fail_expected:
                return ret.stdout + ret.stderr
            if ret.returncode != 0 and fail_expected:
                return ret.stdout + ret.stderr
            if ret.returncode == 0 and fail_expected:
                raise RuntimeError("Failure expected, but process succeeded")
            if ret.returncode != 0 and not fail_expected:
                raise RuntimeError("Error running process:\n" + ret.stdout + "\n" + ret.stderr)

    try:
        yield run_qcfractal_cli()
    finally:
        if pg_harness is not None:
            pg_harness.delete_database()


@pytest.fixture(scope="function")
def cli_runner(cli_runner_core):
    # Creates the postgres instance (if needed) and tables
    cli_runner_core(["init-db"])

    yield cli_runner_core


def test_cli_help():
    subprocess.check_output(["qcfractal-server", "--help"])


def test_cli_info_config(cli_runner):
    output = cli_runner(["info", "server"])
    assert "THISISASECRETKEY" in output
    assert "QCFractal Test Server" in output


def test_cli_info_alembic(cli_runner_core):
    output = cli_runner_core(["info", "alembic"])
    assert "alembic -c" in output


@pytest.mark.parametrize("full", [True, False])
def test_cli_init_config(cli_runner_core, full: bool):
    # Remove the old config
    os.remove(cli_runner_core.config_path)

    cmd = ["init-config"]
    if full:
        cmd.append("--full")

    output = cli_runner_core(cmd)
    assert "Creating initial QCFractal configuration" in output

    # Replace the port in the config file (to make this safe for parallel tests)
    with open(cli_runner_core.config_path, "r") as f:
        config = yaml.safe_load(f)
    config["database"]["port"] = find_open_port()
    with open(cli_runner_core.config_path, "w") as f:
        yaml.dump(config, f)

    # Initialize the db. Needed for the config command
    cli_runner_core(["init-db"])

    # Tests reading the config
    output = cli_runner_core(["info", "server"])
    assert "name: QCFractal Server" in output

    with open(cli_runner_core.config_path, "r") as f:
        config_text = f.read()

    if full:
        assert "hide_internal_errors" in config_text
    else:
        assert "hide_internal_errors" not in config_text


def test_cli_init(cli_runner_core):
    output = cli_runner_core(["init-db"])

    assert "Initializing QCFractal database from configuration" in output

    if cli_runner_core.own_db is True:
        assert "Success. You can now start the database server using" in output
        assert "pg_ctl: server is running" in output
        assert "PostgreSQL successfully stopped" in output
    else:
        assert "Success. You can now start the database server using" not in output
        assert "pg_ctl: server is running" not in output
        assert "PostgreSQL successfully stopped" not in output


def test_cli_user_add_info_list(cli_runner):
    output = cli_runner(["user", "add", "testuser", "--role", "admin", "--fullname", "A. Test User"])
    assert "Created user testuser" in output

    output = cli_runner(["user", "info", "testuser"])
    assert "testuser" in output
    assert "admin" in output
    assert "A. Test User" in output

    output = cli_runner(["user", "list"])
    assert "testuser" in output
    assert "admin" in output
    assert "A. Test User" in output


def test_cli_user_delete_prompt(cli_runner):
    output = cli_runner(["user", "add", "testuser", "--role", "admin", "--fullname", "A. Test User"])
    assert "Created user testuser" in output

    output = cli_runner(["user", "delete", "testuser"], stdin="N")
    assert "ABORTING" in output

    output = cli_runner(["user", "list"])
    assert "testuser" in output

    output = cli_runner(["user", "delete", "testuser"], stdin="Y")
    assert "User testuser deleted"
    assert "Deleted!" in output

    output = cli_runner(["user", "list"])
    assert "testuser" not in output


def test_cli_user_delete_noprompt(cli_runner):
    output = cli_runner(["user", "add", "testuser", "--role", "admin", "--fullname", "A. Test User"])
    assert "Created user testuser" in output

    output = cli_runner(["user", "delete", "testuser", "--no-prompt"])
    assert "User testuser deleted"
    assert "Deleted!" in output


def test_cli_user_modify_basic(cli_runner):
    output = cli_runner(["user", "add", "testuser", "--role", "admin", "--fullname", "A. Test User"])
    assert "Created user testuser" in output

    output = cli_runner(
        [
            "user",
            "modify",
            "testuser",
            "--fullname",
            "A. New Name",
            "--organization",
            "An Organization",
            "--email",
            "testuser@example.com",
            "--role",
            "submit",
        ]
    )

    assert "User testuser modified" in output

    output = cli_runner(["user", "info", "testuser"])
    assert "A. New Name" in output
    assert "An Organization" in output
    assert "testuser@example.com" in output
    assert "submit" in output
    assert "admin" not in output


def test_cli_user_enable_disable(cli_runner):
    output = cli_runner(["user", "add", "testuser", "--role", "admin", "--fullname", "A. Test User"])
    assert "Created user testuser" in output

    output = cli_runner(["user", "modify", "testuser", "--disable"])
    assert "User testuser modified" in output

    output = cli_runner(["user", "info", "testuser"])
    assert "enabled: False" in output

    output = cli_runner(["user", "modify", "testuser", "--enable"])
    assert "User testuser modified" in output

    output = cli_runner(["user", "info", "testuser"])
    assert "enabled: True" in output


def test_cli_user_password(cli_runner):
    output = cli_runner(["user", "add", "testuser", "--role", "admin", "--fullname", "A. Test User"])
    assert "Created user testuser" in output

    output = cli_runner(["user", "modify", "testuser", "--reset-password"])
    assert "User testuser modified" in output
    assert "New password is below"

    output = cli_runner(["user", "modify", "testuser", "--password", "new_password_1234"])
    assert "User testuser modified" in output
    assert "Password for testuser modified"


def test_cli_restore_noinit(cli_runner_core):
    # Restore where the db does not exist and has not been initialized
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner_core(["restore", migdata_path])
    assert "Restore complete!" in output

    if cli_runner_core.own_db:
        assert "Postgresql instance successfully initialized and started" in output
    else:
        assert "Postgresql instance successfully initialized and started" not in output


def test_cli_backup_restore(cli_runner_core, tmp_path):
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    cli_runner_core(["restore", migdata_path])
    cli_runner_core(["upgrade-db"])

    backup_dir = tmp_path / "db_bak"
    backup_dir.mkdir()
    backup_path = str(backup_dir / "backup_file.sqlback")

    output = cli_runner_core(["backup", backup_path])
    assert os.path.isfile(backup_path)
    assert "Backup complete!" in output

    db_name = cli_runner_core.db_name
    output = cli_runner_core(["restore", backup_path], stdin=f"REMOVEALLDATA {db_name}")
    assert "Restore complete!" in output


def test_cli_restore_existing(cli_runner):
    # Restore where the db already exists
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    db_name = cli_runner.db_name
    output = cli_runner(["restore", migdata_path], stdin=f"REMOVEALLDATA {db_name}")
    assert "Restore complete!" in output

    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner(["restore", migdata_path], stdin="ASD")
    assert "does not match. Aborting" in output


def test_cli_upgrade(cli_runner_core):
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    db_name = cli_runner_core.db_name
    output = cli_runner_core(["restore", migdata_path], stdin=f"REMOVEALLDATA {db_name}")
    assert "Restore complete!" in output

    output = cli_runner_core(["upgrade-db"])

    # One of the migrations that should be there
    assert "Running upgrade c1a0b0ee712e -> b9b7b6926b8b" in output

    if cli_runner_core.own_db is True:
        assert "PostgreSQL successfully stopped" in output


def test_cli_upgrade_noinit(cli_runner_core):
    output = cli_runner_core(["upgrade-db"], fail_expected=True)
    if cli_runner_core.own_db:
        assert "has not been initialized" in output
    else:
        assert "does not exist for upgrading" in output


def test_cli_upgrade_config(tmp_path):
    tmp_subdir = tmp_path / "cli_tmp"
    tmp_subdir.mkdir()

    shutil.copy(old_config_file, tmp_subdir)

    old_config_path = os.path.join(tmp_subdir, os.path.basename(old_config_file))

    output = subprocess.check_output(
        ["qcfractal-server", "upgrade-config", "-v", "--config", old_config_path], universal_newlines=True
    )

    assert "Your configuration file has been upgraded" in output
    assert os.path.isfile(old_config_path + ".backup")


def test_cli_start(cli_runner):
    full_cmd = ["qcfractal-server", "--config", cli_runner.config_path, "start"]

    # Manually start then kill
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    time.sleep(20)
    proc.terminate()
    proc.wait(10)

    assert proc.returncode == 0
    output = proc.stdout.read()

    assert "waitress: Serving on" in output


def test_cli_start_options(cli_runner, tmp_path):

    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    log_path = str(log_dir / "qca_test.logfile")

    port = find_open_port()
    full_cmd = [
        "qcfractal-server",
        "--config",
        cli_runner.config_path,
        "start",
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--logfile",
        log_path,
    ]

    # Manually start then kill
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    time.sleep(20)
    proc.terminate()
    proc.wait(10)

    assert proc.returncode == 0

    with open(log_path, "r") as f:
        log_output = f.read()

    assert f"waitress: Serving on http://0.0.0.0:{port}" in log_output


def test_cli_start_outdated(cli_runner_core):
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    db_name = cli_runner_core.db_name
    output = cli_runner_core(["restore", migdata_path], stdin=f"REMOVEALLDATA {db_name}")
    assert "Restore complete!" in output

    output = cli_runner_core(["start"], timeout=20, fail_expected=True)
    assert "Database needs migration" in output
