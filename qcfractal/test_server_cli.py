import os
import shutil
import subprocess
import time
from typing import List, Optional

import pytest
import yaml

from qcfractal.config import read_configuration
from qcfractaltesting import testconfig_path, migrationdata_path

config_file = os.path.join(testconfig_path, "qcf_basic.yaml")
old_config_file = os.path.join(migrationdata_path, "qcfractal_config_v0.15.8.yaml")
old_db_dump = os.path.join(migrationdata_path, "qcfractal_config_v0.15.8.yaml")


@pytest.fixture(scope="function")
def tmp_config(tmp_path_factory):
    tmp_subdir = tmp_path_factory.mktemp("cli_tmp")
    shutil.copy(config_file, tmp_subdir)
    full_config_path = os.path.join(tmp_subdir, os.path.basename(config_file))
    yield full_config_path


# Test with both an own server and
@pytest.fixture(scope="function", params=[True, False])
def cli_runner_core(postgres_server, tmp_config, request):

    assert postgres_server.is_alive(False)

    own_db = request.param
    if not own_db:
        config = read_configuration([tmp_config])
        config.database = postgres_server.config.copy(deep=True)
        config.database.own = False

        with open(tmp_config, "w") as f:
            yaml.dump(config.dict(), f)

    # Use a functor so we can get own_db (and maybe other info in the future)
    class run_qcfractal_cli:
        def __init__(self):
            self.own_db = own_db
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
            full_cmd = ["qcfractal-server", "--config", self.config_path] + cmd_args
            try:
                ret = subprocess.check_output(
                    full_cmd, stderr=subprocess.STDOUT, universal_newlines=True, input=stdin, timeout=timeout
                )
                if fail_expected:
                    raise RuntimeError("Failure expected, but process succeeded")
                return ret

            except subprocess.CalledProcessError as err:
                if fail_expected:
                    return err.output
                else:
                    raise
            except subprocess.TimeoutExpired as err:
                if timeout_expected:
                    return err.output
                else:
                    raise

    try:
        yield run_qcfractal_cli()
    finally:
        if not own_db:
            postgres_server.delete_database()


@pytest.fixture(scope="function")
def cli_runner(cli_runner_core):

    # Creates the postgres instance and what not (if needed)
    if cli_runner_core.own_db:
        cli_runner_core(["init"])

    yield cli_runner_core


def test_cli_help():
    subprocess.check_output(["qcfractal-server", "--help"])


def test_cli_info_config(cli_runner_core):
    output = cli_runner_core(["info", "config"])
    assert "THISISASECRETKEY" in output
    assert "QCFractal Test Server" in output


def test_cli_info_alembic(cli_runner_core):
    output = cli_runner_core(["info", "alembic"])
    assert "alembic -c" in output


def test_cli_init(cli_runner_core):

    # Do this even if we don't own it. Should be safe
    output = cli_runner_core(["init"])

    assert "Initializing QCFractal from configuration" in output

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


def test_cli_role_info_list(cli_runner):
    output = cli_runner(["role", "list"])
    assert "admin" in output
    assert "submit" in output

    output = cli_runner(["role", "info", "admin"])
    assert "Effect='Allow' Action='*' Resource='*'" in output


def test_cli_role_reset(cli_runner):
    output = cli_runner(["role", "reset"])
    assert "Resetting default roles to their original" in output


def test_cli_restore_noinit(cli_runner_core):
    # Restore where the db does not exist and has not been initialized
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner_core(["restore", migdata_path])
    assert "Restore complete!" in output

    if cli_runner_core.own_db:
        assert "Postgresql instance successfully initialized and started" in output
    else:
        assert "Postgresql instance successfully initialized and started" not in output


def test_cli_restore_init(cli_runner):
    # Restore where the db does not exist, but postgres has been initialized
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner(["restore", migdata_path])
    assert "Restore complete!" in output

    if cli_runner.own_db:
        assert "Started a postgres instance for uri" in output
    else:
        assert "Started a postgres instance for uri" not in output


def test_cli_backup_restore(cli_runner, tmp_path_factory):
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    cli_runner(["restore", migdata_path])
    cli_runner(["upgrade-db"])

    backup_path = os.path.join(tmp_path_factory.mktemp("db_bak"), "backup_file.sqlback")

    output = cli_runner(["backup", backup_path])
    assert os.path.isfile(backup_path)
    assert "Backup complete!" in output

    output = cli_runner(["restore", backup_path], stdin="REMOVEALLDATA qcfractal_default")
    assert "Restore complete!" in output


def test_cli_restore_existing(cli_runner):
    # Restore where the db already exists
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner(["restore", migdata_path])
    assert "Restore complete!" in output

    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner(["restore", migdata_path], stdin="REMOVEALLDATA qcfractal_default")
    assert "Restore complete!" in output

    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")
    output = cli_runner(["restore", migdata_path], stdin="ASD")
    assert "does not match. Aborting" in output


def test_cli_upgrade(cli_runner):
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    output = cli_runner(["restore", migdata_path])
    assert "Restore complete!" in output

    output = cli_runner(["upgrade-db"])

    # One of the migrations that should be there
    assert "Running upgrade c1a0b0ee712e -> b9b7b6926b8b" in output

    if cli_runner.own_db is True:
        assert "PostgreSQL successfully stopped" in output


def test_cli_upgrade_noinit(cli_runner_core):

    output = cli_runner_core(["upgrade-db"], fail_expected=True)
    if cli_runner_core.own_db:
        assert "has not been initialized" in output
    else:
        assert "does not exist for upgrading" in output


def test_cli_upgrade_noexist(cli_runner):
    output = cli_runner(["upgrade-db"], fail_expected=True)
    assert "does not exist for upgrading" in output


def test_cli_upgrade_config(tmp_path_factory):
    tmp_subdir = tmp_path_factory.mktemp("cli_tmp")
    shutil.copy(old_config_file, tmp_subdir)

    old_config_path = os.path.join(tmp_subdir, os.path.basename(old_config_file))

    output = subprocess.check_output(
        ["qcfractal-server", "upgrade-config", "--config", old_config_path], universal_newlines=True
    )

    assert "Your configuration file has been upgraded" in output
    assert os.path.isfile(old_config_path + ".backup")


def test_cli_start(cli_runner):
    full_cmd = ["qcfractal-server", "--config", cli_runner.config_path, "start"]

    # Manually start then kill
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    time.sleep(10)
    proc.terminate()
    proc.wait(10)

    assert proc.returncode == 0
    output = proc.stdout.read()

    assert "Booting worker with pid" in output


def test_cli_start_options(cli_runner, tmp_path_factory):
    log_path = os.path.join(tmp_path_factory.mktemp("logs"), "qca_test.logfile")

    full_cmd = [
        "qcfractal-server",
        "--config",
        cli_runner.config_path,
        "start",
        "--host",
        "0.0.0.0",
        "--port",
        "2828",
        "--num-workers",
        "4",
        "--logfile",
        log_path,
    ]

    # Manually start then kill
    proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    time.sleep(10)
    proc.terminate()
    proc.wait(10)

    assert proc.returncode == 0

    with open(log_path, "r") as f:
        log_output = f.read()

    assert "gunicorn.error: 4 workers" in log_output
    assert "Listening at: http://0.0.0.0:2828" in log_output


def test_cli_start_outdated(cli_runner_core):
    migdata_path = os.path.join(migrationdata_path, "empty_v0.15.8.sql_dump")

    output = cli_runner_core(["restore", migdata_path], stdin="REMOVEALLDATA qcfractal_default")
    assert "Restore complete!" in output

    output = cli_runner_core(["start"], timeout=15, fail_expected=True)
    assert "Database needs migration" in output
