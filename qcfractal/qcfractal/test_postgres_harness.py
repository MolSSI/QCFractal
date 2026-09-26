import os
import sys

import pytest

from qcfractal.config import DatabaseConfig
from qcfractal.port_util import find_open_port
from qcfractal.postgres_harness import PostgresHarness


@pytest.mark.parametrize(
    "env,expected",
    [
        ({}, {"LC_ALL": "C"}),
        ({"LC_ALL": "", "LANG": ""}, {"LC_ALL": "C"}),
        ({"LANG": "en_US.UTF-8"}, None),
        ({"LC_ALL": "en_US.UTF-8"}, None),
    ],
)
def test_postgres_harness_pg_ctl_env(monkeypatch, env, expected):
    monkeypatch.delenv("LC_ALL", raising=False)
    monkeypatch.delenv("LANG", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    assert PostgresHarness._pg_ctl_env() == expected


def _postmaster_environ(pg_harness: PostgresHarness) -> dict:
    # First line of postmaster.pid is the pid of the postmaster
    with open(os.path.join(pg_harness.config.data_directory, "postmaster.pid")) as f:
        pid = int(f.readline().strip())

    with open(f"/proc/{pid}/environ", "rb") as f:
        entries = f.read().split(b"\0")

    return dict(x.decode().split("=", 1) for x in entries if b"=" in x)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Requires /proc")
@pytest.mark.parametrize("lang", [None, "C.UTF-8"])
def test_postgres_harness_locale_env(tmp_path, monkeypatch, lang):
    monkeypatch.delenv("LC_ALL", raising=False)
    monkeypatch.delenv("LANG", raising=False)
    if lang is not None:
        monkeypatch.setenv("LANG", lang)

    db_config = DatabaseConfig(
        port=find_open_port(),
        data_directory=str(tmp_path / "db_data"),
        base_folder=str(tmp_path),
        username="test_user",
        password="test_password",
        own=True,
    )

    pg_harness = PostgresHarness(db_config)
    pg_harness.initialize_postgres()

    try:
        environ = _postmaster_environ(pg_harness)
        encoding = pg_harness.sql_command("SHOW server_encoding", use_maintenance_db=True)[0][0]

        if lang is None:
            # No locale set - the server is started with LC_ALL=C
            assert environ.get("LC_ALL") == "C"
        else:
            # The user's locale is left alone, for both initdb and the server
            assert "LC_ALL" not in environ
            assert environ["LANG"] == lang
            assert encoding == "UTF8"
    finally:
        pg_harness.shutdown()
