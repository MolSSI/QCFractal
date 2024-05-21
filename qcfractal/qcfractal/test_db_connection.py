import os

import pytest
from sqlalchemy import create_engine

from qcfractal.config import DatabaseConfig
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.port_util import find_open_port
from qcfractal.postgres_harness import PostgresHarness


@pytest.mark.parametrize(
    "host,port,username,password,dbname,mdbname,query",
    [
        (
            "localhost",
            5432,
            "test_user_1",
            "test_pass_1234",
            "testing_db_1",
            "testing_db_maint_1",
            {"connect_timeout": 10},
        ),
        (
            "192.168.1.234",
            6543,
            "test_user_1",
            "test_pass_2",
            "testing_db_2",
            "testing_db_maint_2",
            {"connect_timeout": 10},
        ),
        (
            "/var/run/postgresql",
            9876,
            "test_user_4",
            "test_pass_9999",
            "testing_db_4",
            "testing_db_maint_4",
            {"connect_timeout": 10},
        ),
    ],
)
def test_db_connection_uri_convert(host, port, username, password, dbname, mdbname, query):
    # Test all the different URI formats
    db_config = DatabaseConfig(
        data_directory="/tmp/fakedir",  # not used in this test
        base_folder="/tmp/fakedir",  # not used in this test
        own=True,
        host=host,
        port=port,
        username=username,
        password=password,
        database_name=dbname,
        query=query,
        maintenance_db=mdbname,
    )

    # print(db_config.database_uri)
    # print(db_config.safe_uri)
    # print(db_config.sqlalchemy_url.render_as_string(False))
    # print(db_config.sqlalchemy_url.render_as_string(True))
    # print(db_config.psycopg2_dsn)
    # print(db_config.psycopg2_maintenance_dsn)

    common = [host, str(port)]
    if username:
        common.append(username)
    if query:
        for k, v in query.items():
            common.append(f"{k}={v}")

    assert host in db_config.database_uri
    assert host in db_config.psycopg2_dsn
    assert host in db_config.psycopg2_maintenance_dsn
    assert host in db_config.safe_uri

    # SQLAlchemy render_as_string mangles sockets :(
    if not host.startswith("/"):
        assert host in db_config.sqlalchemy_url.render_as_string(False)
        assert host in db_config.sqlalchemy_url.render_as_string(True)

    assert str(port) in db_config.database_uri
    assert str(port) in db_config.psycopg2_dsn
    assert str(port) in db_config.psycopg2_maintenance_dsn
    assert str(port) in db_config.safe_uri
    assert str(port) in db_config.sqlalchemy_url.render_as_string(False)
    assert str(port) in db_config.sqlalchemy_url.render_as_string(True)

    if username:
        assert username in db_config.database_uri
        assert username in db_config.psycopg2_dsn
        assert username in db_config.psycopg2_maintenance_dsn
        assert username in db_config.safe_uri
        assert username in db_config.sqlalchemy_url.render_as_string(False)
        assert username in db_config.sqlalchemy_url.render_as_string(True)

    # Passwords in all but safe_uri
    if password:
        assert password in db_config.database_uri
        assert password in db_config.psycopg2_dsn
        assert password in db_config.psycopg2_maintenance_dsn
        assert password not in db_config.safe_uri
        assert password in db_config.sqlalchemy_url.render_as_string(False)
        assert password not in db_config.sqlalchemy_url.render_as_string(True)

    # dbname in all but maintenance uri
    assert dbname in db_config.database_uri
    assert dbname in db_config.psycopg2_dsn
    assert dbname not in db_config.psycopg2_maintenance_dsn
    assert dbname in db_config.sqlalchemy_url.render_as_string(False)
    assert dbname in db_config.sqlalchemy_url.render_as_string(True)
    assert dbname in db_config.safe_uri

    # Maintenance db only in maintenance uri
    assert mdbname not in db_config.database_uri
    assert mdbname not in db_config.psycopg2_dsn
    assert mdbname in db_config.psycopg2_maintenance_dsn
    assert mdbname not in db_config.sqlalchemy_url.render_as_string(False)
    assert mdbname not in db_config.sqlalchemy_url.render_as_string(True)
    assert mdbname not in db_config.safe_uri

    for k, v in query.items():
        s = f"{k}={v}"
        assert s in db_config.database_uri
        assert s in db_config.psycopg2_dsn
        assert s in db_config.psycopg2_maintenance_dsn
        assert s in db_config.safe_uri
        assert s in db_config.sqlalchemy_url.render_as_string(False)
        assert s in db_config.sqlalchemy_url.render_as_string(True)


def test_db_connection_hosts(tmp_path_factory):
    base_path = tmp_path_factory.mktemp("basefolder")
    tmp_path = tmp_path_factory.mktemp("db_data")

    port = find_open_port()
    db_config = DatabaseConfig(
        port=port,
        data_directory=str(tmp_path),
        base_folder=str(base_path),
        username="test_connstr_user",
        password="test_connstr_password_1234",
        database_name="testing_db_connstr",
        query={"connect_timeout": 10},
        own=True,
    )

    pg_harness = PostgresHarness(db_config)

    # Change trust method so we can actually check passwords
    pg_harness.initialize_postgres()
    pg_harness.create_database(create_tables=True)
    assert pg_harness.can_connect()

    # Make sure tests can fail
    new_db_config = pg_harness.config.copy(update={"password": "not_correct"})
    new_pg_harness = PostgresHarness(new_db_config)
    assert new_pg_harness.can_connect() is False

    sock_path = os.path.join(db_config.data_directory, "sock")
    for test_host in ["localhost", "127.0.0.1", sock_path]:
        new_db_config = db_config.copy(update={"host": test_host})
        assert PostgresHarness(new_db_config).can_connect()
        assert create_engine(new_db_config.sqlalchemy_url).connect()
        SQLAlchemySocket.upgrade_database(new_db_config)


def test_db_connection_full_uri(tmp_path_factory):
    base_path = tmp_path_factory.mktemp("basefolder")
    tmp_path = tmp_path_factory.mktemp("db_data")

    port = find_open_port()
    db_config = DatabaseConfig(
        port=port,
        data_directory=str(tmp_path),
        base_folder=str(base_path),
        username="test_connstr_user",
        password="test_connstr_password_1234",
        database_name="testing_db_connstr",
        query={"connect_timeout": 10},
        own=True,
    )

    pg_harness = PostgresHarness(db_config)

    # Change trust method so we can actually check passwords
    pg_harness.initialize_postgres()
    pg_harness.create_database(create_tables=True)
    assert pg_harness.can_connect()

    # Make sure tests can fail
    new_db_config = pg_harness.config.copy(update={"password": "not_correct"})
    new_pg_harness = PostgresHarness(new_db_config)
    assert new_pg_harness.can_connect() is False

    def can_connect(full_uri):
        test_cfg = db_config.copy(update={"full_uri": full_uri})
        assert PostgresHarness(test_cfg).can_connect()
        assert create_engine(test_cfg.sqlalchemy_url).connect()
        SQLAlchemySocket.upgrade_database(test_cfg)

    username = db_config.username
    password = db_config.password
    dbname = db_config.database_name

    for host in ["localhost", "127.0.0.1"]:
        can_connect(f"postgresql://{username}:{password}@{host}:{port}/{dbname}?connect_timeout=10")
        can_connect(f"postgresql://{username}:{password}@{host}:{port}/?dbname={dbname}&connect_timeout=10")
        can_connect(f"postgresql://{host}:{port}/?dbname={dbname}&password={password}&user={username}")
        can_connect(f"postgresql:///?host={host}&port={port}&dbname={dbname}&password={password}&user={username}")
        can_connect(f"postgresql://:{port}/?host={host}&dbname={dbname}&password={password}&user={username}")

    ## Socket file?
    sock_path = os.path.join(db_config.data_directory, "sock")
    can_connect(f"postgresql://{username}:{password}@:{port}/{dbname}?host={sock_path}")
