"""
Pytest fixtures for QCArchive
"""

from __future__ import annotations

import secrets
from typing import Tuple

import pytest

from qcfractal.config import FractalConfig
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcportal import PortalClient
from qcportal.managers import ManagerName
from qcportal.utils import update_nested_dict
from .helpers import geoip_path, geoip_filename, ip_tests_enabled, test_users
from .testing_classes import QCATestingPostgresServer, QCATestingSnowflake, _activated_manager_programs


def _generate_default_config(pg_harness, extra_config=None) -> FractalConfig:
    # Create a configuration. Since this is mostly just for a storage socket,
    # We can use defaults for almost all, since a flask server, etc, won't be instantiated
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    cfg_dict = {}
    cfg_dict["base_folder"] = pg_harness.config.base_folder
    cfg_dict["loglevel"] = "DEBUG"
    cfg_dict["database"] = pg_harness.config.dict()
    cfg_dict["database"]["pool_size"] = 0
    cfg_dict["log_access"] = True

    if ip_tests_enabled:
        cfg_dict["geoip2_dir"] = geoip_path
        cfg_dict["geoip2_filename"] = geoip_filename

    cfg_dict["api"] = {"secret_key": secrets.token_urlsafe(32), "jwt_secret_key": secrets.token_urlsafe(32)}

    if extra_config:
        cfg_dict = update_nested_dict(cfg_dict, extra_config)

    return FractalConfig(**cfg_dict)


@pytest.fixture(scope="session")
def postgres_server(tmp_path_factory):
    """
    A postgres server instance

    This does not contain any databases (except the postgres defaults).

    This is built only once per session, and automatically deleted after the session. It uses
    a pytest-provided session-scoped temporary directory
    """

    db_path = str(tmp_path_factory.mktemp("db"))
    test_pg = QCATestingPostgresServer(db_path)
    yield test_pg


@pytest.fixture(scope="session")
def session_storage_socket(postgres_server):
    """
    A fixture for a storage socket that lasts the entire testing session
    """

    pg_harness = postgres_server.get_new_harness("session_storage")
    qcf_config = _generate_default_config(pg_harness)
    socket = SQLAlchemySocket(qcf_config)

    # Create the template database for use in re-creating the database
    pg_harness.create_template()

    yield socket, pg_harness


@pytest.fixture(scope="function")
def storage_socket(session_storage_socket):
    """
    A fixture for storage socket that lasts a single test function

    The database is deleted then recreated after the test function
    """

    socket, pg_harness = session_storage_socket
    yield socket
    pg_harness.recreate_database()


@pytest.fixture(scope="function")
def session(storage_socket):
    """
    A single sqlalchemy session associated with a storage socket
    """
    with storage_socket.session_scope() as s:
        yield s


@pytest.fixture(scope="session")
def session_snowflake(postgres_server, pytestconfig):
    """
    A QCFractal testing snowflake, existing for the entire session
    """

    pg_harness = postgres_server.get_new_harness("session_snowflake")
    encoding = pytestconfig.getoption("--client-encoding")
    with QCATestingSnowflake(pg_harness, encoding) as snowflake:
        pg_harness.create_template()
        yield snowflake


@pytest.fixture(scope="function")
def snowflake(session_snowflake):
    """
    A QCFractal snowflake used for testing (function-scoped)

    The underlying database is deleted and recreated after each test function
    """

    yield session_snowflake
    session_snowflake.reset()


@pytest.fixture(scope="session")
def session_secure_snowflake(postgres_server, pytestconfig):
    """
    A QCFractal snowflake with authorization/authentication enabled
    """

    pg_harness = postgres_server.get_new_harness("session_secure_snowflake")
    encoding = pytestconfig.getoption("--client-encoding")
    with QCATestingSnowflake(
        pg_harness, encoding, create_users=True, enable_security=True, allow_unauthenticated_read=False
    ) as snowflake:
        pg_harness.create_template()
        yield snowflake


@pytest.fixture(scope="function")
def secure_snowflake(session_secure_snowflake):
    """
    A QCFractal snowflake with authorization/authentication enabled (function-scoped)
    """

    yield session_secure_snowflake
    session_secure_snowflake.reset()


@pytest.fixture(scope="session")
def session_secure_snowflake_allow_read(postgres_server, pytestconfig):
    """
    A QCFractal snowflake with authorization/authentication enabled, but allowing unauthenticated read
    """

    pg_harness = postgres_server.get_new_harness("session_secure_snowflake_allow_read")
    encoding = pytestconfig.getoption("--client-encoding")
    with QCATestingSnowflake(
        pg_harness, encoding, create_users=True, enable_security=True, allow_unauthenticated_read=True
    ) as snowflake:
        pg_harness.create_template()
        yield snowflake


@pytest.fixture(scope="function")
def secure_snowflake_allow_read(session_secure_snowflake_allow_read):
    """
    A QCFractal snowflake with authorization/authentication enabled, but allowing unauthenticated read (function-scoped)
    """

    yield session_secure_snowflake_allow_read
    session_secure_snowflake_allow_read.reset()


@pytest.fixture(scope="function")
def submitter_client(secure_snowflake):
    client = secure_snowflake.client("submit_user", test_users["submit_user"]["pw"])
    yield client


@pytest.fixture(scope="function")
def snowflake_client(snowflake):
    """
    A client connected to a testing snowflake

    This is for a simple snowflake (no security, no compute) because a lot
    of tests will use this. Other tests will need to use a different fixture
    and manually call client() there
    """

    client = snowflake.client()
    yield client


@pytest.fixture(scope="function")
def activated_manager(storage_socket: SQLAlchemySocket) -> Tuple[ManagerName, int]:
    """
    An activated manager, returning only its manager name and id
    """
    mname = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mid = storage_socket.managers.activate(
        name_data=mname,
        manager_version="v2.0",
        username="bill",
        programs=_activated_manager_programs,
        compute_tags=["*"],
    )

    yield mname, mid


@pytest.fixture(scope="function")
def activated_manager_name(activated_manager) -> ManagerName:
    """
    An activated manager, returning only its manager name
    """

    yield activated_manager[0]


@pytest.fixture(scope="function")
def activated_manager_programs(activated_manager) -> ManagerName:
    """
    An activated manager, returning only its programs
    """

    yield _activated_manager_programs


@pytest.fixture(scope="function")
def fulltest_client(pytestconfig):
    """
    A portal client used for full end-to-end tests
    """

    uri = pytestconfig.getoption("--fractal-uri")

    if uri == "snowflake":
        from qcfractal.snowflake import FractalSnowflake

        s = FractalSnowflake()
        yield s.client()
        s.stop()

    else:
        yield PortalClient(address=uri)
