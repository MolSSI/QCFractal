"""
Contains testing infrastructure for QCFractal.
"""

import gc
import logging

import pytest

from qcfractal.config import FractalConfig
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcfractal.postgres_harness import TemporaryPostgres
from qcfractal.testing_helpers import TestingSnowflake
from qcfractaltesting import valid_encodings, geoip_path

#######################################
# Database and storage socket fixtures
#######################################
from qcportal.managers import ManagerName


@pytest.fixture(scope="session")
def postgres_server(tmp_path_factory):
    """
    A postgres server instance

    This does not contain the target database, but does contain an empty template database
    that can be used by snowflake, etc.

    This is built only once per session, and automatically deleted after the session. It uses
    a pytest-provided session-scoped temporary directory
    """

    logger = logging.getLogger(__name__)

    db_path = str(tmp_path_factory.mktemp("db"))
    tmp_pg = TemporaryPostgres(data_dir=db_path)
    pg_harness = tmp_pg._harness
    logger.debug(f"Using database located at {db_path} with uri {pg_harness.database_uri}")
    assert pg_harness.is_alive(False) and not pg_harness.is_alive(True)

    # Create the database, and we will use that as a template
    # We connect to the "postgres" database, so as not to be using the database we want to copy
    pg_harness.create_database()
    pg_harness.sql_command(
        f"CREATE DATABASE template_db TEMPLATE {tmp_pg._config.database_name};", database_name="postgres", returns=False
    )

    # Delete the database - we will create it from the template later
    pg_harness.delete_database()

    yield pg_harness

    if tmp_pg:
        tmp_pg.stop()


def _temporary_database(postgres_server):
    """
    A temporary database that only lasts for one function

    It is part of the postgres instance given by the postgres_server fixture
    """

    # Make sure that the database process is up
    assert postgres_server.is_alive(False)

    # Create the database from the template
    db_name = postgres_server.config.database_name
    postgres_server.sql_command(
        f"CREATE DATABASE {db_name} TEMPLATE template_db;", database_name="postgres", returns=False
    )

    try:
        yield postgres_server
    finally:
        # Force a garbage collection. Sometimes there's session objects or something
        # hanging around, which will prevent the database from being deleted
        gc.collect()

        postgres_server.delete_database()


# Two - one for function scope, and one for module scope
temporary_database = pytest.fixture(_temporary_database, scope="function")
module_temporary_database = pytest.fixture(_temporary_database, scope="module")


@pytest.fixture(scope="function")
def storage_socket(temporary_database):
    """
    A fixture for temporary database and storage socket

    This should not be used with other fixtures, but used for unit testing
    the storage socket.
    """

    # Create a configuration. Since this is mostly just for a storage socket,
    # We can use defaults for almost all, since a flask server, etc, won't be instantiated
    # Also disable connection pooling in the storage socket
    # (which can leave db connections open, causing problems when we go to delete
    # the database)
    cfg_dict = {}
    cfg_dict["base_folder"] = temporary_database.config.base_folder
    cfg_dict["loglevel"] = "DEBUG"
    cfg_dict["database"] = temporary_database.config.dict()
    cfg_dict["database"]["pool_size"] = 0
    cfg_dict["log_access"] = True
    cfg_dict["geo_file_path"] = geoip_path
    qcf_config = FractalConfig(**cfg_dict)

    socket = SQLAlchemySocket(qcf_config)
    yield socket


@pytest.fixture(scope="function", params=valid_encodings)
def stopped_snowflake(temporary_database, request):
    """
    A QCFractal snowflake server used for testing, but with nothing started by default
    """

    db_config = temporary_database.config
    with TestingSnowflake(db_config, encoding=request.param, start_flask=False) as server:
        yield server


@pytest.fixture(scope="function")
def snowflake(stopped_snowflake):
    """
    A QCFractal snowflake server used for testing
    """

    stopped_snowflake.start_flask()
    yield stopped_snowflake


@pytest.fixture(scope="function", params=valid_encodings)
def secure_snowflake(temporary_database, request):
    """
    A QCFractal snowflake server with authorization/authentication enabled
    """

    db_config = temporary_database.config
    with TestingSnowflake(
        db_config,
        encoding=request.param,
        start_flask=True,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
    ) as server:
        yield server


@pytest.fixture(scope="function", params=valid_encodings)
def secure_snowflake_allow_read(temporary_database, request):
    """
    A QCFractal snowflake server with authorization/authentication enabled, but allowing
    unauthenticated read
    """

    db_config = temporary_database.config
    with TestingSnowflake(
        db_config,
        encoding=request.param,
        start_flask=True,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=True,
    ) as server:
        yield server


@pytest.fixture(scope="function")
def snowflake_client(snowflake):
    """
    A client connected to a testing snowflake

    This is for a simple snowflake (no security, no compute) because a lot
    of tests will use this. Other tests will need to use a different fixture
    and manually call client() there
    """

    yield snowflake.client()


@pytest.fixture(scope="function")
def activated_manager_name(storage_socket: SQLAlchemySocket) -> ManagerName:
    """
    An activated manager, returning only its manager name
    """
    mname = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket.managers.activate(
        name_data=mname,
        manager_version="v2.0",
        username="bill",
        programs={
            "qcengine": None,
            "psi4": None,
            "qchem": None,
            "geometric": None,
            "rdkit": None,
            "prog1": None,
            "prog2": None,
            "prog3": None,
            "prog4": None,
        },
        tags=["*"],
    )

    yield mname
