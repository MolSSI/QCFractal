from __future__ import annotations

import logging
from copy import deepcopy

from qcarchivetesting import geoip_path, geoip_filename, ip_tests_enabled
from qcfractal.config import DatabaseConfig
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.postgres_harness import PostgresHarness, create_snowflake_postgres
from qcfractal.snowflake import FractalSnowflake
from qcportal import PortalClient, ManagerClient
from qcportal.auth import UserInfo, GroupInfo
from qcportal.managers import ManagerName
from qcportal.utils import update_nested_dict
from .helpers import test_users, test_groups

_activated_manager_programs = {
    "qcengine": ["unknown"],
    "psi4": ["unknown"],
    "qchem": ["unknown"],
    "geometric": ["unknown"],
    "rdkit": ["unknown"],
    "mopac": ["unknown"],
    "prog1": ["unknown"],
    "prog2": ["unknown"],
    "prog3": ["unknown"],
    "prog4": ["unknown"],
    "optprog1": ["unknown"],
    "optprog2": ["unknown"],
    "optprog3": ["unknown"],
    "optprog4": ["unknown"],
}


class QCATestingPostgresHarness(PostgresHarness):
    def __init__(self, config: DatabaseConfig):
        PostgresHarness.__init__(self, config)
        self.db_name = self.config.database_name
        self.template_name = self.db_name + "_template"

    def create_template(self):
        """
        Creates a template database from an existing database

        It is expected that the template database does not already exist.
        """

        self.sql_command(
            f"CREATE DATABASE {self.template_name} TEMPLATE {self.db_name};",
            use_maintenance_db=True,
            returns=False,
        )

    def recreate_database(self):
        """
        Deletes the database, and recreates it from the template
        """

        self.delete_database()
        self.sql_command(
            f"CREATE DATABASE {self.db_name} TEMPLATE {self.template_name};",
            use_maintenance_db=True,
            returns=False,
        )


class QCATestingPostgresServer:
    """
    A temporary postgres instance used for testing
    """

    def __init__(self, db_path: str):
        self.logger = logging.getLogger(__name__)
        self.harness = create_snowflake_postgres("localhost", db_path)
        self.logger.debug(f"Using database located at {db_path} with uri {self.harness.config.safe_uri}")

        # Postgres process is up, but the database is not created
        assert self.harness.is_alive() and not self.harness.can_connect()

    def get_new_harness(self, db_name: str) -> QCATestingPostgresHarness:
        harness_config = deepcopy(self.harness.config.dict())
        harness_config["database_name"] = db_name

        new_harness = QCATestingPostgresHarness(DatabaseConfig(**harness_config))
        new_harness.create_database(create_tables=True)
        return new_harness


class QCATestingSnowflake(FractalSnowflake):
    """
    A snowflake class used for testing

    This mostly behaves like FractalSnowflake, but
    allows for some extra features such as manual handling of internal jobs
    and creating storage sockets from an instance.

    By default, the job runner and worker subprocesses are not started.
    """

    def __init__(
        self,
        pg_harness: QCATestingPostgresHarness,
        encoding: str,
        create_users=False,
        enable_security=False,
        allow_unauthenticated_read=False,
        log_access=True,
        extra_config=None,
    ):
        self.pg_harness = pg_harness
        self.encoding = encoding

        qcf_config = {}

        # Tighten the service frequency for tests
        # Also disable connection pooling in the storage socket
        # (which can leave db connections open, causing problems when we go to delete
        # the database)
        # Have a short token expiration (in case enable_security is True)

        # expire tokens in 5 seconds
        # Too short and this interferes with some other tests
        api_config = {
            "jwt_access_token_expires": 5,
            "user_session_max_age": 5,
        }

        # Smaller api limits (so we can test chunking)
        api_limits = {
            "manager_tasks_claim": 5,
            "manager_tasks_return": 2,
            "get_records": 10,
            "get_dataset_entries": 5,
            "get_molecules": 11,
            "get_managers": 10,
            "get_error_logs": 10,
            "get_access_logs": 10,
        }

        qcf_config["api"] = api_config
        qcf_config["api_limits"] = api_limits

        qcf_config["enable_security"] = enable_security
        qcf_config["allow_unauthenticated_read"] = allow_unauthenticated_read
        qcf_config["service_frequency"] = 5
        qcf_config["loglevel"] = "DEBUG"
        qcf_config["heartbeat_frequency"] = 3
        qcf_config["heartbeat_frequency_jitter"] = 0.0
        qcf_config["heartbeat_max_missed"] = 2

        qcf_config["database"] = {"pool_size": 0}
        qcf_config["log_access"] = log_access
        qcf_config["access_log_keep"] = 1

        if ip_tests_enabled:
            qcf_config["geoip2_dir"] = geoip_path
            qcf_config["geoip2_filename"] = geoip_filename

        qcf_config["auto_reset"] = {"enabled": False}

        # Merge in any other specified config
        if extra_config:
            update_nested_dict(qcf_config, extra_config)

        FractalSnowflake.__init__(
            self,
            start=False,
            compute_workers=0,
            database_config=pg_harness.config,
            extra_config=qcf_config,
        )

        self._original_config = self._qcf_config.copy(deep=True)

        if create_users:
            self.create_users()

        self.start_api()

    def create_users(self):
        # Get a storage socket and add the roles/users/passwords
        storage = self.get_storage_socket()

        for g in test_groups:
            storage.groups.add(GroupInfo(groupname=g))

        for k, v in test_users.items():
            uinfo = UserInfo(username=k, enabled=True, **v["info"])
            storage.users.add(uinfo, password=v["pw"])

    def reset(self):
        self._stop_job_runner()
        self._stop_compute()
        self._all_completed = set()
        self._qcf_config = self._original_config.copy(deep=True)

        if self._api_proc is None:
            self.start_api()

        self.pg_harness.recreate_database()

    def get_storage_socket(self) -> SQLAlchemySocket:
        """
        Obtain a new SQLAlchemy socket

        This function will create a new socket instance every time it is called
        """

        return SQLAlchemySocket(self._qcf_config)

    def activate_manager(self):
        """
        Activates a manager on the server, but does not start any manager compute process
        """

        mname = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
        socket = self.get_storage_socket()

        mid = socket.managers.activate(
            name_data=mname,
            manager_version="v2.0",
            username="bill",
            programs=_activated_manager_programs,
            compute_tags=["*"],
        )

        return mname, mid

    def activated_manager_programs(self):
        return _activated_manager_programs

    def start_api(self) -> None:
        """
        Starts the flask/api thread
        """
        self._start_api()

    def stop_api(self) -> None:
        """
        Stops the flask/api thread
        """
        self._stop_api()

    def start_job_runner(self) -> None:
        """
        Starts the job runner thread
        """
        self._start_job_runner()

    def stop_job_runner(self) -> None:
        """
        Stops the job_runner thread
        """
        self._stop_job_runner()

    def client(self, username=None, password=None, cache_dir=None) -> PortalClient:
        """
        Obtain a client connected to this snowflake

        Parameters
        ----------
        username
            The username to connect as
        password
            The password to use
        cache_dir
            Directory to store cache files in

        Returns
        -------
        :
            A PortalClient that is connected to this snowflake
        """

        client = PortalClient(self.get_uri(), username=username, password=password, cache_dir=cache_dir)
        client.encoding = self.encoding
        return client

    def manager_client(self, name_data: ManagerName, username=None, password=None) -> ManagerClient:
        """
        Obtain a manager client connected to this snowflake

        Parameters
        ----------
        username
            The username to connect as
        password
            The password to use

        Returns
        -------
        :
            A PortalClient that is connected to this snowflake
        """

        # Now that we know it's up, create a manager client
        client = ManagerClient(name_data, self.get_uri(), username=username, password=password)
        return client
