from __future__ import annotations

import logging
import os
import pathlib
import re
import secrets
import shutil
import subprocess
import time
from typing import TYPE_CHECKING

import psycopg2
import tabulate
from psycopg2.errors import OperationalError, ObjectInUse

from .config import DatabaseConfig
from .db_socket.socket import SQLAlchemySocket
from .port_util import find_open_port, is_port_inuse

if TYPE_CHECKING:
    from typing import Any, List, Optional, Tuple, Dict
    import psycopg2.extensions


class PostgresHarness:
    def __init__(self, config: DatabaseConfig):
        """A manager for postgres server instances

        This class is used to create, start and stop postgres instances, particularly if
        QCFractal is expected to own this instance. This can also be used to detect
        and update postgres databases.

        Parameters
        ----------
        config
            The configuration options
        """
        self.config = config
        self._logger = logging.getLogger("PostgresHarness")
        self._alembic_ini = os.path.join(os.path.abspath(os.path.dirname(__file__)), "alembic.ini")

        # Figure out the directory containing postgres utilities
        # if not specified in the configuration
        # This is only required if own == True
        self._tool_dir = config.pg_tool_dir if config.own is True else None

        # Did we start the database (and therefore should shut it down?)
        self._started_db = False

        # Own the database, but no tool directory specified
        if config.own and config.pg_tool_dir is None:
            # Find pg_config. However, we should try all the possible pg_config available in the PATH.
            # We depend on libpq, which will install pg_config but none of the other tools
            env_path = os.environ.get("PATH", None)

            # Not sure this should ever happen
            search_dirs = list() if env_path is None else env_path.split(os.pathsep)

            # Go through all the path directories, finding pg_config
            # Then, after running pg_config --bindir, test that path for pg_ctl
            for search_dir in search_dirs:
                self._logger.debug(f"Searching for pg_config in path {search_dir}")
                pg_config_path = shutil.which("pg_config", path=search_dir)
                if pg_config_path is None:
                    continue

                self._logger.debug(f"Found pg_config in path {search_dir}")

                # Run pg_config to get the path
                ret, stdout, _ = self._run_subprocess([pg_config_path, "--bindir"])
                if ret != 0:
                    self._logger.error(f"pg_config returned non-zero error code: {ret}")
                else:
                    possible_path = stdout.strip()

                    # Does pg_ctl exist there?
                    pg_ctl_path = shutil.which("pg_ctl", path=possible_path)
                    if pg_ctl_path is not None:
                        self._logger.info(f"Using Postgres tools found via pg_config located in {possible_path}")
                        self._tool_dir = possible_path
                        break

            # If the tool dir is still None, that is a problem (when own = True)
            if self._tool_dir is None:
                raise RuntimeError(
                    "Postgresql tools cannot be found. Postgresql may not be installed, or pg_tool_dir is incorrect. If you do have Postgresql, try setting pg_tool_dir in the configuration to the directory containing psql, pg_ctl, etc"
                )

            # Determine version
            pg_ctl_path = self._get_tool("pg_ctl")
            ret, stdout, stderr = self._run_subprocess([pg_ctl_path, "--version"])
            if ret != 0:
                raise RuntimeError(
                    f"Error running pg_ctl --version. Something is probably pretty wrong. Return code {ret}\noutput:\n{stdout}\nstderr:\n{stderr}"
                )

            pg_version = stdout.strip()
            self._logger.info(f"Postgresql version found: {pg_version}")

    def _get_tool(self, tool: str) -> str:
        """
        Obtains the path to a postgres tool (such as pg_ctl)

        If 'pg_tool_dir' is not specified, the current PATH environment is searched instead

        If the tool is not found, an exception is raised

        Parameters
        ----------
        tool
            Tool to search for (ie, psql)

        Returns
        -------
        :
            Full path to the tools executable
        """

        tool_path = shutil.which(tool, path=self._tool_dir)
        if tool_path is None:
            raise RuntimeError(
                f"Postgresql tool/command {tool} cannot be found in path {self._tool_dir}. Postgresql may not be installed, or pg_tool_dir is incorrect."
            )
        return tool_path

    def _run_subprocess(
        self, command: List[str], env: Optional[Dict[str, Any]] = None, shell: bool = False
    ) -> Tuple[int, str, str]:
        """
        Runs a command using subprocess, and output stdout into the logger

        Parameters
        ----------
        command
            Command to run as a list of strings (see documentation for subprocess)

        Returns
        -------
        :
            Return code, stdout, and stderr as a Tuple
        """

        full_env = os.environ.copy()

        if env is not None:
            full_env.update(env)

        self._logger.debug("Running subprocess: " + str(command))
        if shell:
            proc = subprocess.run(
                " ".join(command),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=full_env,
                shell=True,
                executable="/bin/bash",
            )
        else:
            proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=full_env)

        stdout = proc.stdout.decode()
        stderr = proc.stderr.decode()
        if len(stdout) > 0:
            self._logger.info(stdout)
        if len(stderr) > 0:
            self._logger.info(stderr)

        return proc.returncode, stdout, stderr

    @property
    def database_dsn(self) -> str:
        """Provides the full PostgreSQL connection string (for direct use with psycopg2)"""
        return self.config.psycopg2_dsn

    @property
    def maintenance_dsn(self) -> str:
        """Provides the full PostgreSQL URI string for the maintenance db (for direct use with psycopg2)"""
        return self.config.psycopg2_maintenance_dsn

    def can_connect(self) -> bool:
        """Checks if the postgres instance is alive, that the database exists, and that we can connect to it

        This function swallows most exceptions (OperationalError), and returns False in those cases

        Returns
        -------
        :
            True if the instance is alive and that we can connect to it. Optionally checks that
            the database exists
        """

        try:
            conn = psycopg2.connect(self.database_dsn)
            conn.close()
            return True
        except psycopg2.OperationalError as e:
            return False

    def is_alive(self) -> bool:
        """Checks if the postgres is alive

        This only checks to see if an instance is running on the given host/port. It does not check
        if we have permission to connect to it or if the database exists

        Returns
        -------
        :
            True if a postgres instance is alive
        """

        try:
            conn = psycopg2.connect(self.database_dsn)
            conn.close()
            return True
        except psycopg2.OperationalError as e:
            estr = str(e)

            if "Connection refused" in estr:
                return False

            # right?
            return True

    def ensure_alive(self):
        """
        Checks to see that the postgres instance is up and running

        If it is not running, but we are expected to control the database, it will be started.
        This function does not check anything about the database.

        This will raise an exception if the database is not alive and/or could not be started
        """

        # if own = True, we are responsible for starting the db instance
        # But don't start it if it is already alive. It may have been started
        # elsewhere.
        if not self.is_alive():
            if self.config.own:
                self.start()
                self._logger.info(f"Started a postgres instance for uri {self.config.safe_uri}")
            else:
                raise RuntimeError(
                    f"A running postgres instance serving uri {self.config.safe_uri} does not appear to be running. "
                    f"It must be running for me to continue "
                )

        self._logger.info(f"Postgres instance serving uri {self.config.safe_uri} appears to be up and running")

    def sql_command(
        self, statement: str, use_maintenance_db: bool = False, autocommit: bool = True, returns=True
    ) -> Any:
        """Runs a single SQL query or statement string and returns the output

        Parameters
        ----------
        statement
            A psql command/query string.
        use_maintenance_db: bool
            If true, connect to the maintenance db instead
        autocommit
            If true, enable autocommit on the connection
        returns
            If true, fetch the results and return them. Set to False for commands that
            don't return anything.
        """

        if use_maintenance_db:
            uri = self.maintenance_dsn
        else:
            uri = self.database_dsn

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.autocommit = True
        cursor = conn.cursor()

        try:
            self._logger.debug(f"Executing SQL: {statement}")
            cursor.execute(statement)
            if returns:
                return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

    def pg_ctl(self, cmds: List[str]) -> Tuple[int, str, str]:
        """Runs a pg_ctl command and returns its output

        Parameters
        ----------
        cmds
            A list of PostgreSQL pg_ctl commands to run.

        Returns
        -------
        :
            The return code, stdout, and stderr returned by pg_ctl
        """

        # pg_ctl should only be run if we are managing the db
        assert self.config.own
        self._logger.debug(f"Running pg_ctl command: {cmds}")

        pg_ctl = self._get_tool("pg_ctl")
        all_cmds = [pg_ctl, "-l", self.config.logfile, "-D", self.config.data_directory]
        all_cmds.extend(cmds)
        return self._run_subprocess(all_cmds)

    def get_postgres_version(self) -> str:
        """Returns the version of the postgres instance"""

        return self.sql_command("SELECT version()", use_maintenance_db=True)[0][0]

    def get_alembic_version(self) -> str:
        """Returns the version of the alembic schema in the database"""

        return self.sql_command("SELECT version_num from alembic_version")[0][0]

    def create_database(self, create_tables: bool):
        """Creates a new qcfractal database (and tables)

        The postgres instance must be initialized and running.

        If the database is existing, no changes to the database are made.
        If there is an error, an exception in raised

        Parameters
        ----------
        create_tables
            If true, create the tables (schema) as well
        """

        # First we connect to an existing database
        # The database specified in the config may not exist yet
        conn = psycopg2.connect(self.maintenance_dsn)
        conn.autocommit = True

        cursor = conn.cursor()

        try:
            # Now we can search the pg_catalog to see if the database we want exists yet
            cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.config.database_name}'")
            exists = cursor.fetchone()

            if not exists:
                self._logger.info(f"Creating database {self.config.database_name}...")
                cursor.execute(f"CREATE DATABASE {self.config.database_name}")
                self._logger.info(f"Database {self.config.database_name} created")

                if create_tables:
                    SQLAlchemySocket.create_database_tables(self.config)
            else:
                self._logger.info(f"Database {self.config.database_name} already exists, so I am leaving it alone")

            # Check to see that everything is ok
            if not self.can_connect():
                raise RuntimeError("I created the database, but now can't connect to it? Maybe check the postgres logs")
        finally:
            cursor.close()
            conn.close()

    def delete_database(self) -> None:
        """
        Deletes the database

        This will delete all data associated with the database!
        """
        # First we connect to the maintenance database
        # The database specified in the config may not exist yet
        conn = psycopg2.connect(self.maintenance_dsn)
        conn.autocommit = True

        cursor = conn.cursor()

        # Now we can search the pg_catalog to see if the database we want exists yet
        self._logger.info(f"Deleting/Dropping database {self.config.database_name}")

        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.config.database_name}")
        except (OperationalError, ObjectInUse) as e:
            err = f"Could not delete database. Was it still open somewhere?\nError: {str(e)}\n"
            cursor.execute("SELECT pid,state,query_start,wait_event_type,wait_event,query FROM pg_stat_activity")

            err += "Open Connections\n-----------------------\n"
            err += tabulate.tabulate(
                cursor, headers=["pid", "state", "query_start", "wait_event_type", "wait_event", "query"]
            )
            err += "\n"
            raise RuntimeError(err)
        finally:
            cursor.close()
            conn.close()

    def start(self) -> None:
        """
        Starts a PostgreSQL server based off the current configuration parameters.

        The PostgreSQL server must be initialized and the configured port available.
        The database does not need to exist.
        """

        # We should only do this if we are in charge of the database itself
        assert self.config.own

        # Startup the server
        self._logger.info("Starting the PostgreSQL instance")

        # We should be in charge of this postgres process. If something is running, then that is a problem
        if is_port_inuse("localhost", self.config.port):
            raise RuntimeError(
                f"A process is already running on port {self.config.port} that is not associated with this QCFractal instance's database"
            )
        else:
            retcode, stdout, stderr = self.pg_ctl(["start"])

            err_msg = f"Error starting PostgreSQL. Did you remember to initialize it (qcfractal-server init)?\noutput:\n{stdout}\nstderr:\n{stderr}"

            if retcode != 0:
                raise RuntimeError(err_msg)
            if not (("server started" in stdout) or ("server starting" in stdout)):
                raise RuntimeError(err_msg)

            # Check that we are alive
            for x in range(10):
                if self.is_alive():
                    break
                else:
                    time.sleep(0.2)
            else:
                raise RuntimeError(err_msg)

            retcode, stdout, stderr = self.pg_ctl(["status"])
            if retcode == 0:
                self._logger.info("PostgreSQL successfully started in a background process")
            else:
                err_msg = f"Database seemed to start, but status check failed:\noutput:\n{stdout}\nstderr:\n{stderr}"
                raise RuntimeError(err_msg)

        self._started_db = True

    def shutdown(self) -> None:
        """Shuts down the current postgres instance (if we control it)

        This only shuts down the instance if qcfractal is expected to manage it (own=True) and
        if this process started it
        """

        # We don't manage the database
        if self.config.own is False or self._started_db is False:
            return

        if self.is_alive() is False:
            return

        retcode, stdout, stderr = self.pg_ctl(["stop"])
        if retcode == 0:
            self._logger.info("PostgreSQL successfully stopped")
        else:
            err_msg = f"Error stopping the postgres instance:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def postgres_initialized(self):
        """
        Returns True if the postgres instance has been initialized, False otherwise
        """

        psql_conf_file = os.path.join(self.config.data_directory, "postgresql.conf")
        return os.path.exists(psql_conf_file)

    def initialize_postgres(self) -> None:
        """Initializes a postgresql instance and starts it

        The data directory and port from the configuration is used for the postgres instance

        This does not create the QCFractal database or its tables
        """

        # Can only initialize if we are expected to manage it
        assert self.config.own

        if not self.config.username or not self.config.password:
            raise RuntimeError("Username or password are not given")

        self._logger.info("Initializing the Postgresql database")

        # Is the specified port open? Stop early if in use
        if is_port_inuse("localhost", self.config.port):
            raise RuntimeError("Port is already in use. Specify another port for the database")

        psql_conf_file = os.path.join(self.config.data_directory, "postgresql.conf")

        if os.path.exists(psql_conf_file):
            raise RuntimeError(f"A config already exists at {psql_conf_file}. Database has been initialized already?")

        initdb_path = self._get_tool("initdb")

        cmd = [initdb_path, "-D", self.config.data_directory, "--auth", "scram-sha-256"]
        if self.config.username is not None:
            cmd += ["--username", self.config.username]

        # Initdb requires passwords come from a file
        env = {"PG_SUPER_PASSWORD": self.config.password}
        cmd += ['--pwfile=<(printf "%s\n" ${PG_SUPER_PASSWORD})']

        retcode, stdout, stderr = self._run_subprocess(cmd, env=env, shell=True)

        if retcode != 0 or "Success." not in stdout:
            err_msg = f"Error initializing a PostgreSQL instance:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

        # Change some configuration options
        psql_conf_path = pathlib.Path(psql_conf_file)
        psql_conf = psql_conf_path.read_text()

        assert "#port = 5432" in psql_conf
        psql_conf = psql_conf.replace("#port = 5432", f"port = {self.config.port}")

        # Change the location of the socket file
        # Some OSs/Linux distributions will use a directory not writeable by a normal user
        sock_dir = os.path.join(self.config.data_directory, "sock")
        os.makedirs(sock_dir, exist_ok=True)

        # Only use sockets if the sock_dir path would be less than 103 bytes
        # More is put after the directory, so leave some margin there
        if len(sock_dir) < 80:
            psql_conf = re.sub(
                r"#?unix_socket_directories =.*",
                f"unix_socket_directories = '{sock_dir}'",
                psql_conf,
                re.M,
            )
        else:
            psql_conf = re.sub(
                r"#?unix_socket_directories =.*",
                f"unix_socket_directories = ''",
                psql_conf,
                re.M,
            )

        psql_conf_path.write_text(psql_conf)

        # Start the postgres instance
        self.start()
        self._logger.info("Postgresql instance successfully initialized and started")

    def backup_database(self, filepath: str) -> None:
        """
        Backs up the database into a file

        Parameters
        ----------
        filepath
            File to store the backup to. Must not exist
        """

        filepath = os.path.realpath(filepath)
        if os.path.exists(filepath):
            raise RuntimeError(f"Path {filepath} exists already, so cannot back up to there")

        cmds = [
            self._get_tool("pg_dump"),
            "-Fc",  # Custom postgres format, fast
            "--dbname",
            self.database_dsn,  # Yes, dbname can take the psycopg2 uri like "host=localhost, port=5432"
            "--file",
            filepath,
        ]

        self._logger.debug(f"pg_backup command: {'  '.join(cmds)}")
        retcode, stdout, stderr = self._run_subprocess(cmds)

        if retcode != 0:
            err_msg = f"Error backing up the database\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def restore_database(self, filepath) -> None:
        if not os.path.exists(filepath):
            raise RuntimeError(f"Backup file {filepath} does not exist or is not a file")

        # Create the database, but not the tables. These are in the backup file, and may represent
        # a different version
        self.create_database(create_tables=False)

        cmds = [
            self._get_tool("pg_restore"),
            "-e",
            "-x",
            "-O",
            "--dbname",
            self.database_dsn,  # Yes, dbname can take the psycopg2 uri like "host=localhost, port=5432"
            filepath,
        ]

        self._logger.debug(f"pg_restore command: {'  '.join(cmds)}")
        retcode, stdout, stderr = self._run_subprocess(cmds)

        if retcode != 0:
            err_msg = f"Error restoring the database\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def database_size(self) -> int:
        """
        Returns the size of the database in bytes
        """

        # sql_command returns a list of tuples
        return self.sql_command(f"SELECT pg_database_size('{self.config.database_name}');")[0][0]


def create_snowflake_postgres(host: str, data_dir: str) -> PostgresHarness:
    """Create and Initialize a postgres instance in a particular directory

    Parameters
    ----------
    host
        The host name or IP address to bind to
    data_dir
        Path to the directory to store the database data
    """

    sock_dir = os.path.join(data_dir, "sock")

    # There is a limit of 103 bytes for a path to a socket file
    # There's extra put after sock_dir, so be conservative and if it's too long,
    # use the regular host
    if len(sock_dir) < 80:
        db_host = sock_dir
    else:
        db_host = host

    port = find_open_port(host)
    db_config = {
        "port": port,
        "data_directory": data_dir,
        "base_folder": data_dir,
        "own": True,
        "host": db_host,
        "username": "qcfractal_snowflake",
        "password": secrets.token_urlsafe(32),
    }

    config = DatabaseConfig(**db_config)
    pg_harness = PostgresHarness(config)
    pg_harness.initialize_postgres()
    return pg_harness
