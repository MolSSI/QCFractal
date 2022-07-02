from __future__ import annotations

import logging
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import weakref
from typing import TYPE_CHECKING

import psycopg2

from .config import DatabaseConfig
from .db_socket.socket import SQLAlchemySocket
from .port_util import find_open_port, is_port_inuse

if TYPE_CHECKING:
    from typing import Any, List, Optional, Tuple
    import psycopg2.extensions


def replace_db_in_uri(uri: str, new_dbname: str) -> str:
    """
    Replaces the database name part of a URI with a different database name

    The rest of the URI (host, password, etc) all remain the same.

    For example, can turn postgresql://user:pass@127.0.0.1/some_db into
    postgresql://user:pass@127.0.0.1/postgres

    Parameters
    ----------
    uri: str
        The base URI to use
    new_dbname: str
        The database name to replace with

    Returns
    -------
    :
        A new URI with the database name replaced
    """

    components = urllib.parse.urlparse(uri)
    components = components._replace(path=new_dbname)
    return urllib.parse.urlunparse(components)


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

    def _run_subprocess(self, command: List[str]) -> Tuple[int, str, str]:
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

        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._logger.debug("Running subprocess: " + str(command))
        stdout = proc.stdout.decode()
        stderr = proc.stderr.decode()
        if len(stdout) > 0:
            self._logger.info(stdout)
        if len(stderr) > 0:
            self._logger.info(stderr)

        return proc.returncode, stdout, stderr

    @property
    def database_uri(self) -> str:
        """Provides the full PostgreSQL URI string."""
        return self.config.uri

    @staticmethod
    def connect(uri) -> psycopg2.extensions.connection:
        """Builds a psycopg2 connection object.

        Parameters
        ----------
        uri
            The database to URI to connect to

        Returns
        -------
        :
            A live psycopg2 connection
        """

        # Note that we can just use the URI here. The docs are a little misleading
        return psycopg2.connect(uri)

    def is_alive(self, check_database: bool = True) -> bool:
        """Checks if the postgres is alive, and optionally if the database is present.

        If check_database is True, then we will check to see if the database specified
        in the configuration exists. Otherwise, we will just check to see that the postgres
        instance is running and that we can connect to it

        Parameters
        ----------
        check_database
            If true, check to see if the database specified in the config exists.

        Returns
        -------
        :
            True if the instance is alive and that the database exists (if check_database is True)
        """
        try:
            uri = self.config.uri
            if not check_database:
                # We try to connect to the "postgres" database which should always exist
                uri = replace_db_in_uri(uri, "postgres")
            self.connect(uri)
            return True
        except psycopg2.OperationalError as e:
            return False

    def ensure_alive(self):
        """
        Checks to see that the postgres instance is up and running. Does not check anything
        about the database.

        If it is not running, but we are expected to control the database, it will be started.

        Will raise an exception if the database is not alive and/or could not be started
        """

        # if own = True, we are responsible for starting the db instance
        # But don't start it if it is already alive. It may have been started
        # elsewhere.
        if not self.is_alive(False):
            if self.config.own:
                self.start()
                self._logger.info(f"Started a postgres instance for uri {self.config.safe_uri}")
            else:
                raise RuntimeError(
                    f"A running postgres instance serving uri {self.config.safe_uri} does not appear to be running. "
                    f"It must be running for me to continue "
                )

        self._logger.info(f"Database serving uri {self.config.safe_uri} appears to be up and running")

    def sql_command(
        self, statement: str, database_name: Optional[str] = None, autocommit: bool = True, returns=True
    ) -> Any:
        """Runs a single SQL query or statement string and returns the output

        Parameters
        ----------
        statement
            A psql command/query string.
        database_name: Optional[str]:
            Connect to an alternate database name rather than the one specified in the config
        autocommit
            If true, enable autocommit on the connection
        returns
            If true, fetch the results and return them. Set to False for commands that
            don't return anything.
        """

        uri = self.config.uri
        if database_name:
            uri = replace_db_in_uri(uri, database_name)

        conn = self.connect(uri)
        if autocommit:
            conn.autocommit = True
        cursor = conn.cursor()

        self._logger.debug(f"Executing SQL: {statement}")
        cursor.execute(statement)
        if returns:
            return cursor.fetchall()

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

        psql_cmd = self._get_tool("pg_ctl")
        all_cmds = [psql_cmd, "-l", self.config.logfile, "-D", self.config.data_directory]
        all_cmds.extend(cmds)
        return self._run_subprocess(all_cmds)

    def create_database(self, create_tables: bool = True):
        """Creates a new qcarchive database (and tables) given in the configuration.

        The postgres instance must be up and running.

        If the database is existing, no changes to the database are made.

        If there is an error, an exception in raised

        Parameters
        ----------
        create_tables
            If true, create the tables (schema) as well
        """

        # First we connect to the 'postgres' database
        # The database specified in the config may not exist yet
        pg_uri = replace_db_in_uri(self.config.uri, "postgres")
        conn = self.connect(pg_uri)
        conn.autocommit = True

        cursor = conn.cursor()

        # Now we can search the pg_catalog to see if the database we want exists yet
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.config.database_name}'")
        exists = cursor.fetchone()

        if not exists:
            self._logger.info(f"Database {self.config.database_name} does not exist. Creating...")
            cursor.execute(f"CREATE DATABASE {self.config.database_name}")
            self._logger.info(f"Database {self.config.database_name} created")

            if create_tables:
                SQLAlchemySocket.create_database_tables(self.config)
        else:
            self._logger.info(f"Database {self.config.database_name} already exists, so I am leaving it alone")

        # Check to see that everything is ok
        if not self.is_alive():
            raise RuntimeError("I created the database, but now it is not alive? Maybe check the postgres logs")

        cursor.close()

    def delete_database(self) -> None:
        """
        Deletes the database

        This will delete all data associated with the database!
        """
        # First we connect to the 'postgres' database
        # The database specified in the config may not exist yet
        pg_uri = replace_db_in_uri(self.config.uri, "postgres")
        conn = self.connect(pg_uri)
        conn.autocommit = True

        cursor = conn.cursor()

        # Now we can search the pg_catalog to see if the database we want exists yet
        self._logger.info(f"Deleting/Dropping database {self.config.database_name}")

        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.config.database_name}")
        except psycopg2.OperationalError as e:
            raise RuntimeError(f"Could not delete database. Was it still open somewhere? Error: {str(e)}")

    def start(self) -> None:
        """
        Starts a PostgreSQL server based off the current configuration parameters.

        The PostgreSQL server must be initialized and the configured port open. The database does not need to exist.
        """

        # We should only do this if we are in charge of the database itself
        assert self.config.own

        # Startup the server
        self._logger.info("Starting the PostgreSQL instance")

        # We should be in charge of this postgres process. If something is running, then that is a problem
        if is_port_inuse(self.config.host, self.config.port):
            raise RuntimeError(
                f"A process is already running on 'port:{self.config.port}` that is not associated with this QCFractal instance's database"
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
                if self.is_alive(False):
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
        """Shuts down the current postgres instance."""

        # We don't manage the database
        if self.config.own is False or self._started_db is False:
            return

        if self.is_alive(False) is False:
            return

        retcode, stdout, stderr = self.pg_ctl(["stop"])
        if retcode == 0:
            self._logger.info("PostgreSQL successfully stopped")
        else:
            err_msg = f"Error stopping the postgres instance:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def postgres_initialized(self):
        psql_conf_file = os.path.join(self.config.data_directory, "postgresql.conf")
        return os.path.exists(psql_conf_file)

    def initialize_postgres(self) -> None:
        """Initializes a postgresql instance and starts it

        The data directory and port from the configuration is used for the postgres instance

        This does not create the QCFractal database or its tables
        """

        # Can only initialize if we are expected to manage it
        assert self.config.own

        self._logger.info("Initializing the Postgresql database")

        psql_conf_file = os.path.join(self.config.data_directory, "postgresql.conf")

        if os.path.exists(psql_conf_file):
            raise RuntimeError(f"A config already exists at {psql_conf_file}. Database has been initialized already?")

        initdb_path = self._get_tool("initdb")

        retcode, stdout, stderr = self._run_subprocess([initdb_path, "-D", self.config.data_directory])

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
        psql_conf = re.sub(
            r"#?unix_socket_directories =.*",
            f"unix_socket_directories = '{self.config.data_directory}'",
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
            "-h",
            self.config.host,
            "-p",
            str(self.config.port),
            "-d",
            self.config.database_name,
            "-Fc",  # Custom postgres format, fast
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
            f"--host={self.config.host}",
            f"--port={self.config.port}",
            f"--dbname={self.config.database_name}",
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

        sql = f"SELECT pg_database_size('{self.config.database_name}');"
        size = self.sql_command(sql, "postgres")

        # sql_command returns a list of tuples
        return size[0][0]


class TemporaryPostgres:
    """
    This class is used to create a temporary PostgreSQL instance and database. On destruction,
    the database and all its data will be deleted.
    """

    def __init__(self, data_dir: Optional[str] = None):
        """A PostgreSQL instance run in a temporary folder.

        ! Warning ! All data is lost when this object is deleted.

        Parameters
        ----------
        data_dir: str, optional
            Path to the directory to store the database data. If not provided, one will
            be created (and it will be deleted afterwards)
        """

        logger = logging.getLogger(__name__)

        if data_dir:
            self._data_dir = data_dir
            self._data_tmpdir = None
        else:
            self._data_tmpdir = tempfile.TemporaryDirectory()
            self._data_dir = self._data_tmpdir.name

        port = find_open_port()
        db_config = {"port": port, "data_directory": self._data_dir, "base_folder": self._data_dir, "own": True}

        self._config = DatabaseConfig(**db_config)
        self._harness = PostgresHarness(self._config)
        self._harness.initialize_postgres()

        logger.info(f"Created temporary postgres database at location {self._data_dir} running on port {port}")

        self._finalizer = weakref.finalize(self, self._stop, self._data_tmpdir, self._harness)

    def database_uri(self, safe: bool = True) -> str:
        """Provides the full Postgres URI string.

        Parameters
        ----------
        safe : bool, optional
            If True, hides the postgres password.

        Returns
        -------
        str
            The database URI
        """

        if safe:
            return self._config.safe_uri
        else:
            return self._config.uri

    def stop(self):
        """
        Stops and deletes the temporary database. Once done, it cannot be started again
        """

        # This will call the finalizer, and then detach the finalizer. This object
        # is pretty much dead after that
        self._finalizer()

    @classmethod
    def _stop(cls, tmpdir, harness) -> None:
        ####################################################################################
        # This is written as a class method so that it can be called by a weakref finalizer
        ####################################################################################

        harness.shutdown()

        if tmpdir is not None:
            tmpdir.cleanup()
