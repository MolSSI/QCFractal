from __future__ import annotations

import os
import shutil
import pathlib
import subprocess
import tempfile
import time
import re
import logging
import urllib.parse

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from qcfractal.storage_sockets.models import Base, VersionsORM

from .config import DatabaseConfig
from .port_util import find_port, is_port_open

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Any, List, Optional, Tuple
    import psycopg2.extensions


def replace_db_in_uri(uri: str, new_dbname: str) -> str:
    """
    Replaces the database name part of a URI with a different database name

    The rest of the URI (host, password, etc) all remain the same.

    For example, can turn postgres://user:pass@127.0.0.1/some_db into
    postgres://user:pass@127.0.0.1/postgres

    Parameters
    ----------
    uri: str
        The base URI to use
    new_dbname: str
        The database name to replace with

    Returns
    -------
    str
        A new URI with the database name replaced
    """

    components = urllib.parse.urlparse(uri)
    components = components._replace(path=new_dbname)
    return urllib.parse.urlunparse(components)


def db_uri_base(uri: str) -> str:
    """
    Returns the base part of a uri (scheme, user, password, host, port).

    That is, returns the URI minus the database name at the end

    Parameters
    ----------
    uri: str
        The base URI to use

    Returns
    -------
    str
        The base part of the URI (without the database name)
    """

    return replace_db_in_uri(uri, "")


class PostgresHarness:
    def __init__(self, config: DatabaseConfig):
        """A flexible connection to a PostgreSQL server

        Parameters
        ----------
        config : DatabaseConfig
            The configuration options
        """
        self.config = config
        self._logger = logging.getLogger("PostgresHarness")
        self._alembic_ini = os.path.join(os.path.abspath(os.path.dirname(__file__)), "alembic.ini")

    def _get_tool(self, tool: str) -> str:
        """
        Obtains the path to a postgres tool (such as pg_ctl)

        If 'pg_tool_dir' is not specified, the current PATH environment is searched instead

        If the tool is not found, an exception is raised

        Parameters
        ----------
        tool: str
            Tool to search for (ie, psql)

        Returns
        -------
            Full path to the tools executable
        """

        tool_path = shutil.which(tool, path=self.config.pg_tool_dir)
        if tool_path is None:
            raise RuntimeError(
                f"Postgresql tool/command {tool} cannot be found. Postgresql may not be installed, or pg_tool_dir is incorrect. If you do have Postgrsql, try setting pg_tool_dir in the configuration to the directory containing psql, pg_ctl, etc"
            )
        return tool_path

    def _run_subprocess(self, command: List[str]) -> Tuple[int, str, str]:
        """
        Runs a command using subprocess, and output stdout into the logger

        Parameters
        ----------
        command: List[str]
            Command to run as a list of strings (see documentation for subprocess)

        Returns
        -------
        Tuple[int, str, str]
            Return code, stdout, and stderr as a Tuple
        """

        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._logger.debug("Running subprocess: " + str(command))
        stdout = proc.stdout.decode()
        stderr = proc.stderr.decode()
        self._logger.info(stdout)
        self._logger.error(stderr)

        return proc.returncode, stdout, stderr

    def database_uri(self) -> str:
        """Provides the full PostgreSQL URI string."""
        return self.config.uri

    @staticmethod
    def connect(uri) -> psycopg2.extensions.connection:
        """Builds a psycopg2 connection object.

        Parameters
        ----------
        uri :str
            The database to URI to connect to

        Returns
        -------
        psycopg2.extensions.connection
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
        check_database : Optional[str], optional
            If true, check to see if the database specified in the config exists.

        Returns
        -------
        bool
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
        if self.config.own:
            if self.is_alive(False):
                raise RuntimeError(
                    "I am supposed to start the database, since own = True. However it is already running. Is another postgresql or qcfractal-server process running?"
                )
            self.start()
            self._logger.info(f"Started a postgres instance for uri {self.config.safe_uri}")
        elif not self.is_alive(False):
            raise RuntimeError(
                f"A running postgres instance serving uri {self.config.safe_uri} does not appear to be running. It must be running for me to start"
            )

        self._logger.info(f"Database serving uri {self.config.safe_uri} appears to be up and running")

    def sql_command(self, statement: str, database_name: Optional[str] = None, fail_ok: bool = False) -> Any:
        """Runs a single SQL query or statement string and returns the output

        Parameters
        ----------
        statement: str
            A psql command/query string.
        database_name: Optional[str]:
            Connect to an alternate database name rather than the one specified in the config
        fail_ok: bool
            If False, raise an exception if there is an error
        """

        uri = self.config.uri
        if database_name:
            uri = replace_db_in_uri(uri, database_name)

        conn = self.connect(uri)
        cursor = conn.cursor()

        self._logger.debug(f"Executing SQL: {statement}")
        cursor.execute(statement)
        r = cursor.fetchall()
        return r

    def pg_ctl(self, cmds: List[str]) -> Tuple[int, str, str]:
        """Runs a pg_ctl command and returns its output

        Parameters
        ----------
        cmds : List[str]
            A list of PostgreSQL pg_ctl commands to run.

        Returns
        -------
        Tuple[int, str, str]
            The return code, stdout, and stderr returned by pg_ctl
        """

        # pg_ctl should only be run if we are managing the db
        assert self.config.own
        self._logger.debug(f"Running pg_ctl command: {cmds}")

        psql_cmd = self._get_tool("pg_ctl")
        all_cmds = [psql_cmd, "-l", self.config.logfile, "-D", self.config.data_directory]
        all_cmds.extend(cmds)
        return self._run_subprocess(all_cmds)

    def create_database(self):
        """Creates a new qcarchive database given in the configuration.

        The postgres instance must be up and running.

        If the database is existing, no changes to the database are made.

        If there is an error, an exception in raised
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
            self._init_database()
        else:
            self._logger.info(f"Database {self.config.database_name} already exists, so I am leaving it alone")

        # Check to see that everything is ok
        if not self.is_alive():
            raise RuntimeError("I created the database, but now it is not alive? Maybe check the postgres logs")


    def _update_db_version(self) -> None:
        """Update current version of QCFractal that is stored in the database

        This does not actually perform the upgrade, but will store the current versions of the software stack
        (qcengine, qcelemental, qcfractal) into the database
        """


        # TODO: Move some of this to the socket (this uses ORM)
        uri = self.config.uri

        engine = create_engine(uri, echo=False, pool_size=1)
        session = sessionmaker(bind=engine)()
        try:
            import qcfractal
            import qcelemental
            import qcengine

            elemental_version = qcelemental.__version__
            fractal_version = qcfractal.__version__
            engine_version = qcengine.__version__

            self._logger.info(
                f"Updating current version of QCFractal in DB: {uri} \n" f"to version {qcfractal.__version__}"
            )
            current_ver = VersionsORM(
                elemental_version=elemental_version, fractal_version=fractal_version, engine_version=engine_version
            )
            session.add(current_ver)
            session.commit()
        except Exception as e:
            raise ValueError(f"Failed to Update DB version.\n {str(e)}")
        finally:
            session.close()

    def upgrade(self) -> None:
        """
        Upgrade the database schema using the latest alembic revision.
        """

        retcode, _, _ = self._run_subprocess(self.alembic_commands() + ["upgrade", "head"])

        if retcode != 0:
            raise RuntimeError(
                f"Failed to Upgrade the database, make sure to init the database first before being able to upgrade it."
            )

        self._update_db_version()

    def start(self) -> None:
        """
        Starts a PostgreSQL server based off the current configuration parameters.

        The PostgreSQL server must be initialized and the configured port open. The database does not need to exist.
        """

        # We should only do this if we are in charge of the database itself
        assert self.config.own

        # Startup the server
        self._logger.info("Starting the database")

        # We should be in charge of this postgres process. If something is running, then that is a problem
        if is_port_open(self.config.host, self.config.port):
            raise RuntimeError(
                f"A process is already running on 'port:{self.config.port}` that is not associated with this QCFractal instance's database"
            )
        else:
            retcode, stdout, stderr = self.pg_ctl(["start"])

            err_msg = f"Error starting database. Did you remember to initialize it (qcfractal-server init)?:\noutput:\n{stdout}\nstderr:\n{stderr}"

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

    def shutdown(self) -> None:
        """Shuts down the current postgres instance."""

        # We should only do this if we are in charge of the database itself
        assert self.config.own

        if self.config.own is False:
            return

        retcode, stdout, stderr = self.pg_ctl(["stop"])
        if retcode == 0:
            self._logger.info("PostgreSQL successfully stopped")
        else:
            err_msg = f"Error stopping the postgres instance:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def initialize_postgres(self) -> None:
        """Initializes a postgresql instance and starts it

        The data directory and port from the configuration is used for the postgres instance
        """

        # Can only initialize if we are expected to manage it
        assert self.config.own

        self._logger.info("Initializing the Postgresql database")

        initdb_path = self._get_tool("initdb")
        createdb_path = self._get_tool("createdb")
        retcode, stdout, stderr = self._run_subprocess([initdb_path, "-D", self.config.data_directory])

        if retcode != 0 or "Success." not in stdout:
            err_msg = f"Error initializing a PostgreSQL instance:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

        # Change some configuration options
        psql_conf_file = os.path.join(self.config.data_directory, "postgresql.conf")
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

        # Start the database. It needs to be running for createdb
        self.start()

        # Create the user and database
        self._logger.info(f"Building database user information & creating QCFractal database")
        try:
            retcode, stdout, stderr = self._run_subprocess([createdb_path, "-h", "localhost", "-p", str(self.config.port)])
            if retcode != 0:
                err_msg = f"Error running createdb:\noutput:\n{stdout}\nstderr:\n{stderr}"
                raise RuntimeError(err_msg)

            self.create_database()
        except Exception:
            self.shutdown()
            raise

        self._logger.info("Postgresql instance successfully initialized and started")

    def alembic_commands(self) -> List[str]:
        """
        Get the components of an alembic command that can be passed to _run_subprocess

        This will find the almembic command and also add the uri and alembic configuration information
        to the command line.

        Returns
        -------
        List[str]
            Components of an alembic command line as a list of strings
        """

        db_uri = self.config.uri
        alembic_path = shutil.which("alembic")

        if alembic_path is None:
            raise RuntimeError("Cannot find the 'alembic' command. Is it installed?")
        return [alembic_path, "-c", self._alembic_ini, "-x", "uri=" + db_uri]

    def _init_database(self) -> None:
        """
        Creates the actual database and tables for use by this QCFractal instance
        """

        # create the tables via sqlalchemy
        uri = self.config.uri
        self._logger.info(f"Creating tables for database: {uri}")
        engine = create_engine(uri, echo=False, pool_size=1)

        try:
            Base.metadata.create_all(engine)
        except Exception as e:
            raise RuntimeError(f"SQLAlchemy Connection Error\n{str(e)}")

        # update alembic_version table with the current version
        self._logger.debug(f"Stamping Database with current version")
        retcode, stdout, stderr = self._run_subprocess(self.alembic_commands() + ["stamp", "head"])

        if retcode != 0:
            err_msg = f"Error stamping the database with the current version:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def backup_database(self, filepath: str) -> None:
        """
        Backs up the database into a file

        Parameters
        ----------
        filepath: str
            File to store the backup to. Must not exist
        """

        # Only do this if we manage the database ourselves
        assert self.config.own

        filepath = os.path.realpath(filepath)
        if os.path.exists(filepath):
            raise RuntimeError(f"Path {filepath} exists already, so cannot back up to there")

        cmds = [
            self._get_tool("pg_dump"),
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

        # Only do this if we manage the database ourselves
        assert self.config.own

        self.create_database()

        cmds = [
            self._get_tool("pg_restore"),
            f"--port={self.config.port}",
            f"--dbname={self.config.database_name}",
            filepath,
        ]

        self._logger.debug(f"pg_backup command: {'  '.join(cmds)}")
        retcode, stdout, stderr = self._run_subprocess(cmds)

        if retcode != 0:
            err_msg = f"Error restoring the database\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    def database_size(self) -> str:
        """
        Returns a pretty formatted string of the database size.
        """

        sql = f"SELECT pg_size_pretty( pg_database_size('{self.config.database_name}') );"
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
        else:
            self._data_tmpdir = tempfile.TemporaryDirectory()
            self._data_dir = self._data_tmpdir.name

        port = find_port()
        db_config = {"port": port, "data_directory": self._data_dir, "base_directory": self._data_dir,
                     "database_name": "qcfractal_tmp_db", "own": True}

        self.config = DatabaseConfig(**db_config)
        self.psql = PostgresHarness(self.config)
        self.psql.initialize_postgres()
        self.psql.create_database()
        self._active = True
        logger.info(f"Created temporary postgres database at location {self._data_dir} running on port {port}")

    def __del__(self):
        """
        Cleans up the TemporaryPostgres instance on delete.
        """

        self.stop()

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
            return self.config.safe_uri
        else:
            return self.config.uri

    def stop(self) -> None:
        """
        Shuts down the Snowflake instance. This instance is not recoverable after a stop call.
        """

        if self._active:
            self.psql.shutdown()
