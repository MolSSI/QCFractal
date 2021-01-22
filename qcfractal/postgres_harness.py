import atexit
import os
import shutil
import pathlib
import subprocess
import tempfile
import time
import re
import logging
from typing import Any, Dict, List, Optional, Union

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from qcfractal.storage_sockets.models import Base, VersionsORM

from .config import DatabaseConfig
from .port_util import find_port, is_port_open


class PostgresHarness:
    def __init__(self, config: DatabaseConfig):
        """A flexible connection to a PostgreSQL server

        Parameters
        ----------
        config : DatabaseConfig
            The configuration options
        quiet : bool, optional
            If True, does not log any operations
        logger : print, optional
            The logger to show the operations to.
        """
        self.config = config
        self._logger = logging.getLogger('PostgresHarness')
        self._checked = False

        self._alembic_ini = os.path.join(os.path.abspath(os.path.dirname(__file__)), "alembic.ini")

    def _run(self, commands):
        proc = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = proc.stdout.decode()
        self._logger.info(stdout)

        ret = {"retcode": proc.returncode, "stdout": stdout, "stderr": proc.stderr.decode()}

        return ret

    def _check_psql(self) -> None:
        """
        Checks to see if the proper PostgreSQL commands are present. Raises a ValueError if they are not found.
        """

        if self.config.host != "localhost":
            raise ValueError(f"Cannot modify PostgreSQL as configuration points to non-localhost: {self.config.host}")

        if self._checked:
            return

        msg = """
Could not find 'pg_ctl' in the current path. Please install PostgreSQL with 'conda install postgresql'.

Alternatively, you can install a system PostgreSQL manually, please see the following link: https://www.postgresql.org/download/
"""

        if shutil.which("pg_ctl") is None:
            raise ValueError(msg)
        else:
            self._checked = True

    def database_uri(self) -> str:
        """Provides the full PostgreSQL URI string.

        Returns
        -------
        str
            The database URI
        """
        return self.config.uri

    def connect(self, database: Optional[str] = None) -> "Connection":
        """Builds a psycopg2 connection object.

        Parameters
        ----------
        database : Optional[str], optional
            The database to connect to, otherwise defaults to None

        Returns
        -------
        Connection
            A live Connection object.
        """
        if database is None:
            database = "postgres"
        return psycopg2.connect(
            database=database,
            user=self.config.username,
            host=self.config.host,
            port=self.config.port,
            password=self.config.password,
        )

    def is_alive(self, database: Optional[str] = None) -> bool:
        """Checks if the postgres is alive, and optionally if the database is present.

        Parameters
        ----------
        database : Optional[str], optional
            The datbase to connect to

        Returns
        -------
        bool
            If True, the postgres database is alive.
        """
        try:
            self.connect(database=database)
            return True
        except psycopg2._psycopg.OperationalError:
            return False

    def command(self, cmd: Union[str, List[str]], check: bool = True) -> Any:
        """Runs psql commands and returns their output while connected to the correct postgres instance.

        Parameters
        ----------
        cmd : str
            A psql command string.
            Description

        """
        self._check_psql()

        if isinstance(cmd, str):
            cmd = [cmd]

        self._logger.debug(f"pqsl command: {cmd}")
        psql_cmd = [shutil.which("psql"), "-p", str(self.config.port), "-c"]

        cmd = self._run(psql_cmd + cmd)
        if check:
            if cmd["retcode"] != 0:
                raise ValueError("psql operation did not complete.")
        return cmd

    def pg_ctl(self, cmds: List[str]) -> Any:
        """Runs pg_ctl commands and returns their output while connected to the correct postgres instance.

        Parameters
        ----------
        cmds : List[str]
            A list of PostgreSQL pg_ctl commands to run.
        """
        self._check_psql()

        self._logger.debug(f"pg_ctl command: {cmds}")
        psql_cmd = [shutil.which("pg_ctl"), "-l", self.config.logfile, "-D", self.config.data_directory]
        return self._run(psql_cmd + cmds)

    def create_database(self, database_name: str) -> bool:
        """Creates a new database for the current postgres instance. If the database is existing, no
        changes to the database are made.

        Parameters
        ----------
        database_name : str
            The name of the database to create.

        Returns
        -------
        bool
            If the operation was successful or not.
        """
        conn = self.connect()
        conn.autocommit = True

        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{database_name}'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {database_name}")

        return self.is_alive(database=database_name)

    def create_tables(self):
        """Create database tables using SQLAlchemy models"""

        uri = self.config.uri
        self._logger.info(f"Creating tables for database: {uri}")
        engine = create_engine(uri, echo=False, pool_size=1)

        # actually create the tables
        try:
            Base.metadata.create_all(engine)
        except Exception as e:
            raise ValueError(f"SQLAlchemy Connection Error\n {str(e)}")

        return True

    def update_db_version(self):
        """Update current version of QCFractal that is stored in the database

        This does not actually perform the upgrade, but will store the current versions of the software stack
        (qcengine, qcelemental, qcfractal) into the database
        """

        uri = self.config.uri

        engine = create_engine(uri, echo=False, pool_size=1)
        session = sessionmaker(bind=engine)()
        try:
            import qcfractal, qcelemental, qcengine

            elemental_version = qcelemental.__version__
            fractal_version = qcfractal.__version__
            engine_version = qcengine.__version__

            self._logger.info(f"Updating current version of QCFractal in DB: {uri} \n" f"to version {qcfractal.__version__}")
            current_ver = VersionsORM(
                elemental_version=elemental_version, fractal_version=fractal_version, engine_version=engine_version
            )
            session.add(current_ver)
            session.commit()
        except Exception as e:
            raise ValueError(f"Failed to Update DB version.\n {str(e)}")
        finally:
            session.close()

        return True

    def upgrade(self):
        """
        Upgrade the database schema using the latest alembic revision.
        The database data won't be deleted.
        """

        ret = self._run(self.alembic_commands() + ["upgrade", "head"])

        if ret["retcode"] != 0:
            self._logger.error(ret["stderr"])
            raise ValueError(
                f"\nFailed to Upgrade the database, make sure to init the database first before being able to upgrade it.\n"
            )

        return True

    def start(self) -> Any:
        """
        Starts a PostgreSQL server based off the current configuration parameters. The server must be initialized
        and the configured port open.
        """

        self._check_psql()

        # Startup the server
        self._logger.info("Starting the database:")

        if is_port_open(self.config.host, self.config.port):
            self._logger.info("Service currently running the configured port, current_status:\n")
            status = self.pg_ctl(["status"])

            # If status is ok, exit is 0
            if status["retcode"] != 0:
                raise ValueError(
                    f"A process is already running on 'port:{self.config.port}` that is not associated with the PostgreSQL instance at `location:{self.config.data_directory}.`"
                    "\nThis often happens when two PostgreSQL databases are attempted to run on the same port."
                    "\nEither shut down the other PostgreSQL database or change the settings in the qcfractal configuration."
                    "\nStopping."
                )

            if not self.is_alive():
                raise ValueError(f"PostgreSQL does not is running, but cannot connect to it.")

            self._logger.info("Found running PostgreSQL instance with correct configuration.")

        else:

            start_status = self.pg_ctl(["start"])

            if not (("server started" in start_status["stdout"]) or ("server starting" in start_status["stdout"])):
                raise ValueError(
                    f"Could not start the PostgreSQL server. Error below:\n\n{start_status['stderr']}\n\nstdout:\n\n{start_status['stdout']}"
                )

            # Check that we are alive
            for x in range(10):
                if self.is_alive():
                    break
                else:
                    time.sleep(0.1)
            else:
                raise ValueError(
                    f"Could not connect to the server after booting. Boot log:\n\n{start_status['stderr']}"
                )

            self._run([shutil.which("pg_ctl"), "-D", str(self.config.data_directory), "status"])
            self._logger.info("PostgreSQL successfully started in a background process, current_status:\n")

        return True

    def shutdown(self) -> Any:
        """Shutsdown the current postgres instance."""

        self._check_psql()

        ret = self.pg_ctl(["stop"])
        return ret

    def initialize_postgres(self) -> None:
        """Initializes and starts the current postgres instance."""

        self._check_psql()

        self._logger.info("Initializing the Postgresql database:")

        # Initialize the database
        init_status = self._run([shutil.which("initdb"), "-D", self.config.data_directory])
        if "Success." not in init_status["stdout"]:
            raise ValueError(f"Could not initialize the PostgreSQL server. Error below:\n\n{init_status['stderr']}")

        # Change some configuration options
        psql_conf_file = os.path.join(self.config.data_directory, "postgresql.conf")
        psql_conf_path = pathlib.Path(psql_conf_file)
        psql_conf = psql_conf_path.read_text()

        if self.config.port != 5432:
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

        # Start the database
        self.start()

        # Create the user and database
        self._logger.info(f"Building user information.")
        self._run([shutil.which("createdb"), "-p", str(self.config.port)])

        success = self.create_database(self.config.database_name)

        if success is False:
            self.shutdown()
            raise ValueError("Database created successfully, but could not connect. Shutting down postgres.")

        self._logger.info("\nDatabase server successfully started!")

    def alembic_commands(self) -> List[str]:
        db_uri = self.config.uri
        return [shutil.which("alembic"), "-c", self._alembic_ini, "-x", "uri=" + db_uri]

    def init_database(self) -> None:

        # TODO: drop tables

        # create models
        self.create_tables()

        # update alembic_version table with the current version
        self._logger.info(f"\nStamping Database with current version..")

        ret = self._run(self.alembic_commands() + ["stamp", "head"])

        if ret["retcode"] != 0:
            self._logger.error(ret)
            raise ValueError("\nFailed to Stamp the database with current version.\n")

    def backup_database(self, filename: Optional[str] = None) -> None:

        # Reasonable check here
        self._check_psql()

        if filename is None:
            filename = f"{self.config.database_name}.bak"

        filename = os.path.realpath(filename)

        # fmt: off
        cmds = [
            shutil.which("pg_dump"),
            "-p", str(self.config.port),
            "-d", self.config.database_name,
            "-Fc", # Custom postgres format, fast
            "--file", filename
        ]
        # fmt: on

        self._logger.debug(f"pg_backup command: {'  '.join(cmds)}")
        ret = self._run(cmds)

        if ret["retcode"] != 0:
            self._logger.debug(str(ret))
            raise ValueError("\nFailed to backup the database.\n")

    def restore_database(self, filename) -> None:

        # Reasonable check here
        self._check_psql()

        self.create_database(self.config.database_name)

        # fmt: off
        cmds = [
            shutil.which("pg_restore"),
            f"--port={self.config.port}",
            f"--dbname={self.config.database_name}",
            filename
        ]
        # fmt: on

        self._logger.debug(f"pg_backup command: {'  '.join(cmds)}")
        ret = self._run(cmds)

        if ret["retcode"] != 0:
            self._logger.debug(ret["stderr"])
            raise ValueError("\nFailed to restore the database.\n")

    def database_size(self) -> str:
        """
        Returns a pretty formatted string of the database size.
        """

        return self.command(
            [f"SELECT pg_size_pretty( pg_database_size('{self.config.database_name}') );", "-t", "-A"]
        )


class TemporaryPostgres:
    def __init__(
        self,
        data_dir: Optional[str] = None
    ):
        """A PostgreSQL instance run in a temporary folder.

        ! Warning ! All data is lost when this object is deleted.

        Parameters
        ----------
        data_dir: str, optional
            Path to the directory to store the database data. If not provided, one will
            be created
        """

        self._active = True

        if data_dir:
            self._data_dir = data_dir
        else:
            self._data_tmpdir = tempfile.TemporaryDirectory()
            self._data_dir = self._data_tmpdir.name

        db_config = {"port": find_port(), "data_directory": self._data_dir}
        db_config["base_directory"] = self._data_dir
        db_config["database_name"] = 'qcfractal_tmp_db'

        self.config = DatabaseConfig(**db_config)
        self.psql = PostgresHarness(self.config)
        self.psql.initialize_postgres()
        self.psql.init_database()

        atexit.register(self.stop)

    def __del__(self):
        """
        Cleans up the TemporaryPostgres instance on delete.
        """

        self.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    def database_uri(self, safe: bool = True, database: Optional[str] = None) -> str:
        """Provides the full Postgres URI string.

        Parameters
        ----------
        safe : bool, optional
            If True, hides the postgres password.
        database : Optional[str], optional
            An optional database to add to the string.

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

        if not self._active:
            return

        self.psql.shutdown()

        # Closed down
        self._active = False
        atexit.unregister(self.stop)

