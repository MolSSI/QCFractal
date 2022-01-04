"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""

from __future__ import annotations

import importlib
import logging
import os
import shutil
import subprocess
from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, exc, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import qcfractal

if TYPE_CHECKING:
    from typing import Tuple, List, Optional
    from sqlalchemy.orm.session import Session
    from ..config import FractalConfig, DatabaseConfig


class SQLAlchemySocket:
    """
    Main handler for managing/accessing an SQLAlchemy database
    """

    def __init__(self, qcf_config: FractalConfig):
        """
        Constructs a new SQLAlchemy socket
        """
        self.qcf_config = qcf_config

        # By default, disable watching
        self._completed_queue = None

        # Logging data
        self.logger = logging.getLogger("SQLAlchemySocket")
        uri = qcf_config.database.uri

        self.logger.info(f"SQLAlchemy attempt to connect to {uri}.")

        # Connect to DB and create session
        self.uri = uri

        # If the pool size given in the config is zero, then that corresponds to using a NullPool here.
        # Note that this is different than SQLAlchemy, where pool_size = 0 means unlimited
        # If pool_size in the config is non-zero, then set the pool class to None (meaning use
        # SQLAlchemy default)
        if qcf_config.database.pool_size == 0:
            self.engine = create_engine(uri, echo=qcf_config.database.echo_sql, poolclass=NullPool)
        else:
            self.engine = create_engine(uri, echo=qcf_config.database.echo_sql, pool_size=qcf_config.database.pool_size)

        self.logger.info(
            "Connected SQLAlchemy to DB dialect {} with driver {}".format(self.engine.dialect.name, self.engine.driver)
        )

        # Handle multiprocessing w/ sqlalchemy
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
        @event.listens_for(self.engine, "connect")
        def connect(dbapi_connection, connection_record):
            connection_record.info["pid"] = os.getpid()

        @event.listens_for(self.engine, "checkout")
        def checkout(dbapi_connection, connection_record, connection_proxy):
            pid = os.getpid()
            if connection_record.info["pid"] != pid:
                connection_record.dbapi_connection = connection_proxy.dbapi_connection = None
                raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s" % (connection_record.info["pid"], pid)
                )

        self.Session = sessionmaker(bind=self.engine, future=True)

        # Create/initialize the subsockets
        from ..components.molecules.sockets import MoleculeSocket
        from ..components.keywords.sockets import KeywordsSocket
        from ..components.permissions.user_socket import UserSocket
        from ..components.permissions.role_socket import RoleSocket
        from ..components.serverinfo.sockets import ServerInfoSocket
        from ..components.managers.sockets import ManagerSocket
        from ..components.records.sockets import RecordSocket
        from ..components.tasks.sockets import TaskSocket
        from ..components.services.sockets import ServiceSocket
        from ..components.datasets.sockets import DatasetSocket

        self.serverinfo = ServerInfoSocket(self)
        self.keywords = KeywordsSocket(self)
        self.molecules = MoleculeSocket(self)
        self.datasets = DatasetSocket(self)
        self.records = RecordSocket(self)
        self.tasks = TaskSocket(self)
        self.services = ServiceSocket(self)
        self.managers = ManagerSocket(self)
        self.users = UserSocket(self)
        self.roles = RoleSocket(self)

        # check version compatibility
        db_ver = self.serverinfo.check_lib_versions()
        self.logger.info(f"Software versions info in database:")
        for k, v in db_ver.items():
            self.logger.info(f"      {k}: {v}")

        if not qcf_config.database.skip_version_check and qcfractal.__version__ != db_ver["fractal_version"]:
            raise RuntimeError(
                f"You are running QCFractal version {qcfractal.__version__} "
                f'with an older DB version ({db_ver["fractal_version"]}). '
                f'Please run "qcfractal-server upgrade" first before starting the server.'
            )

    @staticmethod
    def _run_subprocess(command: List[str]) -> Tuple[int, str, str]:
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

        logger = logging.getLogger("SQLAlchemySocket")
        proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.debug("Running subprocess: " + str(command))
        stdout = proc.stdout.decode()
        stderr = proc.stderr.decode()
        if len(stdout) > 0:
            logger.info(stdout)
        if len(stderr) > 0:
            logger.info(stderr)

        return proc.returncode, stdout, stderr

    @staticmethod
    def alembic_commands(db_config: DatabaseConfig) -> List[str]:
        """
        Get the components of an alembic command that can be passed to _run_subprocess

        This will find the almembic command and also add the uri and alembic configuration information
        to the command line.

        Returns
        -------
        List[str]
            Components of an alembic command line as a list of strings
        """

        db_uri = db_config.uri

        # Find the path to the almebic ini

        alembic_ini = os.path.join(qcfractal.qcfractal_topdir, "alembic.ini")
        alembic_path = shutil.which("alembic")

        if alembic_path is None:
            raise RuntimeError("Cannot find the 'alembic' command. Is it installed?")
        return [alembic_path, "-c", alembic_ini, "-x", "uri=" + db_uri]

    @staticmethod
    def init_database(db_config: DatabaseConfig):
        logger = logging.getLogger("SQLAlchemySocket")

        # Register all classes that derive from the BaseORM
        importlib.import_module("qcfractal.components.register_all")

        # create the tables via sqlalchemy
        uri = db_config.uri
        logger.info(f"Creating tables for database: {uri}")
        engine = create_engine(uri, echo=False, poolclass=NullPool)
        session = sessionmaker(bind=engine)()

        from qcfractal.db_socket.base_orm import BaseORM
        from qcfractal.components.permissions.db_models import RoleORM
        from qcfractal.components.permissions.role_socket import default_roles

        try:
            BaseORM.metadata.create_all(engine)
        except Exception as e:
            raise RuntimeError(f"SQLAlchemy Connection Error\n{str(e)}")

        try:
            for rolename, permissions in default_roles.items():
                orm = RoleORM(rolename=rolename, permissions=permissions)
                session.add(orm)
            session.commit()
        except Exception as e:
            raise RuntimeError(f"Failed to populate default roles:\n {str(e)}")
        finally:
            session.close()

        # update alembic_version table with the current version
        logger.debug(f"Stamping Database with current version")
        alembic_commands = SQLAlchemySocket.alembic_commands(db_config)
        retcode, stdout, stderr = SQLAlchemySocket._run_subprocess(alembic_commands + ["stamp", "head"])

        if retcode != 0:
            err_msg = f"Error stamping the database with the current version:\noutput:\n{stdout}\nstderr:\n{stderr}"
            raise RuntimeError(err_msg)

    @staticmethod
    def upgrade_database(db_config: DatabaseConfig) -> None:
        """
        Upgrade the database schema using the latest alembic revision.
        """
        logger = logging.getLogger("SQLAlchemySocket")

        alembic_commands = SQLAlchemySocket.alembic_commands(db_config)
        retcode, _, _ = SQLAlchemySocket._run_subprocess(alembic_commands + ["upgrade", "head"])

        if retcode != 0:
            raise RuntimeError(f"Failed to Upgrade the database")

        # Now upgrade the stored version information
        uri = db_config.uri
        engine = create_engine(uri, echo=False, poolclass=NullPool)
        session = sessionmaker(bind=engine)()
        try:
            import qcfractal
            import qcelemental

            elemental_version = qcelemental.__version__
            fractal_version = qcfractal.__version__

            logger.info(f"Updating current version of QCFractal in DB: {uri} \n" f"to version {qcfractal.__version__}")

            from ..components.serverinfo.db_models import VersionsORM

            current_ver = VersionsORM(elemental_version=elemental_version, fractal_version=fractal_version)
            session.add(current_ver)
            session.commit()
        except Exception as e:
            raise ValueError(f"Failed to Update DB version.\n {str(e)}")
        finally:
            session.close()

    def __str__(self) -> str:
        return f"<SQLAlchemySocket: address='{self.uri}`>"

    @contextmanager
    def session_scope(self, read_only: bool = False):
        """Provide a transactional scope"""

        session = self.Session()
        try:
            yield session

            if not read_only:
                session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def optional_session(self, existing_session: Optional[Session], read_only: bool = False):
        """
        Use the existing session if available, otherwise use a new session

        If an existing session is used, it is automatically flushed at the end, but not committed

        This is meant to be used with `with`, and where existing_session may be None.

         .. code-block:: python

            def somefunction(session: Optional[Session]=None):
                # will use session if not None, or will create a new session
                with storage_socket.optional_session(session) as s:
                    s.add(stuff)

        Parameters
        ----------
        existing_session
            An optional, existing sqlalchemy session
        read_only
            If True and a new session is created, it will be a read-only session
        """

        @contextmanager
        def autoflushing_scope(session: Session, read_only: bool):
            """
            Wraps an existing session to flush at the end
            """
            try:
                yield session

                if not read_only:
                    session.flush()
            except:
                raise

        if existing_session is not None:
            return autoflushing_scope(existing_session, read_only)
        else:
            return self.session_scope(read_only)

    def set_completed_watch(self, mp_queue):
        self._completed_queue = mp_queue

    def notify_completed_watch(self, base_result_id, status):
        if self._completed_queue is not None:
            # Don't want to block here. Just put it in the queue and move on
            self._completed_queue.put((int(base_result_id), status), block=False)
