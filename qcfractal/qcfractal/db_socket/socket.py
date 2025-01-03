"""
Main/Root socket for accessing the database
"""

from __future__ import annotations

import importlib
import logging
import os
import shutil
from contextlib import contextmanager
from functools import lru_cache
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, exc, event, inspect, select, union, MetaData, Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import qcfractal

if TYPE_CHECKING:
    from typing import List, Optional, Generator, Any
    from sqlalchemy.orm.session import Session
    from ..config import FractalConfig, DatabaseConfig


class SQLAlchemySocket:
    """
    Main/Root socket accessing/managing an SQLAlchemy database
    """

    def __init__(self, qcf_config: FractalConfig):
        self.qcf_config = qcf_config

        # By default, disable watching
        self._finished_queue = None

        # Logging data
        self.logger = logging.getLogger("SQLAlchemySocket")

        self.logger.info(f"SQLAlchemy attempt to connect to {qcf_config.database.safe_uri}.")

        # Connect to DB and create session
        # If the pool size given in the config is zero, then that corresponds to using a NullPool here.
        # Note that this is different than SQLAlchemy, where pool_size = 0 means unlimited
        # If pool_size in the config is non-zero, then set the pool class to None (meaning use
        # SQLAlchemy default)
        if qcf_config.database.pool_size == 0:
            self.engine = create_engine(
                self.qcf_config.database.sqlalchemy_url,
                echo=qcf_config.database.echo_sql,
                poolclass=NullPool,
                future=True,
            )
        else:
            self.engine = create_engine(
                self.qcf_config.database.sqlalchemy_url,
                echo=qcf_config.database.echo_sql,
                pool_size=qcf_config.database.pool_size,
                future=True,
            )

        self.logger.info(
            "Connected SQLAlchemy to DB dialect {} with driver {}".format(self.engine.dialect.name, self.engine.driver)
        )

        # Create an equivalent engine with autocommit
        # See https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#session-transaction-isolation
        self.autocommit_engine = self.engine.execution_options(isolation_level="AUTOCOMMIT")

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

        # Check to see if the db is up-to-date
        self._check_db_revision(self.engine)

        self.Session = sessionmaker(bind=self.engine, future=True)

        # Create/initialize the subsockets
        from ..components.internal_jobs.socket import InternalJobSocket
        from ..components.molecules.socket import MoleculeSocket
        from ..components.auth.user_socket import UserSocket
        from ..components.auth.group_socket import GroupSocket
        from ..components.auth.role_socket import RoleSocket
        from ..components.auth.auth_socket import AuthSocket
        from ..components.serverinfo.socket import ServerInfoSocket
        from ..components.managers.socket import ManagerSocket
        from ..components.tasks.socket import TaskSocket
        from ..components.services.socket import ServiceSocket
        from ..components.external_files import ExternalFileSocket
        from ..components.record_socket import RecordSocket
        from ..components.dataset_socket import DatasetSocket

        # Internal job socket goes first - others may depend on this
        self.internal_jobs = InternalJobSocket(self)

        # Then the rest
        self.serverinfo = ServerInfoSocket(self)
        self.molecules = MoleculeSocket(self)
        self.datasets = DatasetSocket(self)
        self.records = RecordSocket(self)
        self.external_files = ExternalFileSocket(self)
        self.tasks = TaskSocket(self)
        self.services = ServiceSocket(self)
        self.managers = ManagerSocket(self)
        self.users = UserSocket(self)
        self.groups = GroupSocket(self)
        self.roles = RoleSocket(self)
        self.auth = AuthSocket(self)

    def __str__(self) -> str:
        return f"<SQLAlchemySocket: address='{self.qcf_config.database.safe_uri}`>"

    @staticmethod
    def alembic_commands(db_config: DatabaseConfig) -> List[str]:
        """
        Get the components of an alembic command that can be used on the command line

        This will find the almembic command and also add the uri and alembic configuration information
        to the command line.

        Returns
        -------
        List[str]
            Components of an alembic command line as a list of strings
        """

        # Find the path to the almebic ini
        alembic_ini = os.path.join(qcfractal.qcfractal_topdir, "alembic.ini")
        alembic_path = shutil.which("alembic")

        if alembic_path is None:
            raise RuntimeError("Cannot find the 'alembic' command. Is it installed?")
        return [
            alembic_path,
            "-c",
            alembic_ini,
            "-x",
            "uri=" + db_config.database_uri,
        ]

    @staticmethod
    def get_alembic_config(db_config: DatabaseConfig):
        """
        Obtain an alembic Config object given a QCFractal database configuration
        """

        from alembic.config import Config

        alembic_ini = os.path.join(qcfractal.qcfractal_topdir, "alembic.ini")
        alembic_cfg = Config(alembic_ini)

        # Tell alembic to not set up logging. We already did that
        alembic_cfg.set_main_option("skip_logging", "True")
        alembic_cfg.set_main_option("sqlalchemy.url", db_config.database_uri)

        return alembic_cfg

    @staticmethod
    def create_database_tables(db_config: DatabaseConfig):
        """
        Create all the tables in a database

        The database is expected to exist already
        """

        logger = logging.getLogger("SQLAlchemySocket")

        # Register all classes that derive from the BaseORM
        importlib.import_module("qcfractal.components.register_all")

        # create the tables via sqlalchemy
        logger.info(f"Creating tables for database: {db_config.safe_uri}")
        engine = create_engine(db_config.sqlalchemy_url, echo=False, poolclass=NullPool, future=True)

        from qcfractal.db_socket.base_orm import BaseORM

        try:
            BaseORM.metadata.create_all(engine)
        except Exception as e:
            raise RuntimeError(f"SQLAlchemy Connection Error\n{str(e)}")

        SQLAlchemySocket.populate_defaults(engine)

        # update alembic_version table with the current version
        logger.debug(f"Stamping Database with current alembic revision")
        from alembic import command

        alembic_cfg = SQLAlchemySocket.get_alembic_config(db_config)
        command.stamp(alembic_cfg, "head")

    @staticmethod
    def populate_defaults(engine):
        """
        Populates any default values in the database
        """

        from qcfractal.components.auth.db_models import RoleORM
        from qcfractal.components.auth.role_socket import default_roles

        session = sessionmaker(bind=engine)()
        try:
            for rolename, permissions in default_roles.items():
                orm = RoleORM(rolename=rolename, permissions=permissions)
                session.add(orm)
            session.commit()
        except Exception as e:
            raise RuntimeError(f"Failed to populate default roles:\n {str(e)}")
        finally:
            session.close()

    @staticmethod
    def upgrade_database(db_config: DatabaseConfig, revision: str = "head") -> None:
        """
        Upgrade the database schema using the latest alembic revision.
        """

        from alembic import command

        alembic_cfg = SQLAlchemySocket.get_alembic_config(db_config)
        command.upgrade(alembic_cfg, revision)

    @staticmethod
    def _check_db_revision(engine: Engine):
        """
        Checks to make sure the database is up-to-date, given an engine

        Will raise an exception if it is not up-to-date
        """

        from alembic.migration import MigrationContext
        from alembic.script import ScriptDirectory

        script_dir = os.path.join(qcfractal.qcfractal_topdir, "alembic")
        script = ScriptDirectory(script_dir)
        heads = script.get_heads()

        conn = engine.connect()
        context = MigrationContext.configure(connection=conn)
        current_rev = context.get_current_revision()

        try:
            if len(heads) > 1:
                raise RuntimeError("Multiple alembic revision heads not supported")

            if heads[0] != current_rev:
                raise RuntimeError(
                    "Database needs migration. Please run `qcfractal-server upgrade-db` (after backing up!)"
                )
        finally:
            conn.close()

    @staticmethod
    def check_db_revision(db_config: DatabaseConfig):
        """
        Checks to make sure the database is up-to-date, given a configuration

        Will raise an exception if it is not up-to-date
        """

        engine = create_engine(db_config.sqlalchemy_url, echo=False, poolclass=NullPool, future=True)
        SQLAlchemySocket._check_db_revision(engine)

    @contextmanager
    def session_scope(self, read_only: bool = False):
        """Provide a session as a context manager

        You can use this as a context manager (ie, `with session_scope() as session`).
        After the `with` block exits, the session is committed (if not `read_only`)
        and then closed. On exception, the session is rolled back.
        """

        if read_only:
            session = self.Session(bind=self.autocommit_engine)
        else:
            session = self.Session(bind=self.engine)
        try:
            yield session

            if not read_only:
                session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def optional_session(
        self, existing_session: Optional[Session], read_only: bool = False
    ) -> Generator[Session, Any, Any]:
        """
        Use the existing session if available, otherwise use a new session

        If an existing session is used, it is automatically flushed at the end, but not committed

        This is meant to be used with `with`, and where existing_session may be None.

         .. code-block:: python

            def somefunction(session: Optional[Session] = None):
                # will use session if not None, or will create a new session
                with storage_socket.optional_session(session) as s:
                    s.add(stuff)

        Parameters
        ----------
        existing_session
            An optional, existing sqlalchemy session
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

    def set_finished_watch(self, mp_queue):
        """
        Set the finished watch queue to the given multiprocessing queue

        When a calculation finishes, its record ID and status will be placed in this queue
        """

        self._finished_queue = mp_queue

    def notify_finished_watch(self, record_id, status):
        """
        Place information into the queue used to notify of finished calculations
        """

        if self._finished_queue is not None:
            # Don't want to block here. Just put it in the queue and move on
            self._finished_queue.put((int(record_id), status), block=False)

    @lru_cache()
    def find_referencing_cols(self, column):
        """
        Finds columns that have a foreign key referencing the given column

        For example, `find_referencing_cols(LargeBinaryORM.id)` will find all columns (in all tables in the database)
        that reference `LargeBinaryORM.id`
        """

        md = MetaData()
        md.reflect(bind=self.engine)

        insp_col = inspect(column)
        table = insp_col.table

        this_table_name = table.name
        this_column_name = insp_col.name

        db_reflection = inspect(self.engine)

        referencing_cols = []

        all_fkc = db_reflection.get_multi_foreign_keys()

        # Looking at all foreign keys, we want all that refer to this table and column
        # (constrained_columns is the columns on the other table)
        for (schema, table_name), fks in all_fkc.items():
            for fk in fks:
                if fk["referred_table"] == this_table_name and this_column_name in fk["referred_columns"]:
                    # Find what column refers to this one. It may be part of a composite fk
                    col_idx = fk["referred_columns"].index(this_column_name)
                    tb = md.tables[table_name]

                    # Turns it into something like BaseRecord.id (or the equivalent of a Table)
                    referencing_cols.append(getattr(tb.columns, fk["constrained_columns"][col_idx]))

        return referencing_cols

    def find_orphaned_rows(self, session: Session, column):
        """
        Finds any rows in a table that are not referenced elsewhere

        Only the given column is searched to find references
        """

        cte_parts = []
        referencing_columns = self.find_referencing_cols(column)
        for rc in referencing_columns:
            cp = select(rc.label("ref_col"))
            cte_parts.append(cp)

        cte = union(*cte_parts).cte()
        stmt = select(column)
        stmt = stmt.join(cte, cte.c.ref_col == column, isouter=True)
        stmt = stmt.where(cte.c.ref_col.is_(None))

        r = session.execute(stmt).scalars().all()
        return r
