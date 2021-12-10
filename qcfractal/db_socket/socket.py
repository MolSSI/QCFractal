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
from qcfractal.interface.models import prepare_basis

if TYPE_CHECKING:
    from typing import Tuple, Any, List, Optional, Union, Iterable
    from sqlalchemy.orm.session import Session
    from ..config import FractalConfig, DatabaseConfig


def format_query(ORMClass, **query: Union[None, str, int, Iterable[int], Iterable[str]]) -> List[Any]:
    """
    Formats a query into a SQLAlchemy format.
    """

    _null_keys = {"basis", "keywords"}
    _lower_func = lambda x: x.lower()
    _prepare_keys = {"program": _lower_func, "basis": prepare_basis, "method": _lower_func, "procedure": _lower_func}

    ret = []
    for k, v in query.items():
        if v is None:
            continue

        # Handle None keys
        k = k.lower()
        if (k in _null_keys) and (v == "null"):
            v = None

        # TODO: Remove these fixups at some point. This should be handled at a different level
        if k in _prepare_keys:
            f = _prepare_keys[k]
            if isinstance(v, (list, tuple)):
                v = [f(x) for x in v]
            else:
                v = f(v)

        if isinstance(v, (list, tuple)):
            col = getattr(ORMClass, k)
            ret.append(getattr(col, "in_")(v))
        else:
            ret.append(getattr(ORMClass, k) == v)

    return ret


def get_count_fast(query):
    """
    returns total count of the query using:
        Fast: SELECT COUNT(*) FROM TestModel WHERE ...

    Not like q.count():
        Slow: SELECT COUNT(*) FROM (SELECT ... FROM TestModel WHERE ...) ...
    """
    # TODO - sqlalchemy 1.4 broke the "fast" way. Reverting to the slow way
    # count_q = query.statement.with_only_columns([func.count()]).order_by(None)
    # count = query.session.execute(count_q).scalar()
    return query.count()


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
        engine = create_engine(uri, echo=False, pool_size=1)
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
        engine = create_engine(uri, echo=False, pool_size=1)
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

    def get_query_projection(self, className, query, *, limit=None, skip=0, include=None, exclude=None):

        table_name = className.__tablename__

        if include and exclude:
            raise AttributeError(
                f"Either include or exclude can be "
                f"used, not both at the same query. "
                f"Given include: {include}, exclude: {exclude}"
            )

        prop, hybrids, relationships = className._get_col_types()

        # build projection from include or exclude
        _projection = []
        if include:
            _projection = set(include)
        elif exclude:
            _projection = set(className._all_col_names()) - set(exclude) - set(className.db_related_fields)
        _projection = list(_projection)

        proj = []
        join_attrs = {}
        callbacks = []

        # prepare hybrid attributes for callback and joins
        for key in _projection:
            if key in prop:  # normal column
                proj.append(getattr(className, key))

            # if hybrid property, save callback, and relation if any
            elif key in hybrids:
                callbacks.append(key)

                # if it has a relationship
                if key + "_obj" in relationships.keys():

                    # join_class_name = relationships[key + '_obj']
                    join_attrs[key] = relationships[key + "_obj"]
            else:
                raise AttributeError(f"Atrribute {key} is not found in class {className}.")

        for key in join_attrs:
            _projection.remove(key)

        with self.session_scope() as session:
            if _projection or join_attrs:

                if join_attrs and "id" not in _projection:  # if the id is need for joins
                    proj.append(getattr(className, "id"))
                    _projection.append("_id")  # not to be returned to user

                # query with projection, without joins
                data = session.query(*proj).filter(*query)

                n_found = get_count_fast(data)  # before iterating on the data
                data = data.limit(limit).offset(skip)
                rdata = [dict(zip(_projection, row)) for row in data]

                # query for joins if any (relationships and hybrids)
                if join_attrs:
                    res_ids = [d.get("id", d.get("_id")) for d in rdata]
                    res_ids.sort()
                    join_data = {res_id: {} for res_id in res_ids}

                    # relations data
                    for key, relation_details in join_attrs.items():
                        ret = (
                            session.query(
                                relation_details["remote_side_column"].label("id"), relation_details["join_class"]
                            )
                            .filter(relation_details["remote_side_column"].in_(res_ids))
                            .order_by(relation_details["remote_side_column"])
                            .all()
                        )
                        for res_id in res_ids:
                            join_data[res_id][key] = []
                            for res in ret:
                                if res_id == res[0]:
                                    join_data[res_id][key].append(res[1])

                        for data in rdata:
                            parent_id = data.get("id", data.get("_id"))
                            data[key] = join_data[parent_id][key]
                            data.pop("_id", None)

                # call hybrid methods
                for callback in callbacks:
                    for res in rdata:
                        res[callback] = getattr(className, "_" + callback)(res[callback])

                id_fields = className._get_fieldnames_with_DB_ids_()
                for d in rdata:
                    # Expand extra json into fields
                    if "extra" in d:
                        d.update(d["extra"])
                        del d["extra"]

                    # transform ids from int into str
                    for key in id_fields:
                        if key in d.keys() and d[key] is not None:
                            if isinstance(d[key], Iterable):
                                d[key] = [str(i) for i in d[key]]
                            else:
                                d[key] = str(d[key])
                # print('--------rdata after: ', rdata)
            else:
                data = session.query(className).filter(*query)

                # from sqlalchemy.dialects import postgresql
                # print(data.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
                n_found = get_count_fast(data)
                data = data.limit(limit).offset(skip).all()
                rdata = [d.to_dict() for d in data]

        return rdata, n_found

    def set_completed_watch(self, mp_queue):
        self._completed_queue = mp_queue

    def notify_completed_watch(self, base_result_id, status):
        if self._completed_queue is not None:
            # Don't want to block here. Just put it in the queue and move on
            self._completed_queue.put((int(base_result_id), status), block=False)
