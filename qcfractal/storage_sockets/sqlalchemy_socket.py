"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""

from __future__ import annotations

try:
    from sqlalchemy import create_engine, and_, or_, case, func, exc
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import sessionmaker, with_polymorphic
    from sqlalchemy.sql.expression import desc
    from sqlalchemy.sql.expression import case as expression_case
    from sqlalchemy.pool import NullPool
except ImportError:
    raise ImportError(
        "SQLAlchemy_socket requires sqlalchemy, please install this python " "module or try a different db_socket."
    )

import logging
from collections.abc import Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

# pydantic classes
from qcfractal.interface.models import (
    GridOptimizationRecord,
    KeywordSet,
    Molecule,
    ObjectId,
    OptimizationRecord,
    ResultRecord,
    TaskRecord,
    TorsionDriveRecord,
    KVStore,
    prepare_basis,
)

from qcfractal.storage_sockets.db_queries import QUERY_CLASSES
from qcfractal.storage_sockets.models import (
    BaseResultORM,
    CollectionORM,
    GridOptimizationProcedureORM,
    KVStoreORM,
    MoleculeORM,
    OptimizationProcedureORM,
    QueueManagerLogORM,
    QueueManagerORM,
    ResultORM,
    ServiceQueueORM,
    TaskQueueORM,
    TorsionDriveProcedureORM,
    VersionsORM,
    WavefunctionStoreORM,
)
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template

from .models import Base

if TYPE_CHECKING:
    from ..services.service_util import BaseService
    from ..config import FractalConfig

# for version checking
import qcelemental
import qcfractal
import qcengine

_null_keys = {"basis", "keywords"}
_id_keys = {"id", "molecule", "keywords", "procedure_id"}
_lower_func = lambda x: x.lower()
_prepare_keys = {"program": _lower_func, "basis": prepare_basis, "method": _lower_func, "procedure": _lower_func}


def dict_from_tuple(keys, values):
    return [dict(zip(keys, row)) for row in values]


def calculate_limit(max_limit: int, given_limit: Optional[int]):
    """Get the allowed limit on results to return for a particular or type of object

    If 'given_limit' is given (ie, by the user), this will return min(limit, max_limit)
    where max_limit is the set value for the table/type of object
    """

    if given_limit is None:
        return max_limit

    return min(given_limit, max_limit)


def format_query(ORMClass, **query: Dict[str, Union[str, List[str]]]) -> Dict[str, Union[str, List[str]]]:
    """
    Formats a query into a SQLAlchemy format.
    """

    ret = []
    for k, v in query.items():
        if v is None:
            continue

        # Handle None keys
        k = k.lower()
        if (k in _null_keys) and (v == "null"):
            v = None

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

    count_q = query.statement.with_only_columns([func.count()]).order_by(None)
    count = query.session.execute(count_q).scalar()

    return count


def get_procedure_class(record):

    if isinstance(record, OptimizationRecord):
        procedure_class = OptimizationProcedureORM
    elif isinstance(record, TorsionDriveRecord):
        procedure_class = TorsionDriveProcedureORM
    elif isinstance(record, GridOptimizationRecord):
        procedure_class = GridOptimizationProcedureORM
    else:
        raise TypeError("Procedure of type {} is not valid or supported yet.".format(type(record)))

    return procedure_class


class SQLAlchemySocket:
    """
    Main handler for managing/accessing an SQLAlchemy database
    """

    def __init__(
        self,
        qcf_config: FractalConfig
    ):
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
            self.engine = create_engine(
                uri,
                echo=qcf_config.database.echo_sql,
                poolclass=NullPool
            )
        else:
            self.engine = create_engine(
                uri,
                echo=qcf_config.database.echo_sql,
                pool_size=qcf_config.database.pool_size
            )

        self.logger.info(
            "Connected SQLAlchemy to DB dialect {} with driver {}".format(self.engine.dialect.name, self.engine.driver)
        )

        self.Session = sessionmaker(bind=self.engine)

        # check version compatibility
        db_ver = self.check_lib_versions()
        self.logger.info(f"DB versions: {db_ver}")
        if (not qcf_config.database.skip_version_check) and (db_ver and qcfractal.__version__ != db_ver["fractal_version"]):
            raise TypeError(
                f"You are running QCFractal version {qcfractal.__version__} "
                f'with an older DB version ({db_ver["fractal_version"]}). '
                f'Please run "qcfractal-server upgrade" first before starting the server.'
            )

        # Check for compatible versions of the QCFractal database schema
        try:
            self.check_lib_versions()  # update version if new DB
        except Exception as e:
            raise ValueError(f"SQLAlchemy Connection Error\n {str(e)}") from None

        # Advanced queries objects
        # TODO - replace max limit
        self._query_classes = {
            cls._class_name: cls(self.engine.url.database, max_limit=1000) for cls in QUERY_CLASSES
        }

        # Create/initialize the subsockets
        from qcfractal.storage_sockets.subsockets.server_logs import ServerLogSocket
        from qcfractal.storage_sockets.subsockets.output_store import OutputStoreSocket
        from qcfractal.storage_sockets.subsockets.keywords import KeywordsSocket
        from qcfractal.storage_sockets.subsockets.molecule import MoleculeSocket
        from qcfractal.storage_sockets.subsockets.collection import CollectionSocket
        from qcfractal.storage_sockets.subsockets.result import ResultSocket
        from qcfractal.storage_sockets.subsockets.procedure import ProcedureSocket
        from qcfractal.storage_sockets.subsockets.service import ServiceSocket
        from qcfractal.storage_sockets.subsockets.wavefunction import WavefunctionSocket
        from qcfractal.storage_sockets.subsockets.manager import ManagerSocket
        from qcfractal.storage_sockets.subsockets.task import TaskSocket
        from qcfractal.storage_sockets.subsockets.user import UserSocket
        from qcfractal.storage_sockets.subsockets.role import RoleSocket

        self.server_log = ServerLogSocket(self)
        self.output_store = OutputStoreSocket(self)
        self.keywords = KeywordsSocket(self)
        self.molecule = MoleculeSocket(self)
        self.collection = CollectionSocket(self)
        self.result = ResultSocket(self)
        self.procedure = ProcedureSocket(self)
        self.service = ServiceSocket(self)
        self.wavefunction = WavefunctionSocket(self)
        self.manager = ManagerSocket(self)
        self.task = TaskSocket(self)
        self.user = UserSocket(self)
        self.role = RoleSocket(self)

        # Add User Roles if doesn't exist
        self._add_default_roles()


    def __str__(self) -> str:
        return f"<SQLAlchemySocket: address='{self.uri}`>"

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope"""

        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def check_lib_versions(self):
        """Check the stored versions of elemental and fractal"""

        # check if versions table exist
        if not self.engine.dialect.has_table(self.engine, "versions"):
            return None

        with self.session_scope() as session:
            db_ver = session.query(VersionsORM).order_by(VersionsORM.created_on.desc())

            # Table exists but empty
            if db_ver.count() == 0:
                elemental_version = qcelemental.__version__
                fractal_version = qcfractal.__version__
                engine_version = qcengine.__version__
                current = VersionsORM(
                    elemental_version=elemental_version, fractal_version=fractal_version, engine_version=engine_version
                )
                session.add(current)
                session.commit()
            else:
                current = db_ver.first()

            ver = current.to_dict(exclude=["id"])

        return ver

    def _clear_db(self, db_name: str = None):
        """Dangerous, make sure you are deleting the right DB"""

        self.logger.warning("SQL: Clearing database '{}' and dropping all tables.".format(db_name))

        # drop all tables that it knows about
        Base.metadata.drop_all(self.engine)

        # create the tables again
        Base.metadata.create_all(self.engine)

        # self.client.drop_database(db_name)

    def _delete_DB_data(self, db_name):
        """TODO: needs more testing"""

        with self.session_scope() as session:
            # Metadata
            session.query(VersionsORM).delete(synchronize_session=False)
            # Task and services
            session.query(TaskQueueORM).delete(synchronize_session=False)
            session.query(QueueManagerLogORM).delete(synchronize_session=False)
            session.query(QueueManagerORM).delete(synchronize_session=False)
            session.query(ServiceQueueORM).delete(synchronize_session=False)

            # Collections
            session.query(CollectionORM).delete(synchronize_session=False)

            # Records
            session.query(TorsionDriveProcedureORM).delete(synchronize_session=False)
            session.query(GridOptimizationProcedureORM).delete(synchronize_session=False)
            session.query(OptimizationProcedureORM).delete(synchronize_session=False)
            session.query(ResultORM).delete(synchronize_session=False)
            session.query(WavefunctionStoreORM).delete(synchronize_session=False)
            session.query(BaseResultORM).delete(synchronize_session=False)

            # Auxiliary tables
            session.query(KVStoreORM).delete(synchronize_session=False)
            session.query(MoleculeORM).delete(synchronize_session=False)

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

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def custom_query(self, class_name: str, query_key: str, **kwargs):
        """
        Run advanced or specialized queries on different classes

        Parameters
        ----------
        class_name : str
            REST APIs name of the class (not the actual python name),
             e.g., torsiondrive
        query_key : str
            The feature or attribute to look for, like initial_molecule
        kwargs
            Extra arguments needed by the query, like the id of the torison drive

        Returns
        -------
            Dict[str,Any]:
                Query result dictionary with keys:
                data: returned data by the query (variable format)
                meta:
                    success: True or False
                    error_description: Error msg to show to the user
        """

        ret = {"data": [], "meta": get_metadata_template()}

        try:
            if class_name not in self._query_classes:
                raise AttributeError(f"Class name {class_name} is not found.")

            session = self.Session()
            ret["data"] = self._query_classes[class_name].query(session, query_key, **kwargs)
            ret["meta"]["success"] = True
            try:
                ret["meta"]["n_found"] = len(ret["data"])
            except TypeError:
                ret["meta"]["n_found"] = 1
            finally:
                session.close()
        except Exception as err:
            ret["meta"]["error_description"] = str(err)

        return ret


    def get_total_count(self, className, **kwargs):

        with self.session_scope() as session:
            query = session.query(className).filter(**kwargs)
            count = get_count_fast(query)

        return count


#    def is_queue_empty(self):
#        with self.session_scope() as session:
#            query1 = session.query(TaskQueueORM).filter(and_(TaskQueueORM.status == TaskStatusEnum.running,
#                                                             TaskQueueORM.status == TaskStatusEnum.waiting))
#
#            count1 = get_count_fast(query1)
#
#            # No need to
#            if count1 > 0:
#                return False
#
#            query2 = session.query(ServiceQueueORM).filter(or_(ServiceQueueORM.status == TaskStatusEnum.running,
#                                                               ServiceQueueORM.status == TaskStatusEnum.waiting))
#
#            count2 = get_count_fast(query2)
#
#        return count2 == 0

    def set_completed_watch(self, mp_queue):
        self._completed_queue = mp_queue

    def notify_completed_watch(self, base_result_id, status):
        if self._completed_queue is not None:
            # Don't want to block here. Just put it in the queue and move on
            self._completed_queue.put((int(base_result_id), status), block=False)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Logging ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def save_access(self, log_data):
        return self.server_log.save_access(log_data)

    def log_server_stats(self):
        return self.server_log.update()

    def get_server_stats_log(self, before=None, after=None, limit=None, skip=0):
        return self.server_log.get(before, after, limit, skip)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Logs (KV store) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_kvstore(self, outputs: List[KVStore]):
        return self.output_store.add(outputs)

    def get_kvstore(self, id: List[ObjectId] = None, limit: int = None, skip: int = 0):
        return self.output_store.get(id, limit, skip)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Molecule ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_add_molecules_mixed(self, data: List[Union[ObjectId, Molecule]]) -> List[Molecule]:
        return self.molecule.get_add_mixed(data)

    def add_molecules(self, molecules: List[Molecule]):
        return self.molecule.add(molecules)

    def get_molecules(self, id=None, molecule_hash=None, molecular_formula=None, limit: int = None, skip: int = 0):
        return self.molecule.get(id, molecule_hash, molecular_formula, limit, skip)

    def del_molecules(self, id: List[str] = None, molecule_hash: List[str] = None):
        return self.molecule.delete(id, molecule_hash)

    # ~~~~~~~~~~~~~~~~~~~~~~~ Keywords ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_keywords(self, keyword_sets: List[KeywordSet]):
        return self.keywords.add(keyword_sets)

    def get_keywords(
        self,
        id: Union[str, list] = None,
        hash_index: Union[str, list] = None,
        limit: int = None,
        skip: int = 0,
        return_json: bool = False,
        with_ids: bool = True,
    ) -> List[KeywordSet]:
        return self.keywords.get(id, hash_index, limit, skip, return_json, with_ids)

    def get_add_keywords_mixed(self, data):
        return self.keywords.get_add_mixed(data)

    def del_keywords(self, id: str) -> int:
        return self.keywords.delete(id)

    # ~~~~~~~~~~~~~~~~~ Collections ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

    def add_collection(self, data: Dict[str, Any], overwrite: bool = False):
        return self.collection.add(data, overwrite)

    def get_collections(
        self,
        collection: Optional[str] = None,
        name: Optional[str] = None,
        col_id: Optional[int] = None,
        limit: Optional[int] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        skip: int = 0,
    ) -> Dict[str, Any]:
        return self.collection.get(collection, name, col_id, limit, include, exclude, skip)

    def del_collection(
        self, collection: Optional[str] = None, name: Optional[str] = None, col_id: Optional[int] = None
    ) -> bool:
        return self.collection.delete(collection, name, col_id)


    # ~~~~~~~~~~~~~~~~~ Results ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

    def add_results(self, record_list: List[ResultRecord]):
        return self.result.add(record_list)

    def update_results(self, record_list: List[ResultRecord]):
        return self.result.update(record_list)

    def get_results(
        self,
        id: Union[str, List] = None,
        program: str = None,
        method: str = None,
        basis: str = None,
        molecule: str = None,
        driver: str = None,
        keywords: str = None,
        task_id: Union[str, List] = None,
        manager_id: Union[str, List] = None,
        status: str = "COMPLETE",
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        limit: int = None,
        skip: int = 0,
        return_json=True,
        with_ids=True,
    ):
        return self.result.get(id, program, method, basis, molecule, driver, keywords, task_id, manager_id, status, include, exclude, limit, skip, return_json, with_ids)

    def del_results(self, ids: List[str]):
        return self.result.delete(ids)

    # ~~~~~~~~~~~~~~~~~ Wavefunction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_wavefunction_store(self, blobs_list: List[Dict[str, Any]]):
        return self.wavefunction.add(blobs_list)

    def get_wavefunction_store(
        self,
        id: List[str] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        limit: int = None,
        skip: int = 0,
    ) -> Dict[str, Any]:
        return self.wavefunction.get(id, include, exclude, limit, skip)


    # ~~~~~~~~~~~~~~~~~~ Procedures ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def add_procedures(self, record_list: List["BaseRecord"]):
        return self.procedure.add(record_list)

    def get_procedures(
        self,
        id: Union[str, List] = None,
        procedure: str = None,
        program: str = None,
        hash_index: str = None,
        task_id: Union[str, List] = None,
        manager_id: Union[str, List] = None,
        status: str = "COMPLETE",
        include=None,
        exclude=None,
        limit: int = None,
        skip: int = 0,
        return_json=True,
        with_ids=True,
    ):
        return self.procedure.get(id, procedure, program, hash_index, task_id, manager_id, status, include, exclude, limit, skip, return_json, with_ids)

    def update_procedures(self, records_list: List["BaseRecord"]):
        return self.procedure.update(records_list)

    def del_procedures(self, ids: List[str]):
        return self.procedure.delete(ids)


    # ~~~~~~~~ Services ~~~~~~~~~~~~~

    def add_services(self, service_list: List["BaseService"]):
        return self.service.add(service_list)

    def get_services(
        self,
        id: Union[List[str], str] = None,
        procedure_id: Union[List[str], str] = None,
        hash_index: Union[List[str], str] = None,
        status: str = None,
        limit: int = None,
        skip: int = 0,
        return_json=True,
    ):
        return self.service.get(id, procedure_id, hash_index, status, limit, skip, return_json)

    def update_services(self, records_list: List["BaseService"]) -> int:
        return self.service.update(records_list)

    def update_service_status(
        self, status: str, id: Union[List[str], str] = None, procedure_id: Union[List[str], str] = None
    ) -> int:
        return self.service.update_status(status, id, procedure_id)

    def services_completed(self, records_list: List["BaseService"]) -> int:
        return self.service.completed(records_list)

    # ~~~~~~~~~~~~~~~~~ Task queue ~~~~~~~~~~~~~~~~~~

    def queue_submit(self, data: List[TaskRecord]):
        return self.task.add(data)

    def queue_get_next(
        self, manager, available_programs, available_procedures, limit=None, tag=None
    ) -> List[TaskRecord]:
        return self.task.get_next(manager, available_programs, available_procedures, limit, tag)

    def get_queue(
        self,
        id=None,
        hash_index=None,
        program=None,
        status: str = None,
        base_result: str = None,
        tag=None,
        manager=None,
        include=None,
        exclude=None,
        limit: int = None,
        skip: int = 0,
        return_json=False,
        with_ids=True,
    ):
        return self.task.get(id, hash_index, program, status, base_result, tag, manager, include, exclude, limit, skip, return_json, with_ids)

    def queue_get_by_id(self, id: List[str], limit: int = None, skip: int = 0, as_json: bool = True):
        return self.task.get_by_id(id, limit, skip, as_json)

    def queue_mark_complete(self, task_ids: List[str]) -> int:
        return self.task.mark_complete(task_ids)

    def queue_mark_error(self, task_ids: List[str]) -> int:
        return self.task.mark_error(task_ids)

    def queue_reset_status(
        self,
        id: Union[str, List[str]] = None,
        base_result: Union[str, List[str]] = None,
        manager: Optional[str] = None,
        reset_running: bool = False,
        reset_error: bool = False,
    ) -> int:
        return self.task.reset_status(id, base_result, manager, reset_running, reset_error)

    def reset_base_result_status(
        self,
        id: Union[str, List[str]] = None,
    ) -> int:
        return self.task.reset_base_result_status(id)

    def queue_modify_tasks(
        self,
        id: Union[str, List[str]] = None,
        base_result: Union[str, List[str]] = None,
        new_tag: Optional[str] = None,
        new_priority: Optional[int] = None,
    ):
        return self.task.modify(id, base_result, new_tag, new_priority)

    def del_tasks(self, id: Union[str, list]):
        return self.task.delete(id)


    ### QueueManagerORMs

    def manager_update(self, name, **kwargs):
        return self.manager.update(name, **kwargs)

    def get_managers(
        self, name: str = None, status: str = None, modified_before=None, modified_after=None, limit=None, skip=0
    ):
        return self.manager.get(name, status, modified_before, modified_after, limit, skip)

    def get_manager_logs(self, manager_ids: Union[List[str], str], timestamp_after=None, limit=None, skip=0):
        return self.manager.get_logs(manager_ids, timestamp_after, limit, skip)

    def _copy_managers(self, record_list: Dict):
        return self.manager._copy_managers(record_list)

    ### UserORMs

    def add_user(self, username: str, password: Optional[str] = None, rolename: str = "read") -> bool:
        return self.user.add(username, password, rolename)

    def verify_user(self, username: str, password: str) -> Tuple[bool, str, Any]:
        return self.user.verify(username, password)

    def modify_user(
        self,
        username: str,
        password: Optional[str] = None,
        reset_password: bool = False,
        rolename: Optional[str] = None,
    ) -> Tuple[bool, str]:
        return self.user.modify(username, password, reset_password, rolename)

    def remove_user(self, username: str) -> bool:
        return self.user.delete(username)

    def get_user_permissions(self, username: str) -> Dict:
        return self.user.get_permissions(username)

    ### RoleORMs

    def get_roles(self):
        return self.role.list()

    def get_role(self, rolename: str):
        return self.role.get(rolename)

    def add_role(self, rolename: str, permissions: Dict):
        return self.role.add(rolename, permissions)

    def _add_default_roles(self):
        return self.role.add_default()

    def update_role(self, rolename: str, permissions: Dict):
        return self.role.update(rolename, permissions)

    def delete_role(self, rolename: str):
        return self.role.delete(rolename)

