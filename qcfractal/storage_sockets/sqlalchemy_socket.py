"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""

from __future__ import annotations

try:
    from sqlalchemy.event import listen
    from sqlalchemy import create_engine, and_, or_, case, func, exc
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import sessionmaker, with_polymorphic
    from sqlalchemy.sql.expression import desc
    from sqlalchemy.sql.expression import case as expression_case
except ImportError:
    raise ImportError(
        "SQLAlchemy_socket requires sqlalchemy, please install this python " "module or try a different db_socket."
    )

import json
import logging
import secrets
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime as dt
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import bcrypt

# pydantic classes
from qcfractal.interface.models import (
    GridOptimizationRecord,
    KeywordSet,
    Molecule,
    ObjectId,
    OptimizationRecord,
    ResultRecord,
    TaskRecord,
    TaskStatusEnum,
    TorsionDriveRecord,
    KVStore,
    CompressionEnum,
    prepare_basis,
)
from qcfractal.interface.models.records import RecordStatusEnum

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
    RoleORM,
    ResultORM,
    ServiceQueueORM,
    TaskQueueORM,
    TorsionDriveProcedureORM,
    UserORM,
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
    SQLAlcehmy QCDB wrapper class.
    """

    def __init__(self):
        self.initialized = False

    def init(
        self,
        qcf_config: FractalConfig
    ):
        """
        Constructs a new SQLAlchemy socket
        """
        self.qcf_config = qcf_config

        # Logging data
        self.logger = logging.getLogger("SQLAlchemySocket")
        uri = qcf_config.database.uri

        #if "psycopg2" not in uri:
        #    uri = uri.replace("postgresql", "postgresql+psycopg2")

        self.logger.info(f"SQLAlchemy attempt to connect to {uri}.")

        # Connect to DB and create session
        self.uri = uri
        self.engine = create_engine(
            uri,
            echo=qcf_config.database.echo_sql,  # echo for logging into python logging
            pool_size=5,  # 5 is the default, 0 means unlimited
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
        from qcfractal.storage_sockets.subsockets.wavefunction import WavefunctionSocket
        from qcfractal.storage_sockets.subsockets.manager import ManagerSocket

        self.server_log = ServerLogSocket(self)
        self.output_store = OutputStoreSocket(self)
        self.keywords = KeywordsSocket(self)
        self.molecule = MoleculeSocket(self)
        self.collection = CollectionSocket(self)
        self.result = ResultSocket(self)
        self.wavefunction = WavefunctionSocket(self)
        self.manager = ManagerSocket(self)

        # Add User Roles if doesn't exist
        #self._add_default_roles()

        self.initialized = True

    def init_app(self, qcf_config: FractalConfig):
        if self.initialized:
            raise RuntimeError("Cannot initialize a database that is already initialized")

        self.init(qcf_config)

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

    def get_limit(self, table: str, limit: Optional[int] = None) -> int:
        """Get the allowed limit on results to return for a particular table.

        If 'limit' is given, will return min(limit, max_limit) where max_limit is
        the set value for the table.
        """

        max_limit = self.qcf_config.response_limits.get_limit(table)
        if limit is not None:
            return min(limit, max_limit)
        else:
            return max_limit

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

        limit = self.get_limit(table_name, limit)
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
        def on_baseresult_update(mapper, conn, target):
            if target.status != RecordStatusEnum.running and target.status != RecordStatusEnum.incomplete:
                mp_queue.put((target.id, target.result_type, target.status), block=False)

        listen(BaseResultORM, 'after_update', on_baseresult_update, propagate=True)

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

    # ~~~~~~~~~~~~~~~~~ Wavefunction ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

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


    def add_procedures(self, record_list: List["BaseRecord"]):
        """
        Add procedures from a given dict. The dict should have all the required
        keys of a result.

        Parameters
        ----------
        record_list : List["BaseRecord"]
            Each dict must have:
            procedure, program, keywords, qc_meta, hash_index
            In addition, it should have the other attributes that it needs
            to store

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta, data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        if not record_list:
            return {"data": [], "meta": meta}

        procedure_class = get_procedure_class(record_list[0])

        procedure_ids = []
        with self.session_scope() as session:
            for procedure in record_list:
                doc = session.query(procedure_class).filter_by(hash_index=procedure.hash_index)

                if get_count_fast(doc) == 0:
                    data = procedure.dict(exclude={"id"})
                    proc_db = procedure_class(**data)
                    session.add(proc_db)
                    session.commit()
                    proc_db.update_relations(**data)
                    session.commit()
                    procedure_ids.append(str(proc_db.id))
                    meta["n_inserted"] += 1
                else:
                    id = str(doc.first().id)
                    meta["duplicates"].append(id)  # TODO
                    procedure_ids.append(id)
        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

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
        """

        Parameters
        ----------
        id : str or List[str]
        procedure : str
        program : str
        hash_index : str
        task_id : str or List[str]
        status : bool, optional
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
            Default is 'COMPLETE'
        include : Optional[List[str]], optional
            The fields to return, default to return all
        exclude : Optional[List[str]], optional
            The fields to not return, default to return all
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, optional
            skip the first 'skip' resaults. Used to paginate
            Default is 0
        return_json : bool, optional
            Return the results as a list of json inseated of objects
            Default is True
        with_ids : bool, optional
            Include the ids in the returned objects/dicts
            Default is True

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data and meta. Data is the objects found
        """

        meta = get_metadata_template()

        if id is not None or task_id is not None:
            status = None

        if procedure == "optimization":
            className = OptimizationProcedureORM
        elif procedure == "torsiondrive":
            className = TorsionDriveProcedureORM
        elif procedure == "gridoptimization":
            className = GridOptimizationProcedureORM
        else:
            # raise TypeError('Unsupported procedure type {}. Id: {}, task_id: {}'
            #                 .format(procedure, id, task_id))
            className = BaseResultORM  # all classes, including those with 'selectin'
            program = None  # make sure it's not used
            if id is None:
                self.logger.error(f"Procedure type not specified({procedure}), and ID is not given.")
                raise KeyError("ID is required if procedure type is not specified.")

        query = format_query(
            className,
            id=id,
            procedure=procedure,
            program=program,
            hash_index=hash_index,
            task_id=task_id,
            manager_id=manager_id,
            status=status,
        )

        data = []
        try:
            # TODO: decide a way to find the right type

            data, meta["n_found"] = self.get_query_projection(
                className, query, limit=limit, skip=skip, include=include, exclude=exclude
            )
            meta["success"] = True
        except Exception as err:
            meta["error_description"] = str(err)

        return {"data": data, "meta": meta}

    def update_procedures(self, records_list: List["BaseRecord"]):
        """
        TODO: needs to be of specific type
        """

        updated_count = 0
        with self.session_scope() as session:
            for procedure in records_list:

                className = get_procedure_class(procedure)
                # join_table = get_procedure_join(procedure)
                # Must have ID
                if procedure.id is None:
                    self.logger.error(
                        "No procedure id found on update (hash_index={}), skipping.".format(procedure.hash_index)
                    )
                    continue

                proc_db = session.query(className).filter_by(id=procedure.id).first()

                data = procedure.dict(exclude={"id"})
                proc_db.update_relations(**data)

                for attr, val in data.items():
                    setattr(proc_db, attr, val)

                # session.add(proc_db)

                # Upsert relations (insert or update)
                # needs primarykeyconstraint on the table keys
                # for result_id in procedure.trajectory:
                #     statement = postgres_insert(opt_result_association)\
                #         .values(opt_id=procedure.id, result_id=result_id)\
                #         .on_conflict_do_update(
                #             index_elements=[opt_result_association.c.opt_id, opt_result_association.c.result_id],
                #             set_=dict(result_id=result_id))
                #     session.execute(statement)

                session.commit()
                updated_count += 1

        # session.commit()  # save changes, takes care of inheritance

        return updated_count

    def del_procedures(self, ids: List[str]):
        """
        Removes results from the database using their ids
        (Should be cautious! other tables maybe referencing results)

        Parameters
        ----------
        ids : List[str]
            The Ids of the results to be deleted

        Returns
        -------
        int
            number of results deleted
        """

        with self.session_scope() as session:
            procedures = (
                session.query(
                    with_polymorphic(
                        BaseResultORM,
                        [OptimizationProcedureORM, TorsionDriveProcedureORM, GridOptimizationProcedureORM],
                    )
                )
                .filter(BaseResultORM.id.in_(ids))
                .all()
            )
            # delete through session to delete correctly from base_result
            for proc in procedures:
                session.delete(proc)
            # session.commit()
            count = len(procedures)

        return count

    def add_services(self, service_list: List["BaseService"]):
        """
        Add services from a given list of dict.

        Parameters
        ----------
        services_list : List[Dict[str, Any]]
            List of services to be added
        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the hash_index of the inserted/existing docs
        """

        meta = add_metadata_template()

        procedure_ids = []
        with self.session_scope() as session:
            for service in service_list:

                # Add the underlying procedure
                new_procedure = self.add_procedures([service.output])

                # ProcedureORM already exists
                proc_id = new_procedure["data"][0]

                if new_procedure["meta"]["duplicates"]:
                    procedure_ids.append(proc_id)
                    meta["duplicates"].append(proc_id)
                    continue

                # search by hash index
                doc = session.query(ServiceQueueORM).filter_by(hash_index=service.hash_index)
                service.procedure_id = proc_id

                if doc.count() == 0:
                    doc = ServiceQueueORM(**service.dict(include=set(ServiceQueueORM.__dict__.keys())))
                    doc.extra = service.dict(exclude=set(ServiceQueueORM.__dict__.keys()))
                    doc.priority = doc.priority.value  # Must be an integer for sorting
                    session.add(doc)
                    session.commit()  # TODO
                    procedure_ids.append(proc_id)
                    meta["n_inserted"] += 1
                else:
                    procedure_ids.append(None)
                    meta["errors"].append((doc.id, "Duplicate service, but not caught by procedure."))

        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

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
        """

        Parameters
        ----------
        id / hash_index : List[str] or str, optional
            service id
        procedure_id : List[str] or str, optional
            procedure_id for the specific procedure
        status : str, optional
            status of the record queried for
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, optional
            skip the first 'skip' resaults. Used to paginate
            Default is 0
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the objects found
        """

        meta = get_metadata_template()
        query = format_query(ServiceQueueORM, id=id, hash_index=hash_index, procedure_id=procedure_id, status=status)

        with self.session_scope() as session:
            data = (
                session.query(ServiceQueueORM)
                .filter(*query)
                .order_by(ServiceQueueORM.priority.desc(), ServiceQueueORM.created_on)
                .limit(limit)
                .offset(skip)
                .all()
            )
            data = [x.to_dict() for x in data]

        meta["n_found"] = len(data)
        meta["success"] = True

        # except Exception as err:
        #     meta['error_description'] = str(err)

        return {"data": data, "meta": meta}

    def update_services(self, records_list: List["BaseService"]) -> int:
        """
        Replace existing service

        Raises exception if the id is invalid

        Parameters
        ----------
        records_list: List[Dict[str, Any]]
            List of Service items to be updated using their id

        Returns
        -------
        int
            number of updated services
        """

        updated_count = 0
        for service in records_list:
            if service.id is None:
                self.logger.error("No service id found on update (hash_index={}), skipping.".format(service.hash_index))
                continue

            with self.session_scope() as session:

                doc_db = session.query(ServiceQueueORM).filter_by(id=service.id).first()

                data = service.dict(include=set(ServiceQueueORM.__dict__.keys()))
                data["extra"] = service.dict(exclude=set(ServiceQueueORM.__dict__.keys()))

                data["id"] = int(data["id"])
                for attr, val in data.items():
                    setattr(doc_db, attr, val)

                session.add(doc_db)
                session.commit()

            procedure = service.output
            procedure.__dict__["id"] = service.procedure_id

            # Copy the stdout/error from the service itself to its procedure
            if service.stdout:
                stdout = KVStore(data=service.stdout)
                stdout_id = self.add_kvstore([stdout])["data"][0]
                procedure.__dict__["stdout"] = stdout_id
            if service.error:
                error = KVStore(data=service.error.dict())
                error_id = self.add_kvstore([error])["data"][0]
                procedure.__dict__["error"] = error_id

            self.update_procedures([procedure])

            updated_count += 1

        return updated_count

    def update_service_status(
        self, status: str, id: Union[List[str], str] = None, procedure_id: Union[List[str], str] = None
    ) -> int:
        """
        Update the status of the existing services in the database.

        Raises an exception if any of the ids are invalid.
        Parameters
        ----------
        status : str
            The input status string ready to replace the previous status
        id : Optional[Union[List[str], str]], optional
            ids of all the services requested to be updated, by default None
        procedure_id : Optional[Union[List[str], str]], optional
            procedure_ids for the specific procedures, by default None

        Returns
        -------
        int
            1 indicating that the status update was successful
        """

        if (id is None) and (procedure_id is None):
            raise KeyError("id or procedure_id must not be None.")

        status = status.lower()
        with self.session_scope() as session:

            query = format_query(ServiceQueueORM, id=id, procedure_id=procedure_id)

            # Update the service
            service = session.query(ServiceQueueORM).filter(*query).first()
            service.status = status

            # Update the procedure
            if status == "waiting":
                status = "incomplete"
            session.query(BaseResultORM).filter(BaseResultORM.id == service.procedure_id).update({"status": status})

            session.commit()

        return 1

    def services_completed(self, records_list: List["BaseService"]) -> int:
        """
        Delete the services which are completed from the database.

        Parameters
        ----------
        records_list : List["BaseService"]
            List of Service objects which are completed.

        Returns
        -------
        int
            Number of deleted active services from database.
        """
        done = 0
        for service in records_list:
            if service.id is None:
                self.logger.error(
                    "No service id found on completion (hash_index={}), skipping.".format(service.hash_index)
                )
                continue

            # in one transaction
            with self.session_scope() as session:

                procedure = service.output
                procedure.__dict__["id"] = service.procedure_id
                self.update_procedures([procedure])

                session.query(ServiceQueueORM).filter_by(id=service.id).delete()  # synchronize_session=False)

            done += 1

        return done

    ### Mongo queue handling functions

    def queue_submit(self, data: List[TaskRecord]):
        """Submit a list of tasks to the queue.
        Tasks are unique by their base_result, which should be inserted into
        the DB first before submitting it's corresponding task to the queue
        (with result.status='INCOMPLETE' as the default)
        The default task.status is 'WAITING'

        Parameters
        ----------
        data : List[TaskRecord]
            A task is a dict, with the following fields:
            - hash_index: idx, not used anymore
            - spec: dynamic field (dict-like), can have any structure
            - tag: str
            - base_results: tuple (required), first value is the class type
             of the result, {'results' or 'procedure'). The second value is
             the ID of the result in the DB. Example:
             "base_result": ('results', result_id)

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta.
            'data' is a list of the IDs of the tasks IN ORDER, including
            duplicates. An errored task has 'None' in its ID
            meta['duplicates'] has the duplicate tasks
        """

        meta = add_metadata_template()

        results = ["placeholder"] * len(data)

        with self.session_scope() as session:
            # preserving all the base results for later check
            all_base_results = [record.base_result for record in data]
            query_res = (
                session.query(TaskQueueORM.id, TaskQueueORM.base_result_id)
                .filter(TaskQueueORM.base_result_id.in_(all_base_results))
                .all()
            )

            # constructing a dict of found tasks and their ids
            found_dict = {str(base_result_id): str(task_id) for task_id, base_result_id in query_res}
            new_tasks, new_idx = [], []
            duplicate_idx = []
            for task_num, record in enumerate(data):

                if found_dict.get(record.base_result):
                    # if found, get id from found_dict
                    # Note: found_dict may return a task object because the duplicate id is of an object in the input.
                    results[task_num] = found_dict.get(record.base_result)
                    # add index of duplicates
                    duplicate_idx.append(task_num)
                    meta["duplicates"].append(task_num)

                else:
                    task_dict = record.dict(exclude={"id"})
                    task = TaskQueueORM(**task_dict)
                    new_idx.append(task_num)
                    task.priority = task.priority.value
                    # append all the new tasks that should be added
                    new_tasks.append(task)
                    # add the (yet to be) inserted object id to dictionary
                    found_dict[record.base_result] = task

            session.add_all(new_tasks)
            session.commit()

            meta["n_inserted"] += len(new_tasks)
            # setting the id for new inserted objects, cannot be done before commiting as new objects do not have ids
            for i, task_idx in enumerate(new_idx):
                results[task_idx] = str(new_tasks[i].id)

            # finding the duplicate items in input, for which ids are found only after insertion
            for i in duplicate_idx:
                if not isinstance(results[i], str):
                    results[i] = str(results[i].id)

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def queue_get_next(
        self, manager, available_programs, available_procedures, limit=100, tag=None
    ) -> List[TaskRecord]:
        """Obtain tasks for a manager

        Given tags and available programs/procedures on the manager, obtain
        waiting tasks to run.
        """

        proc_filt = TaskQueueORM.procedure.in_([p.lower() for p in available_procedures])
        none_filt = TaskQueueORM.procedure == None  # lgtm [py/test-equals-none]

        order_by = []
        if tag is not None:
            if isinstance(tag, str):
                tag = [tag]

        order_by.extend([TaskQueueORM.priority.desc(), TaskQueueORM.created_on])
        queries = []
        if tag is not None:
            for t in tag:
                query = format_query(TaskQueueORM, status=TaskStatusEnum.waiting, program=available_programs, tag=t)
                query.append(or_(proc_filt, none_filt))
                queries.append(query)
        else:
            query = format_query(TaskQueueORM, status=TaskStatusEnum.waiting, program=available_programs)
            query.append((or_(proc_filt, none_filt)))
            queries.append(query)

        new_limit = limit
        found = []
        update_count = 0

        update_fields = {"status": TaskStatusEnum.running, "modified_on": dt.utcnow(), "manager": manager}
        with self.session_scope() as session:
            for q in queries:

                # Have we found all we needed to find
                if new_limit == 0:
                    break

                # with_for_update locks the rows. skip_locked=True makes it skip already-locked rows
                # (possibly from another process)
                query = (
                    session.query(TaskQueueORM)
                    .filter(*q)
                    .order_by(*order_by)
                    .limit(new_limit)
                    .with_for_update(skip_locked=True)
                )

                new_items = query.all()
                new_ids = [x.id for x in new_items]

                # Update all the task records to reflect this manager claiming them
                update_count += (
                    session.query(TaskQueueORM)
                    .filter(TaskQueueORM.id.in_(new_ids))
                    .update(update_fields, synchronize_session=False)
                )

                # After commiting, the row locks are released
                session.commit()

                # How many more do we have to query
                new_limit = limit - len(new_items)

                # I would assume this is always true. If it isn't,
                # that would be really bad, and lead to an infinite loop
                assert new_limit >= 0

                # Store in dict form for returning. We will add the updated fields later
                found.extend([task.to_dict(exclude=update_fields.keys()) for task in new_items])

            # avoid another trip to the DB to get the updated values, set them here
            found = [TaskRecord(**task, **update_fields) for task in found]

        if update_count != len(found):
            self.logger.warning("QUEUE: Number of found tasks does not match the number of updated tasks.")

        return found

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
        """
        TODO: check what query keys are needs
        Parameters
        ----------
        id : Optional[List[str]], optional
            Ids of the tasks
        Hash_index: Optional[List[str]], optional,
            hash_index of service, not used
        program, list of str or str, optional
        status : Optional[bool], optional (find all)
            The status of the task: 'COMPLETE', 'RUNNING', 'WAITING', or 'ERROR'
        base_result: Optional[str], optional
            base_result id
        include : Optional[List[str]], optional
            The fields to return, default to return all
        exclude : Optional[List[str]], optional
            The fields to not return, default to return all
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, optional
            skip the first 'skip' results. Used to paginate, default is 0
        return_json : bool, optional
            Return the results as a list of json inseated of objects, deafult is True
        with_ids : bool, optional
            Include the ids in the returned objects/dicts, default is True

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the objects found
        """

        meta = get_metadata_template()
        query = format_query(
            TaskQueueORM,
            program=program,
            id=id,
            hash_index=hash_index,
            status=status,
            base_result_id=base_result,
            tag=tag,
            manager=manager,
        )

        data = []
        try:
            data, meta["n_found"] = self.get_query_projection(
                TaskQueueORM, query, limit=limit, skip=skip, include=include, exclude=exclude
            )
            meta["success"] = True
        except Exception as err:
            meta["error_description"] = str(err)

        data = [TaskRecord(**task) for task in data]

        return {"data": data, "meta": meta}

    def queue_get_by_id(self, id: List[str], limit: int = None, skip: int = 0, as_json: bool = True):
        """Get tasks by their IDs

        Parameters
        ----------
        id : List[str]
            List of the task Ids in the DB
        limit : Optional[int], optional
            max number of returned tasks. If limit > max_limit, max_limit
            will be returned instead (safe query)
        skip : int, optional
            skip the first 'skip' results. Used to paginate, default is 0
        as_json : bool, optioanl
            Return tasks as JSON, default is True

        Returns
        -------
        List[TaskRecord]
            List of the found tasks
        """

        limit = self.get_limit('task_queue', limit)
        with self.session_scope() as session:
            found = (
                session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(id)).limit(limit).offset(skip)
            )

            if as_json:
                found = [TaskRecord(**task.to_dict()) for task in found]

        return found

    def queue_mark_complete(self, task_ids: List[str]) -> int:
        """Update the given tasks as complete
        Note that each task is already pointing to its result location
        Mark the corresponding result/procedure as complete

        Parameters
        ----------
        task_ids : List[str]
            IDs of the tasks to mark as COMPLETE

        Returns
        -------
        int
            number of TaskRecord objects marked as COMPLETE, and deleted from the database consequtively.
        """

        if not task_ids:
            return 0

        with self.session_scope() as session:
            # delete completed tasks
            tasks_c = (
                session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).delete(synchronize_session=False)
            )

        return tasks_c

    def queue_mark_error(self, task_ids: List[str]) -> int:
        """
        update the given tasks as errored

        Parameters
        ----------
        task_ids : List[str]
            IDs of the tasks to mark as ERROR

        Returns
        -------
        int
            Number of tasks updated as errored.
        """

        if not task_ids:
            return 0

        updated_ids = []
        with self.session_scope() as session:
            task_objects = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).all()

            for task_obj in task_objects:
                task_obj.status = TaskStatusEnum.error
                task_obj.modified_on = dt.utcnow()

            session.commit()

        return len(task_ids)

    def queue_reset_status(
        self,
        id: Union[str, List[str]] = None,
        base_result: Union[str, List[str]] = None,
        manager: Optional[str] = None,
        reset_running: bool = False,
        reset_error: bool = False,
    ) -> int:
        """
        Reset the status of the tasks that a manager owns from Running to Waiting
        If reset_error is True, then also reset errored tasks AND its results/proc

        Parameters
        ----------
        id : Optional[Union[str, List[str]]], optional
            The id of the task to modify
        base_result : Optional[Union[str, List[str]]], optional
            The id of the base result to modify
        manager : Optional[str], optional
            The manager name to reset the status of
        reset_running : bool, optional
            If True, reset running tasks to be waiting
        reset_error : bool, optional
            If True, also reset errored tasks to be waiting,
            also update results/proc to be INCOMPLETE

        Returns
        -------
        int
            Updated count
        """

        if not (reset_running or reset_error):
            # nothing to do
            return 0

        if sum(x is not None for x in [id, base_result, manager]) == 0:
            raise ValueError("All query fields are None, reset_status must specify queries.")

        status = []
        if reset_running:
            status.append(TaskStatusEnum.running)
        if reset_error:
            status.append(TaskStatusEnum.error)

        query = format_query(TaskQueueORM, id=id, base_result_id=base_result, manager=manager, status=status)

        # Must have status + something, checking above as well(being paranoid)
        if len(query) < 2:
            raise ValueError("All query fields are None, reset_status must specify queries.")

        with self.session_scope() as session:
            # Update results and procedures if reset_error
            task_ids = session.query(TaskQueueORM.id).filter(*query)
            session.query(BaseResultORM).filter(TaskQueueORM.base_result_id == BaseResultORM.id).filter(
                TaskQueueORM.id.in_(task_ids)
            ).update(dict(status=RecordStatusEnum.incomplete, modified_on=dt.utcnow()), synchronize_session=False)

            updated = (
                session.query(TaskQueueORM)
                .filter(TaskQueueORM.id.in_(task_ids))
                .update(dict(status=TaskStatusEnum.waiting, modified_on=dt.utcnow()), synchronize_session=False)
            )

        return updated

    def reset_base_result_status(
        self,
        id: Union[str, List[str]] = None,
    ) -> int:
        """
        Reset the status of a base result to "incomplete". Will only work if the
        status is not complete.

        This should be rarely called. Handle with care!

        Parameters
        ----------
        id : Optional[Union[str, List[str]]], optional
            The id of the base result to modify

        Returns
        -------
        int
            Number of base results modified
        """

        query = format_query(BaseResultORM, id=id)
        update_dict = {"status": RecordStatusEnum.incomplete, "modified_on": dt.utcnow()}

        with self.session_scope() as session:
            updated = (
                session.query(BaseResultORM)
                .filter(*query)
                .filter(BaseResultORM.status != RecordStatusEnum.complete)
                .update(update_dict, synchronize_session=False)
            )

        return updated

    def queue_modify_tasks(
        self,
        id: Union[str, List[str]] = None,
        base_result: Union[str, List[str]] = None,
        new_tag: Optional[str] = None,
        new_priority: Optional[int] = None,
    ):
        """
        Modifies the tag and priority of tasks.

        This will only modify if the status is not running

        Parameters
        ----------
        id : Optional[Union[str, List[str]]], optional
            The id of the task to modify
        base_result : Optional[Union[str, List[str]]], optional
            The id of the base result to modify
        new_tag : Optional[str], optional
            New tag to assign to the given tasks
        new_priority: int, optional
            New priority to assign to the given tasks

        Returns
        -------
        int
            Updated count
        """

        if new_tag is None and new_priority is None:
            # nothing to do
            return 0

        if sum(x is not None for x in [id, base_result]) == 0:
            raise ValueError("All query fields are None, modify_task must specify queries.")

        query = format_query(TaskQueueORM, id=id, base_result_id=base_result)

        update_dict = {}
        if new_tag is not None:
            update_dict["tag"] = new_tag
        if new_priority is not None:
            update_dict["priority"] = new_priority

        update_dict["modified_on"] = dt.utcnow()

        with self.session_scope() as session:
            updated = (
                session.query(TaskQueueORM)
                .filter(*query)
                .filter(TaskQueueORM.status != TaskStatusEnum.running)
                .update(update_dict, synchronize_session=False)
            )

        return updated

    def del_tasks(self, id: Union[str, list]):
        """
        Delete a task from the queue. Use with cautious

        Parameters
        ----------
        id : str or List
            Ids of the tasks to delete
        Returns
        -------
        int
            Number of tasks deleted
        """

        task_ids = [id] if isinstance(id, (int, str)) else id
        with self.session_scope() as session:
            count = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).delete(synchronize_session=False)

        return count

    def _copy_task_to_queue(self, record_list: List[TaskRecord]):
        """
        copy the given tasks as-is to the DB. Used for data migration

        Parameters
        ----------
        record_list : List[TaskRecords]
            List of task records to be copied

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        task_ids = []
        with self.session_scope() as session:
            for task in record_list:
                doc = session.query(TaskQueueORM).filter_by(base_result_id=task.base_result_id)

                if get_count_fast(doc) == 0:
                    doc = TaskQueueORM(**task.dict(exclude={"id"}))
                    doc.priority = doc.priority.value
                    if isinstance(doc.error, dict):
                        doc.error = json.dumps(doc.error)

                    session.add(doc)
                    session.commit()  # TODO: faster if done in bulk
                    task_ids.append(str(doc.id))
                    meta["n_inserted"] += 1
                else:
                    id = str(doc.first().id)
                    meta["duplicates"].append(id)  # TODO
                    # If new or duplicate, add the id to the return list
                    task_ids.append(id)
        meta["success"] = True

        ret = {"data": task_ids, "meta": meta}
        return ret

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

    _valid_permissions = frozenset({"read", "write", "compute", "queue", "admin"})

    @staticmethod
    def _generate_password() -> str:
        """
        Generates a random password e.g. for add_user and modify_user.

        Returns
        -------
        str
            An unhashed random password.
        """
        return secrets.token_urlsafe(32)

    def add_user(self, username: str, password: Optional[str] = None, rolename: str = "read") -> bool:
        """
        Adds a new user. Passwords are stored using bcrypt.

        Parameters
        ----------
        username : str
            New user's username
        password : Optional[str], optional
            The user's password. If None, a new password will be generated.
        rolename: str (default 'user')
            Role name in the database, like user, admin, etc

        Returns
        -------
        bool :
            A Boolean of success flag
        """

        if password is None:
            password = self._generate_password()

        success = False
        with self.session_scope() as session:
            role = session.query(RoleORM).filter_by(rolename=rolename).first()

            if role is None:
                return False, f"Role {rolename} is not found."

            hashed = bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6))
            blob = {"username": username.lower(), "password": hashed, "role_id": role.id}

            try:
                user = UserORM(**blob)
                session.add(user)
                session.commit()
                success = True
            except IntegrityError as err:
                self.logger.warning(str(err))
                session.rollback()

        return success, password

    def verify_user(self, username: str, password: str) -> Tuple[bool, str, Any]:
        """
        Verifies if a user has the requested permissions or not. Passwords are
        stored and verified using bcrypt.

        Parameters
        ----------
        username : str
            The username to verify
        password : str
            The password associated with the username

        Returns
        --------
            Tuple:
                - success True/False
                - Message: sucess or error msg
                - permissions object (list of allowed/denied actions on resources)
        """

        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username.lower()).first()

            if data is None:
                return (False, "User not found.", {})

            try:
                pwcheck = bcrypt.checkpw(password.encode("UTF-8"), data.password)
            except Exception as e:
                self.logger.warning(f"Password check failure, error: {str(e)}")
                self.logger.warning(
                    f"Error likely caused by encryption salt mismatch, potentially fixed by creating a new password for user {username}."
                )
                return (False, "Password decryption failure, please contact your database administrator.")

            if pwcheck is False:
                return (False, "Incorrect password.", {})

            return (True, "Success", data.role_obj.permissions)

    def modify_user(
        self,
        username: str,
        password: Optional[str] = None,
        reset_password: bool = False,
        rolename: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        Alters a user's password, permissions, or both

        Passwords are stored using bcrypt.

        Parameters
        ----------
        username : str
            The username
        password : Optional[str], optional
            The user's new password. If None, the password will not be updated. Excludes reset_password.
        reset_password: bool, optional
            Reset the user's password to a new autogenerated one. The default is False.
        rolename : Optional[str], optional
            Role name in the Role table

        Returns
        -------
        Tuple[bool, str]
            A tuple of (success flag, message)
        """

        if reset_password and password is not None:
            return False, "only one of reset_password and password may be specified"

        username = username.lower()
        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username).first()

            if data is None:
                return False, f"User {username} not found."

            blob = {"username": username}

            if rolename is not None:
                role = session.query(RoleORM).filter_by(rolename=rolename).first()

                if role is None:
                    return False, f"Role {rolename} is not found."
                blob["role_id"] = role.id

            if reset_password:
                password = self._generate_password()
            if password is not None:
                blob["password"] = bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6))

            count = session.query(UserORM).filter_by(username=username).update(blob)
            success = count == 1

        if success:
            return True, None if password is None else f"New password is {password}"
        else:
            return False, f"Failed to modify user {username}"

    def remove_user(self, username: str) -> bool:
        """Removes a user

        Parameters
        ----------
        username : str
            The username to remove

        Returns
        -------
        bool
            If the operation was successful or not.
        """

        with self.session_scope() as session:
            count = session.query(UserORM).filter_by(username=username.lower()).delete(synchronize_session=False)

        return count == 1

    def get_user_permissions(self, username: str) -> Dict:
        """

        Parameters
        ----------
        username : str
            The username

        Returns
        -------
        permissions (dict)
            Dict of user permissions, or None if user is not found.
        """

        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username.lower()).first()
            try:
                ret = data.role_obj.permissions
            except AttributeError:
                ret = None

        return ret

    def _get_users(self):

        with self.session_scope() as session:
            data = session.query(UserORM).filter().all()
            data = [x.to_dict(exclude=["id"]) for x in data]

        return data

    # TODO: not checked after user-roles refactor
    def _copy_users(self, record_list: Dict):
        """
        copy the given users as-is to the DB. Used for data migration

        Parameters
        ----------
        record_list : Dict[str, Any]
            List of dict of managers data

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        user_names = []
        with self.session_scope() as session:
            for user in record_list:
                doc = session.query(UserORM).filter_by(username=user["username"])

                if get_count_fast(doc) == 0:
                    doc = UserORM(**user)
                    if isinstance(doc.password, str):  # TODO, for mongo
                        doc.password = doc.password.encode("ascii")
                    session.add(doc)
                    session.commit()
                    user_names.append(doc.username)
                    meta["n_inserted"] += 1
                else:
                    name = doc.first().username
                    meta["duplicates"].append(name)
                    user_names.append(name)
        meta["success"] = True

        ret = {"data": user_names, "meta": meta}
        return ret

    ### RoleORMs

    def get_roles(self):
        """
        get all roles
        """
        with self.session_scope() as session:
            data = session.query(RoleORM).filter().all()
            data = [x.to_dict(exclude=["id"]) for x in data]
        return data

    def get_role(self, rolename: str):
        """"""
        if rolename is None:
            return False, f"Role {rolename} not found."

        rolename = rolename.lower()
        with self.session_scope() as session:
            data = session.query(RoleORM).filter_by(rolename=rolename).first()

            if data is None:
                return False, f"Role {rolename} not found."
            role = data.to_dict(exclude=["id"])
        return True, role

    def add_role(self, rolename: str, permissions: Dict):
        """
        Adds a new role.

        Parameters
        ----------
        rolename : str
        permissions : Dict
            Examples:
                permissions = {
                "Statement": [
                    {"Effect": "Allow","Action": "*","Resource": "*"},
                    {"Effect": "Deny","Action": "GET","Resource": "user"},
                ]}


        Returns
        -------
        bool :
            A Boolean of success flag
        """

        rolename = rolename.lower()
        with self.session_scope() as session:
            blob = {"rolename": rolename, "permissions": permissions}

            try:
                role = RoleORM(**blob)
                session.add(role)
                return True, f"Role: {rolename} was added successfully."
            except IntegrityError as err:
                self.logger.warning(str(err))
                session.rollback()
                return False, str(err.orig.args)

    def _add_default_roles(self):
        """
        Add default roles to the DB IF they don't exists

        Default roles are Admin, read (readonly)

        """

        read_permissions = {
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "*"},
                {"Effect": "Deny", "Action": "*", "Resource": ["user", "manager", "role"]},
            ]
        }

        admin_permissions = {
            "Statement": [
                {"Effect": "Allow", "Action": "*", "Resource": "*"},
            ]
        }

        with self.session_scope() as session:
            user1 = {"rolename": "read", "permissions": read_permissions}
            user2 = {"rolename": "admin", "permissions": admin_permissions}

            try:
                session.add_all([RoleORM(**user1), RoleORM(**user2)])
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False

    def update_role(self, rolename: str, permissions: Dict):
        """
        Update role's permissions.

        Parameters
        ----------
        rolename : str
        permissions : Dict

        Returns
        -------
        bool :
            A Boolean of success flag
        """

        rolename = rolename.lower()
        with self.session_scope() as session:
            role = session.query(RoleORM).filter_by(rolename=rolename).first()

            if role is None:
                return False, f"Role {rolename} is not found."

            success = session.query(RoleORM).filter_by(rolename=rolename).update({"permissions": permissions})

        return success

    def delete_role(self, rolename: str):
        """
        Delete role.

        Parameters
        ----------
        rolename : str

        Returns
        -------
        bool :
            A Boolean of success flag
        """
        with self.session_scope() as session:
            success = session.query(RoleORM).filter_by(rolename=rolename.lower()).delete()

        return success

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

    def get_total_count(self, className, **kwargs):

        with self.session_scope() as session:
            query = session.query(className).filter(**kwargs)
            count = get_count_fast(query)

        return count

