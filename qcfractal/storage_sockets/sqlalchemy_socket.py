"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""

try:
    from sqlalchemy import create_engine, or_, case, func
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import sessionmaker, with_polymorphic
    from sqlalchemy.sql.expression import desc
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
from typing import Any, Dict, List, Optional, Tuple, Union

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
    prepare_basis,
)
from qcfractal.storage_sockets.db_queries import QUERY_CLASSES
from qcfractal.storage_sockets.models import (
    AccessLogORM,
    BaseResultORM,
    CollectionORM,
    DatasetORM,
    GridOptimizationProcedureORM,
    KeywordsORM,
    KVStoreORM,
    MoleculeORM,
    OptimizationProcedureORM,
    QueueManagerLogORM,
    QueueManagerORM,
    ReactionDatasetORM,
    ResultORM,
    ServerStatsLogORM,
    ServiceQueueORM,
    TaskQueueORM,
    TorsionDriveProcedureORM,
    UserORM,
    VersionsORM,
    WavefunctionStoreORM,
)
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template

from .models import Base

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


def get_collection_class(collection_type):

    collection_map = {"dataset": DatasetORM, "reactiondataset": ReactionDatasetORM}

    collection_class = CollectionORM

    if collection_type in collection_map:
        collection_class = collection_map[collection_type]

    return collection_class


class SQLAlchemySocket:
    """
        SQLAlcehmy QCDB wrapper class.
    """

    def __init__(
        self,
        uri: str,
        project: str = "molssidb",
        bypass_security: bool = False,
        allow_read: bool = True,
        logger: "Logger" = None,
        sql_echo: bool = False,
        max_limit: int = 1000,
    ):
        """
        Constructs a new SQLAlchemy socket

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger("SQLAlcehmySocket")

        # Security
        self._bypass_security = bypass_security
        self._allow_read = allow_read

        self._lower_results_index = ["method", "basis", "program"]

        # disconnect from any active default connection
        # disconnect()
        if "psycopg2" not in uri:
            uri = uri.replace("postgresql", "postgresql+psycopg2")

        if project and not uri.endswith("/"):
            uri = uri + "/"

        uri = uri + project
        self.logger.info(f"SQLAlchemy attempt to connect to {uri}.")

        # Connect to DB and create session
        self.uri = uri
        self.engine = create_engine(
            uri,
            echo=sql_echo,  # echo for logging into python logging
            pool_size=5,  # 5 is the default, 0 means unlimited
        )
        self.logger.info(
            "Connected SQLAlchemy to DB dialect {} with driver {}".format(self.engine.dialect.name, self.engine.driver)
        )

        self.Session = sessionmaker(bind=self.engine)

        # actually create the tables
        try:
            Base.metadata.create_all(self.engine)
        except Exception as e:
            raise ValueError(f"SQLAlchemy Connection Error\n {str(e)}") from None

        # Advanced queries objects
        self._query_classes = {
            cls._class_name: cls(self.engine.url.database, max_limit=max_limit) for cls in QUERY_CLASSES
        }

        # if expanded_uri["password"] is not None:
        #     # connect to mongoengine
        #     self.client = db.connect(db=project, host=uri, authMechanism=authMechanism, authSource=authSource)
        # else:
        #     # connect to mongoengine
        #     self.client = db.connect(db=project, host=uri)

        # self._url, self._port = expanded_uri["nodelist"][0]

        # try:
        #     version_array = self.client.server_info()['versionArray']
        #
        #     if tuple(version_array) < (3, 2):
        #         raise RuntimeError
        # except AttributeError:
        #     raise RuntimeError(
        #         "Could not detect MongoDB version at URL {}. It may be a very old version or installed incorrectly. "
        #         "Choosing to stop instead of assuming version is at least 3.2.".format(uri))
        # except RuntimeError:
        #     # Trap low version
        #     raise RuntimeError("Connected MongoDB at URL {} needs to be at least version 3.2, found version {}.".
        #                        format(uri, self.client.server_info()['version']))

        self._project_name = project
        self._max_limit = max_limit

        self.check_lib_versions()

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

    def get_project_name(self) -> str:
        return self._project_name

    def get_limit(self, limit: Optional[int]) -> int:
        """Get the allowed limit on results to return in queries based on the
         given `limit`. If this number is greater than the
         SQLAlchemySocket.max_limit then the max_limit will be returned instead.
        """

        return limit if limit and limit < self._max_limit else self._max_limit

    def get_query_projection(self, className, query, *, limit=None, skip=0, include=None, exclude=None):

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
                data = data.limit(self.get_limit(limit)).offset(skip)
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
                data = data.limit(self.get_limit(limit)).offset(skip).all()
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
            Query results dict:
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
        except Exception as err:
            ret["meta"]["error_description"] = str(err)

        return ret

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Logging ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def save_access(self, log_data):

        with self.session_scope() as session:
            log = AccessLogORM(**log_data)
            session.add(log)
            session.commit()

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Logs (KV store) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_kvstore(self, blobs_list: List[Any]):
        """
        Adds to the key/value store table.

        Parameters
        ----------
        blobs_list : List[Any]
            A list of data blobs to add.

        Returns
        -------
        TYPE

            Description
        """

        meta = add_metadata_template()
        blob_ids = []
        with self.session_scope() as session:
            for blob in blobs_list:
                if blob is None:
                    blob_ids.append(None)
                    continue

                doc = KVStoreORM(value=blob)
                session.add(doc)
                session.commit()
                blob_ids.append(str(doc.id))
                meta["n_inserted"] += 1

        meta["success"] = True

        return {"data": blob_ids, "meta": meta}

    def get_kvstore(self, id: List[str] = None, limit: int = None, skip: int = 0):
        """
        Pulls from the key/value store table.

        Parameters
        ----------
        id : List[str]
            A list of ids to query
        limit : int, optional
            Maximum number of results to return.
        skip : int, optional
            skip the `skip` results
        Returns
        -------
        TYPE
            Description
        """

        meta = get_metadata_template()

        query = format_query(KVStoreORM, id=id)

        rdata, meta["n_found"] = self.get_query_projection(KVStoreORM, query, limit=limit, skip=skip)

        meta["success"] = True

        # meta['error_description'] = str(err)

        data = {d["id"]: d["value"] for d in rdata}

        return {"data": data, "meta": meta}

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Molecule ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def get_add_molecules_mixed(self, data: List[Union[ObjectId, Molecule]]) -> List[Molecule]:
        """
        Get or add the given molecules (if they don't exit).
        MoleculeORMs are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ordered_mol_dict = {indx: mol for indx, mol in enumerate(data)}
        new_molecules = {}
        id_mols = {}
        for idx, mol in ordered_mol_dict.items():
            if isinstance(mol, (int, str)):
                id_mols[idx] = mol
            elif isinstance(mol, Molecule):
                new_molecules[idx] = mol
            else:
                meta["errors"].append((idx, "Data type not understood"))

        ret_mols = {}

        # Add all new molecules
        flat_mols = []
        flat_mol_keys = []
        for k, v in new_molecules.items():
            flat_mol_keys.append(k)
            flat_mols.append(v)
        flat_mols = self.add_molecules(flat_mols)["data"]

        id_mols.update({k: v for k, v in zip(flat_mol_keys, flat_mols)})

        # Get molecules by index and translate back to dict
        tmp = self.get_molecules(list(id_mols.values()))
        id_mols_list = tmp["data"]
        meta["errors"].extend(tmp["meta"]["errors"])

        inv_id_mols = {v: k for k, v in id_mols.items()}

        for mol in id_mols_list:
            ret_mols[inv_id_mols[mol.id]] = mol

        meta["success"] = True
        meta["n_found"] = len(ret_mols)
        meta["missing"] = list(ordered_mol_dict.keys() - ret_mols.keys())

        # Rewind to flat last
        ret = []
        for ind in range(len(ordered_mol_dict)):
            if ind in ret_mols:
                ret.append(ret_mols[ind])
            else:
                ret.append(None)

        return {"meta": meta, "data": ret}

    def add_molecules(self, molecules: List[Molecule]):
        """
        Adds molecules to the database.

        Parameters
        ----------
        data : dict of molecule-like JSON objects
            A {key: molecule} dictionary of molecules to input.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        meta = add_metadata_template()

        results = []
        with self.session_scope() as session:

            # Build out the ORMs
            orm_molecules = []
            for dmol in molecules:

                if dmol.validated is False:
                    dmol = Molecule(**dmol.dict(), validate=True)

                mol_dict = dmol.dict(exclude={"id", "validated"})

                # TODO: can set them as defaults in the sql_models, not here
                mol_dict["fix_com"] = True
                mol_dict["fix_orientation"] = True

                # Build fresh indices
                mol_dict["molecule_hash"] = dmol.get_hash()
                mol_dict["molecular_formula"] = dmol.get_molecular_formula()

                mol_dict["identifiers"] = {}
                mol_dict["identifiers"]["molecule_hash"] = mol_dict["molecule_hash"]
                mol_dict["identifiers"]["molecular_formula"] = mol_dict["molecular_formula"]

                # search by index keywords not by all keys, much faster
                orm_molecules.append(MoleculeORM(**mol_dict))

            # Check if we have duplicates
            hash_list = [x.molecule_hash for x in orm_molecules]
            query = format_query(MoleculeORM, molecule_hash=hash_list)
            indices = session.query(MoleculeORM.molecule_hash, MoleculeORM.id).filter(*query)
            previous_id_map = {k: v for k, v in indices}

            # For a bulk add there must be no pre-existing and there must be no duplicates in the add list
            bulk_ok = len(hash_list) == len(set(hash_list))
            bulk_ok &= len(previous_id_map) == 0
            # bulk_ok = False

            if bulk_ok:
                # Bulk save, doesn't update fields for speed
                session.bulk_save_objects(orm_molecules)
                session.commit()

                # Query ID's and reorder based off orm_molecule ordered list
                query = format_query(MoleculeORM, molecule_hash=hash_list)
                indices = session.query(MoleculeORM.molecule_hash, MoleculeORM.id).filter(*query)

                id_map = {k: v for k, v in indices}
                n_inserted = len(orm_molecules)

            else:
                # Start from old ID map
                id_map = previous_id_map

                new_molecules = []
                n_inserted = 0

                for orm_mol in orm_molecules:
                    duplicate_id = id_map.get(orm_mol.molecule_hash, False)
                    if duplicate_id is not False:
                        meta["duplicates"].append(str(duplicate_id))
                    else:
                        new_molecules.append(orm_mol)
                        id_map[orm_mol.molecule_hash] = "placeholder_id"
                        n_inserted += 1
                        session.add(orm_mol)

                    # We should make sure there was not a hash collision?
                    # new_mol.compare(old_mol)
                    # raise KeyError("!!! WARNING !!!: Hash collision detected")

                session.commit()

                for new_mol in new_molecules:
                    id_map[new_mol.molecule_hash] = new_mol.id

            results = [str(id_map[x.molecule_hash]) for x in orm_molecules]
            assert "placeholder_id" not in results
            meta["n_inserted"] = n_inserted

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def get_molecules(self, id=None, molecule_hash=None, molecular_formula=None, limit: int = None, skip: int = 0):

        meta = get_metadata_template()

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash, molecular_formula=molecular_formula)

        # Don't include the hash or the molecular_formula in the returned result
        rdata, meta["n_found"] = self.get_query_projection(
            MoleculeORM, query, limit=limit, skip=skip, exclude=["molecule_hash", "molecular_formula"]
        )

        meta["success"] = True

        # ret["meta"]["errors"].extend(errors)

        data = [Molecule(**d, validate=False, validated=True) for d in rdata]

        return {"meta": meta, "data": data}

    def del_molecules(self, id: List[str] = None, molecule_hash: List[str] = None):
        """
        Removes a molecule from the database from its hash.

        Parameters
        ----------
        values : str or list of strs
            The hash of a molecule.

        Returns
        -------
        bool
            Number of deleted molecules.
        """

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash)

        with self.session_scope() as session:
            ret = session.query(MoleculeORM).filter(*query).delete(synchronize_session=False)

        return ret

    # ~~~~~~~~~~~~~~~~~~~~~~~ Keywords ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def add_keywords(self, keyword_sets: List[KeywordSet]):
        """Add one KeywordSet uniquly identified by 'program' and the 'name'.

        Parameters
        ----------
         data
            A list of KeywordSets to be inserted.

        Returns
        -------
            A dict with keys: 'data' and 'meta'
            (see add_metadata_template())
            The 'data' part is a list of ids of the inserted options
            data['duplicates'] has the duplicate entries

        Notes
        ------
            Duplicates are not considered errors.

        """

        meta = add_metadata_template()

        keywords = []
        with self.session_scope() as session:
            for kw in keyword_sets:

                kw_dict = kw.dict(exclude={"id"})

                # search by index keywords not by all keys, much faster
                found = session.query(KeywordsORM).filter_by(hash_index=kw_dict["hash_index"]).first()
                if not found:
                    doc = KeywordsORM(**kw_dict)
                    session.add(doc)
                    session.commit()
                    keywords.append(str(doc.id))
                    meta["n_inserted"] += 1
                else:
                    meta["duplicates"].append(str(found.id))  # TODO
                    keywords.append(str(found.id))
                meta["success"] = True

        ret = {"data": keywords, "meta": meta}

        return ret

    def get_keywords(
        self,
        id: Union[str, list] = None,
        hash_index: Union[str, list] = None,
        limit: int = None,
        skip: int = 0,
        return_json: bool = False,
        with_ids: bool = True,
    ) -> List[KeywordSet]:
        """Search for one (unique) option based on the 'program'
        and the 'name'. No overwrite allowed.

        Parameters
        ----------
        id : list or str
            Ids of the keywords
        hash_index : list or str
            hash index of keywords
        limit : int, optional
            Maximum number of results to return.
            If this number is greater than the SQLAlchemySocket.max_limit then
            the max_limit will be returned instead.
            Default is to return the socket's max_limit (when limit=None or 0)
        skip : int, optional
        return_json : bool, optional
            Return the results as a json object
            Default is True
        with_ids : bool, optional
            Include the DB ids in the returned object (names 'id')
            Default is True


        Returns
        -------
            A dict with keys: 'data' and 'meta'
            (see get_metadata_template())
            The 'data' part is an object of the result or None if not found
        """

        meta = get_metadata_template()
        query = format_query(KeywordsORM, id=id, hash_index=hash_index)

        rdata, meta["n_found"] = self.get_query_projection(
            KeywordsORM, query, limit=limit, skip=skip, exclude=[None if with_ids else "id"]
        )

        meta["success"] = True

        # meta['error_description'] = str(err)

        if not return_json:
            data = [KeywordSet(**d) for d in rdata]
        else:
            data = rdata

        return {"data": data, "meta": meta}

    def get_add_keywords_mixed(self, data):
        """
        Get or add the given options (if they don't exit).
        KeywordsORM are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ids = []
        for idx, kw in enumerate(data):
            if isinstance(kw, (int, str)):
                ids.append(kw)

            elif isinstance(kw, KeywordSet):
                new_id = self.add_keywords([kw])["data"][0]
                ids.append(new_id)
            else:
                meta["errors"].append((idx, "Data type not understood"))
                ids.append(None)

        missing = []
        ret = []
        for idx, id in enumerate(ids):
            if id is None:
                ret.append(None)
                missing.append(idx)
                continue

            tmp = self.get_keywords(id=id)["data"]
            if tmp:
                ret.append(tmp[0])
            else:
                ret.append(None)

        meta["success"] = True
        meta["n_found"] = len(ret) - len(missing)
        meta["missing"] = missing

        return {"meta": meta, "data": ret}

    def del_keywords(self, id: str) -> int:
        """
        Removes a option set from the database based on its id.

        Parameters
        ----------
        id : str
            id of the keyword

        Returns
        -------
        int
           number of deleted documents
        """

        count = 0
        with self.session_scope() as session:
            count = session.query(KeywordsORM).filter_by(id=id).delete(synchronize_session=False)

        return count

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

    ### database functions

    def add_collection(self, data: Dict[str, Any], overwrite: bool = False):
        """Add (or update) a collection to the database.

        Parameters
        ----------
        data : dict, which should inlcude at least:
            collection : str (immutable)
            name : str (immutable)

        overwrite : bool
            Update existing collection

        Returns
        -------
        A dict with keys: 'data' and 'meta'
            (see add_metadata_template())
            The 'data' part is the id of the inserted document or none

        Notes
        -----
        ** Change: The data doesn't have to include the ID, the document
        is identified by the (collection, name) pairs.
        ** Change: New fields will be added to the collection, but existing won't
            be removed.
        """

        meta = add_metadata_template()
        col_id = None
        # try:

        # if ("id" in data) and (data["id"] == "local"):
        #     data.pop("id", None)
        if "id" in data:  # remove the ID in any case
            data.pop("id", None)
        lname = data.get("name").lower()
        collection = data.pop("collection").lower()

        # Get collection class if special type is implemented
        collection_class = get_collection_class(collection)

        update_fields = {}
        for field in collection_class._all_col_names():
            if field in data:
                update_fields[field] = data.pop(field)

        update_fields["extra"] = data  # todo: check for sql injection

        with self.session_scope() as session:

            try:
                if overwrite:
                    col = session.query(collection_class).filter_by(collection=collection, lname=lname).first()
                    for key, value in update_fields.items():
                        setattr(col, key, value)
                else:
                    col = collection_class(collection=collection, lname=lname, **update_fields)

                session.add(col)
                session.commit()
                col.update_relations(**update_fields)
                session.commit()

                col_id = str(col.id)
                meta["success"] = True
                meta["n_inserted"] = 1

            except Exception as err:
                session.rollback()
                meta["error_description"] = str(err)

        ret = {"data": col_id, "meta": meta}
        return ret

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
        """Get collection by collection and/or name

        Parameters
        ----------
        collection: Optional[str], optional
            Type of the collection, e.g. ReactionDataset
        name: Optional[str], optional
            Name of the collection, e.g. S22
        col_id: Optional[int], optional
            Database id of the collection
        limit: Optional[int], optional
            Maximum number of results to return
        include: Optional[List[str]], optional
            Columns to return
        exclude: Optional[List[str]], optional
            Return all but these columns
        skip: int, optional
            Skip the first `skip` results

        Returns
        -------
        A dict with keys: 'data' and 'meta'
            The data is a list of the collections found
        """

        meta = get_metadata_template()
        if name:
            name = name.lower()
        if collection:
            collection = collection.lower()

        collection_class = get_collection_class(collection)
        query = format_query(collection_class, lname=name, collection=collection, id=col_id)

        # try:
        rdata, meta["n_found"] = self.get_query_projection(
            collection_class, query, include=include, exclude=exclude, limit=limit, skip=skip
        )

        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

        return {"data": rdata, "meta": meta}

    def del_collection(self, collection: str, name: str) -> bool:
        """
        Remove a collection from the database from its keys.

        Parameters
        ----------
        collection: str
            CollectionORM type
        name : str
            CollectionORM name

        Returns
        -------
        int
            Number of documents deleted
        """

        with self.session_scope() as session:
            count = (
                session.query(CollectionORM)
                .filter_by(collection=collection.lower(), lname=name.lower())
                .delete(synchronize_session=False)
            )
        return count

    ## ResultORMs functions

    def add_results(self, record_list: List[ResultRecord]):
        """
        Add results from a given dict. The dict should have all the required
        keys of a result.

        Parameters
        ----------
        data : list of dict
            Each dict must have:
            program, driver, method, basis, options, molecule
            Where molecule is the molecule id in the DB
            In addition, it should have the other attributes that it needs
            to store

        Returns
        -------
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        result_ids = []
        with self.session_scope() as session:
            for result in record_list:

                doc = session.query(ResultORM).filter_by(
                    program=result.program,
                    driver=result.driver,
                    method=result.method,
                    basis=result.basis,
                    keywords=result.keywords,
                    molecule=result.molecule,
                )

                if get_count_fast(doc) == 0:
                    doc = ResultORM(**result.dict(exclude={"id"}))
                    session.add(doc)
                    session.commit()  # TODO: faster if done in bulk
                    result_ids.append(str(doc.id))
                    meta["n_inserted"] += 1
                else:
                    id = str(doc.first().id)
                    meta["duplicates"].append(id)  # TODO
                    # If new or duplicate, add the id to the return list
                    result_ids.append(id)
        meta["success"] = True

        ret = {"data": result_ids, "meta": meta}
        return ret

    def update_results(self, record_list: List[ResultRecord]):
        """
        Update results from a given dict (replace existing)

        Parameters
        ----------
        id : list of str
            Ids of the results to update, must exist in the DB
        data : list of dict
            Data that needs to be updated
            Shouldn't update:
            program, driver, method, basis, options, molecule

        Returns
        -------
            number of records updated
        """

        # try:
        updated_count = 0
        for result in record_list:

            if result.id is None:
                self.logger.error("Attempted update without ID, skipping")
                continue
            with self.session_scope() as session:

                result_db = session.query(ResultORM).filter_by(id=result.id).first()

                data = result.dict(exclude={"id"})

                for attr, val in data.items():
                    setattr(result_db, attr, val)

                session.commit()
                updated_count += 1

        return updated_count

    def get_results_count(self):
        """
        TODO: just return the count, used for big queries

        Returns
        -------

        """

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
        """

        Parameters
        ----------
        id : str or list
        program : str
        method : str
        basis : str
        molecule : str
            MoleculeORM id in the DB
        driver : str
        keywords : str
            The id of the option in the DB
        task_id: str or list
            id or a list of ids of tasks
        manager_id: str or list
            id or a list of ids of queue_mangers
        status : bool, default is 'COMPLETE'
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
        include : list/set/tuple, default is None
            The fields to return, default to return all
        exclude : list/set/tuple, default is None
            The fields to not return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, default is 0
            skip the first 'skip' results. Used to paginate
        return_json : bool, default is True
            Return the results as a list of json inseated of objects
        with_ids : bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        if task_id:
            return self._get_results_by_task_id(task_id)

        meta = get_metadata_template()

        # Ignore status if Id is present
        if id is not None:
            status = None

        query = format_query(
            ResultORM,
            id=id,
            program=program,
            method=method,
            basis=basis,
            molecule=molecule,
            driver=driver,
            keywords=keywords,
            manager_id=manager_id,
            status=status,
        )

        data, meta["n_found"] = self.get_query_projection(
            ResultORM, query, include=include, exclude=exclude, limit=limit, skip=skip
        )
        meta["success"] = True

        return {"data": data, "meta": meta}

    def _get_results_by_task_id(self, task_id: Union[str, List] = None, return_json=True):
        """

        Parameters
        ----------
        task_id : str or list

        return_json : bool, default is True
            Return the results as a list of json inseated of objects

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()

        data = []
        task_id_list = [task_id] if isinstance(task_id, (int, str)) else task_id
        # try:
        with self.session_scope() as session:
            data = (
                session.query(BaseResultORM)
                .filter(BaseResultORM.id == TaskQueueORM.base_result_id)
                .filter(TaskQueueORM.id.in_(task_id_list))
            )
            meta["n_found"] = get_count_fast(data)
            data = [d.to_dict() for d in data.all()]
            meta["success"] = True
            # except Exception as err:
            #     meta['error_description'] = str(err)

        return {"data": data, "meta": meta}

    def del_results(self, ids: List[str]):
        """
        Removes results from the database using their ids
        (Should be cautious! other tables maybe referencing results)

        Parameters
        ----------
        ids : list of str
            The Ids of the results to be deleted

        Returns
        -------
        int
            number of results deleted
        """

        with self.session_scope() as session:
            results = session.query(ResultORM).filter(ResultORM.id.in_(ids)).all()
            # delete through session to delete correctly from base_result
            for result in results:
                session.delete(result)
            session.commit()
            count = len(results)

        return count

    def add_wavefunction_store(self, blobs_list: List[Dict[str, Any]]):
        """
        Adds to the wavefunction key/value store table.

        Parameters
        ----------
        blobs_list : List[Dict[str, Any]]
            A list of wavefunction data blobs to add.

        Returns
        -------
        TYPE
            Description
        """

        meta = add_metadata_template()
        blob_ids = []
        with self.session_scope() as session:
            for blob in blobs_list:
                if blob is None:
                    blob_ids.append(None)
                    continue

                doc = WavefunctionStoreORM(**blob)
                session.add(doc)
                session.commit()
                blob_ids.append(str(doc.id))
                meta["n_inserted"] += 1

        meta["success"] = True

        return {"data": blob_ids, "meta": meta}

    def get_wavefunction_store(
        self,
        id: List[str] = None,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        limit: int = None,
        skip: int = 0,
    ):
        """
        Pulls from the wavefunction key/value store table.

        Parameters
        ----------
        id : List[str], optional
            A list of ids to query
        include : Dict[str, bool], optional
            Description
        limit : int, optional
            Maximum number of results to return.
        skip : int, optional
            skip the `skip` results

        Returns
        -------
        TYPE
            Description
        """

        meta = get_metadata_template()

        query = format_query(WavefunctionStoreORM, id=id)
        rdata, meta["n_found"] = self.get_query_projection(
            WavefunctionStoreORM, query, limit=limit, skip=skip, include=include, exclude=exclude
        )

        meta["success"] = True

        return {"data": rdata, "meta": meta}

    ### Mongo procedure/service functions

    def add_procedures(self, record_list: List["BaseRecord"]):
        """
        Add procedures from a given dict. The dict should have all the required
        keys of a result.

        Parameters
        ----------
        data : list of dict
            Each dict must have:
            procedure, program, keywords, qc_meta, hash_index
            In addition, it should have the other attributes that it needs
            to store
        update_existing : bool (default False)
            Update existing results

        Returns
        -------
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
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
        id : str or list
        procedure : str
        program : str
        hash_index : str
        task_id : str or list
        status : bool, default is 'COMPLETE'
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
        include : list/set/tuple of keys, default is None
            The fields to return, default to return all
        exclude : list/set/tuple of keys, default is None
            The fields to not return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, default is 0
            skip the first 'skip' resaults. Used to paginate
        return_json : bool, deafult is True
            Return the results as a list of json inseated of objects
        with_ids : bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
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
        ids : list of str
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
        Add services from a given dict.

        Parameters
        ----------
        data : list of dict

        Returns
        -------
            Dict with keys: data, meta
            Data is the hash_index of the inserted/existing docs
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
        id / hash_index : List of str or str
            service id / hash_index that ran the results
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, default is 0
            skip the first 'skip' resaults. Used to paginate
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
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
        id
        updates

        Returns
        -------
            if operation is succesful
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
            self.update_procedures([procedure])

            updated_count += 1

        return updated_count

    def update_service_status(
        self, status: str, id: Union[List[str], str] = None, procedure_id: Union[List[str], str] = None
    ) -> int:

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

        Duplicate tasks sould be a rare case.
        Hooks are merged if the task already exists

        Parameters
        ----------
        data : list of tasks (dict)
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
        dict (data and meta)
            'data' is a list of the IDs of the tasks IN ORDER, including
            duplicates. An errored task has 'None' in its ID
            meta['duplicates'] has the duplicate tasks
        """

        meta = add_metadata_template()

        results = []
        with self.session_scope() as session:
            for task_num, record in enumerate(data):
                try:
                    task_dict = record.dict(exclude={"id"})
                    # # for compatibility with mongoengine
                    # if isinstance(task_dict['base_result'], dict):
                    #     task_dict['base_result'] = task_dict['base_result']['id']
                    task = TaskQueueORM(**task_dict)
                    task.priority = task.priority.value  # Must be an integer for sorting
                    session.add(task)
                    session.commit()
                    results.append(str(task.id))
                    meta["n_inserted"] += 1
                except IntegrityError as err:  # rare case
                    # print(str(err))
                    session.rollback()
                    # TODO: merge hooks
                    task = session.query(TaskQueueORM).filter_by(base_result_id=record.base_result.id).first()
                    self.logger.warning("queue_submit got a duplicate task: {}".format(task.to_dict()))
                    results.append(str(task.id))
                    meta["duplicates"].append(task_num)
                # except Exception as err:
                #     self.logger.warning('queue_submit submission error: {}'.format(str(err)))
                #     meta["success"] = False
                #     meta["errors"].append(str(err))
                #     results.append(None)

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def queue_get_next(
        self, manager, available_programs, available_procedures, limit=100, tag=None, as_json=True
    ) -> List[TaskRecord]:
        """Done in a transaction"""

        # Figure out query, tagless has no requirements
        query = format_query(TaskQueueORM, status=TaskStatusEnum.waiting, program=available_programs, tag=tag)

        proc_filt = TaskQueueORM.procedure.in_([p.lower() for p in available_procedures])
        none_filt = TaskQueueORM.procedure == None  # lgtm [py/test-equals-none]
        query.append(or_(proc_filt, none_filt))

        with self.session_scope() as session:
            query = (
                session.query(TaskQueueORM)
                .filter(*query)
                .order_by(TaskQueueORM.priority.desc(), TaskQueueORM.created_on)
                .limit(limit)
            )

            # print(query.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
            found = query.all()

            ids = [x.id for x in found]
            update_fields = {"status": TaskStatusEnum.running, "modified_on": dt.utcnow(), "manager": manager}
            # Bulk update operation in SQL
            update_count = (
                session.query(TaskQueueORM)
                .filter(TaskQueueORM.id.in_(ids))
                .update(update_fields, synchronize_session=False)
            )

            if as_json:
                # avoid another trip to the DB to get the updated values, set them here
                found = [TaskRecord(**task.to_dict(exclude=update_fields.keys()), **update_fields) for task in found]
            session.commit()

        if update_count != len(found):
            self.logger.warning("QUEUE: Number of found projects does not match the number of updated projects.")

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
        id : list
            Ids of the tasks
        Hash_index
        status : bool, default is None (find all)
            The status of the task: 'COMPLETE', 'RUNNING', 'WAITING', or 'ERROR'
        base_result: str (optional)
            base_result id
        include : list/set/tuple of keys, default is None
            The fields to return, default to return all
        exclude : list/set/tuple of keys, default is None
            The fields to not return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, default is None 0
            skip the first 'skip' resaults. Used to paginate
        return_json : bool, deafult is True
            Return the results as a list of json inseated of objects
        with_ids : bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
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
        id : list of str
            List of the task Ids in the DB
        limit : int (optional)
            max number of returned tasks. If limit > max_limit, max_limit
            will be returned instead (safe query)
        as_json : bool
            Return tasks as JSON

        Returns
        -------
        list of the found tasks
        """

        with self.session_scope() as session:
            found = (
                session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(id)).limit(self.get_limit(limit)).offset(skip)
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
        task_ids : list
            IDs of the tasks to mark as COMPLETE

        Returns
        -------
        int
            Updated count
        """

        if not task_ids:
            return 0

        update_fields = dict(status=TaskStatusEnum.complete, modified_on=dt.utcnow())
        with self.session_scope() as session:
            # assuming all task_ids are valid, then managers will be in order by id
            managers = (
                session.query(TaskQueueORM.manager)
                .filter(TaskQueueORM.id.in_(task_ids))
                .order_by(TaskQueueORM.id)
                .all()
            )
            managers = [manager[0] if manager else manager for manager in managers]
            task_manger_map = {task_id: manager for task_id, manager in zip(sorted(task_ids), managers)}
            update_fields[BaseResultORM.manager_name] = case(task_manger_map, value=TaskQueueORM.id)

            session.query(BaseResultORM).filter(BaseResultORM.id == TaskQueueORM.base_result_id).filter(
                TaskQueueORM.id.in_(task_ids)
            ).update(update_fields, synchronize_session=False)

            # delete completed tasks
            tasks_c = (
                session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).delete(synchronize_session=False)
            )

        return tasks_c

    def queue_mark_error(self, data: List[Tuple[int, str]]):
        """update the given tasks as errored
        Mark the corresponding result/procedure as Errored

        """

        if not data:
            return 0

        task_ids = []
        with self.session_scope() as session:
            # Make sure retuened results are in the same order as the task ids
            # SQL queries change the order when using "in"
            data_dict = {item[0]: item[1] for item in data}
            sorted_data = {key: data_dict[key] for key in sorted(data_dict.keys())}
            task_objects = (
                session.query(TaskQueueORM)
                .filter(TaskQueueORM.id.in_(sorted_data.keys()))
                .order_by(TaskQueueORM.id)
                .all()
            )
            base_results = (
                session.query(BaseResultORM)
                .filter(BaseResultORM.id == TaskQueueORM.base_result_id)
                .filter(TaskQueueORM.id.in_(sorted_data.keys()))
                .order_by(TaskQueueORM.id)
                .all()
            )

            for (task_id, msg), task_obj, base_result in zip(sorted_data.items(), task_objects, base_results):

                task_ids.append(task_id)
                # update task
                task_obj.status = TaskStatusEnum.error
                task_obj.modified_on = dt.utcnow()

                # update result
                base_result.status = TaskStatusEnum.error
                base_result.manager_name = task_obj.manager
                base_result.modified_on = dt.utcnow()
                base_result.error_obj = KVStoreORM(value=msg)

                # session.add(task_obj)

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
        id : Union[str, List[str]], optional
            The id of the task to modify
        base_result : Union[str, List[str]], optional
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
            ).update(dict(status="INCOMPLETE", modified_on=dt.utcnow()), synchronize_session=False)

            updated = (
                session.query(TaskQueueORM)
                .filter(TaskQueueORM.id.in_(task_ids))
                .update(dict(status=TaskStatusEnum.waiting, modified_on=dt.utcnow()), synchronize_session=False)
            )

        return updated

    def del_tasks(self, id: Union[str, list]):
        """Delete a task from the queue. Use with cautious

        Parameters
        ----------
        id : str or list
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
        record_list : list of TaskRecords

        Returns
        -------
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        task_ids = []
        with self.session_scope() as session:
            for task in record_list:
                doc = session.query(TaskQueueORM).filter_by(base_result_id=task.base_result.id)

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

        do_log = kwargs.pop("log", False)

        inc_count = {
            # Increment relevant data
            "submitted": QueueManagerORM.submitted + kwargs.pop("submitted", 0),
            "completed": QueueManagerORM.completed + kwargs.pop("completed", 0),
            "returned": QueueManagerORM.returned + kwargs.pop("returned", 0),
            "failures": QueueManagerORM.failures + kwargs.pop("failures", 0),
        }

        upd = {key: kwargs[key] for key in QueueManagerORM.__dict__.keys() if key in kwargs}

        with self.session_scope() as session:
            # QueueManagerORM.objects()  # init
            manager = session.query(QueueManagerORM).filter_by(name=name)
            if manager.count() > 0:  # existing
                upd.update(inc_count, modified_on=dt.utcnow())
                num_updated = manager.update(upd)
            else:  # create new, ensures defaults and validations
                manager = QueueManagerORM(name=name, **upd)
                session.add(manager)
                session.commit()
                num_updated = 1

            if do_log:
                # Pull again in case it was updated
                manager = session.query(QueueManagerORM).filter_by(name=name).first()

                manager_log = QueueManagerLogORM(
                    manager_id=manager.id,
                    completed=manager.completed,
                    submitted=manager.submitted,
                    failures=manager.failures,
                    total_worker_walltime=manager.total_worker_walltime,
                    total_task_walltime=manager.total_task_walltime,
                    active_tasks=manager.active_tasks,
                    active_cores=manager.active_cores,
                    active_memory=manager.active_memory,
                )

                session.add(manager_log)
                session.commit()

        return num_updated == 1

    def get_managers(
        self, name: str = None, status: str = None, modified_before=None, modified_after=None, limit=None, skip=0
    ):

        meta = get_metadata_template()
        query = format_query(QueueManagerORM, name=name, status=status)

        if modified_before:
            query.append(QueueManagerORM.modified_on <= modified_before)

        if modified_after:
            query.append(QueueManagerORM.modified_on >= modified_after)

        data, meta["n_found"] = self.get_query_projection(QueueManagerORM, query, limit=limit, skip=skip)
        meta["success"] = True

        return {"data": data, "meta": meta}

    def get_manager_logs(self, manager_ids: Union[List[str], str], timestamp_after=None, limit=None, skip=0):
        meta = get_metadata_template()
        query = format_query(QueueManagerLogORM, manager_id=manager_ids)

        if timestamp_after:
            query.append(QueueManagerLogORM.timestamp >= timestamp_after)

        data, meta["n_found"] = self.get_query_projection(
            QueueManagerLogORM, query, limit=limit, skip=skip, exclude=["id"]
        )
        meta["success"] = True

        return {"data": data, "meta": meta}

    def _copy_managers(self, record_list: Dict):
        """
        copy the given managers as-is to the DB. Used for data migration

        Parameters
        ----------
        record_list : list of dict of managers data

        Returns
        -------
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        manager_names = []
        with self.session_scope() as session:
            for manager in record_list:
                doc = session.query(QueueManagerORM).filter_by(name=manager["name"])

                if get_count_fast(doc) == 0:
                    doc = QueueManagerORM(**manager)
                    if isinstance(doc.created_on, float):
                        doc.created_on = dt.fromtimestamp(doc.created_on / 1e3)
                    if isinstance(doc.modified_on, float):
                        doc.modified_on = dt.fromtimestamp(doc.modified_on / 1e3)
                    session.add(doc)
                    session.commit()  # TODO: faster if done in bulk
                    manager_names.append(doc.name)
                    meta["n_inserted"] += 1
                else:
                    name = doc.first().name
                    meta["duplicates"].append(name)  # TODO
                    # If new or duplicate, add the id to the return list
                    manager_names.append(id)
        meta["success"] = True

        ret = {"data": manager_names, "meta": meta}
        return ret

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

    def add_user(
        self, username: str, password: Optional[str] = None, permissions: List[str] = ["read"], overwrite: bool = False
    ) -> Tuple[bool, str]:
        """
        Adds a new user and associated permissions.

        Passwords are stored using bcrypt.

        Parameters
        ----------
        username : str
            New user's username
        password : str, optional
            The user's password. If None, a new password will be generated.
        permissions : list of str, optional
            The associated permissions of a user ['read', 'write', 'compute', 'queue', 'admin']
        overwrite: bool, optional
            Overwrite the user if it already exists.
        Returns
        -------
        tuple
            A tuple of (success flag, password)
        """

        # Make sure permissions are valid
        if not self._valid_permissions >= set(permissions):
            raise KeyError("Permissions settings not understood: {}".format(set(permissions) - self._valid_permissions))

        if password is None:
            password = self._generate_password()

        hashed = bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6))

        blob = {"username": username, "password": hashed, "permissions": permissions}

        success = False
        with self.session_scope() as session:
            if overwrite:
                count = session.query(UserORM).filter_by(username=username).update(blob)
                # doc.upsert_one(**blob)
                success = count == 1

            else:
                try:
                    user = UserORM(**blob)
                    session.add(user)
                    session.commit()
                    success = True
                except IntegrityError as err:
                    self.logger.warning(str(err))
                    success = False
                    session.rollback()

        return success, password

    def verify_user(self, username: str, password: str, permission: str) -> Tuple[bool, str]:
        """
        Verifies if a user has the requested permissions or not.

        Passwords are stored and verified using bcrypt.

        Parameters
        ----------
        username : str
            The username to verify
        password : str
            The password associated with the username
        permission : str
            The associated permissions of a user ['read', 'write', 'compute', 'queue', 'admin']

        Returns
        -------
        tuple
            A tuple of (success flag, failure string)

        Examples
        --------

        >>> db.add_user("george", "shortpw")

        >>> db.verify_user("george", "shortpw", "read")[0]
        True

        >>> db.verify_user("george", "shortpw", "admin")[0]
        False

        """

        if self._bypass_security or (self._allow_read and (permission == "read")):
            return (True, "Success")

        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username).first()

            if data is None:
                return (False, "User not found.")

            # Completely general failure
            try:
                pwcheck = bcrypt.checkpw(password.encode("UTF-8"), data.password)
            except Exception as e:
                self.logger.warning(f"Password check failure, error: {str(e)}")
                self.logger.warning(
                    f"Error likely caused by encryption salt mismatch, potentially fixed by creating a new password for user {username}."
                )
                return (False, "Password decryption failure, please contact your database administrator.")

            if pwcheck is False:
                return (False, "Incorrect password.")

            # Admin has access to everything
            if (permission.lower() not in data.permissions) and ("admin" not in data.permissions):
                return (False, "User has insufficient permissions.")

        return (True, "Success")

    def modify_user(
        self,
        username: str,
        password: Optional[str] = None,
        reset_password: bool = False,
        permissions: Optional[List[str]] = None,
    ) -> Tuple[bool, str]:
        """
        Alters a user's password, permissions, or both

        Passwords are stored using bcrypt.

        Parameters
        ----------
        username : str
            The username
        password : str, optional
            The user's new password. If None, the password will not be updated. Excludes reset_password.
        reset_password: bool, optional
            Reset the user's password to a new autogenerated one.
        permissions : list of str, optional
            The associated permissions of a user ['read', 'write', 'compute', 'queue', 'admin']

        Returns
        -------
        tuple
            A tuple of (success flag, message)
        """

        if reset_password and password is not None:
            return False, "only one of reset_password and password may be specified"

        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username).first()

            if data is None:
                return False, f"User {username} not found."

            blob = {"username": username}

            if permissions is not None:
                # Make sure permissions are valid
                if not self._valid_permissions >= set(permissions):
                    return False, "Permissions not understood: {}".format(set(permissions) - self._valid_permissions)
                blob["permissions"] = permissions
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
            count = session.query(UserORM).filter_by(username=username).delete(synchronize_session=False)

        return count == 1

    def get_user_permissions(self, username: str) -> Optional[List[str]]:
        """

        Parameters
        ----------
        username : str
            The username

        Returns
        -------
        Optional[List[str]]
            List of user permissions, or None if user is not found.
        """

        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username).first()
            try:
                ret = data.permissions
            except AttributeError:
                ret = None

        return ret

    def _get_users(self):

        with self.session_scope() as session:
            data = session.query(UserORM).filter().all()
            data = [x.to_dict(exclude=["id"]) for x in data]

        return data

    def _copy_users(self, record_list: Dict):
        """
        copy the given users as-is to the DB. Used for data migration

        Parameters
        ----------
        record_list : list of dict of managers data

        Returns
        -------
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
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

    def check_lib_versions(self):
        """Check the stored versions of elemental and fractal"""

        with self.session_scope() as session:
            db_ver = session.query(VersionsORM).order_by(VersionsORM.created_on.desc())
            if db_ver.count() == 0:
                # FIXME: get versions from the right place
                import qcelemental
                import qcfractal
                import qcengine

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

    def log_server_stats(self):

        table_info = self.custom_query("database_stats", "table_information")["data"]

        # Calculate table info
        table_size = 0
        index_size = 0
        for row in table_info["rows"]:
            table_size += row[2] - row[3] - (row[4] or 0)
            index_size += row[3]

        # Calculate result state info, turns out to be very costly for large databases
        # state_data = self.custom_query("result", "count", groupby={'result_type', 'status'})["data"]
        # result_states = {}

        # for row in state_data:
        #     result_states.setdefault(row["result_type"], {})
        #     result_states[row["result_type"]][row["status"]] = row["count"]
        result_states = {}

        counts = {}
        for table in ["collection", "molecule", "base_result", "kv_store", "access_log"]:
            counts[table] = self.custom_query("database_stats", "table_count", table_name=table)["data"][0]

        # Build out final data
        data = {
            "collection_count": counts["collection"],
            "molecule_count": counts["molecule"],
            "result_count": counts["base_result"],
            "kvstore_count": counts["kv_store"],
            "access_count": counts["access_log"],
            "result_states": result_states,
            "db_total_size": self.custom_query("database_stats", "database_size")["data"],
            "db_table_size": table_size,
            "db_index_size": index_size,
            "db_table_information": table_info,
        }

        with self.session_scope() as session:
            log = ServerStatsLogORM(**data)
            session.add(log)
            session.commit()

        return data

    def get_server_stats_log(self, before=None, after=None, limit=None, skip=0):

        meta = get_metadata_template()
        query = []

        if before:
            query.append(ServerStatsLogORM.timestamp <= before)

        if after:
            query.append(ServerStatsLogORM.timestamp >= after)

        with self.session_scope() as session:
            pose = session.query(ServerStatsLogORM).filter(*query).order_by(desc("timestamp"))
            meta["n_found"] = get_count_fast(pose)

            data = pose.limit(self.get_limit(limit)).offset(skip).all()
            data = [d.to_dict() for d in data]

        meta["success"] = True

        return {"data": data, "meta": meta}
