"""
SQLAlchemy Database class to handle access to Pstgres through ORM
"""

try:
    import sqlalchemy  # lgtm [py/unused-import]
except ImportError:
    raise ImportError("SQLAlchemy_socket requires sqlalchemy, please install this python "
                      "module or try a different db_socket.")

import logging
import secrets
from contextlib import contextmanager
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Union

import bcrypt
from sqlalchemy import create_engine, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, with_polymorphic
from sqlalchemy.sql.expression import func
# from sqlalchemy.dialects import postgresql
from collections.abc import Iterable
import datetime

# pydantic classes
from qcfractal.interface.models import (KeywordSet, Molecule, ObjectId, OptimizationRecord, ResultRecord, TaskRecord,
                                        TaskStatusEnum, TorsionDriveRecord, prepare_basis, GridOptimizationRecord)
# SQL ORMs
from qcfractal.storage_sockets.sql_models import (BaseResultORM, CollectionORM, KeywordsORM, KVStoreORM, MoleculeORM,
                                                  OptimizationProcedureORM, QueueManagerORM, ResultORM,
                                                  ServiceQueueORM, TaskQueueORM, TorsionDriveProcedureORM, UserORM,
                                                  GridOptimizationProcedureORM, VersionsORM)
# from sqlalchemy.dialects.postgresql import insert as postgres_insert
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template

from .sql_models import Base

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
        if (k in _null_keys) and (v == 'null'):
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
        raise TypeError('Procedure of type {} is not valid or supported yet.'.format(type(record)))

    return procedure_class


class SQLAlchemySocket:
    """
        SQLAlcehmy QCDB wrapper class.
    """

    def __init__(self,
                 uri: str,
                 project: str = "molssidb",
                 bypass_security: bool = False,
                 allow_read: bool = True,
                 logger: 'Logger' = None,
                 sql_echo: bool = False,
                 max_limit: int = 1000):
        """
        Constructs a new SQLAlchemy socket

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('SQLAlcehmySocket')

        # Security
        self._bypass_security = bypass_security
        self._allow_read = allow_read

        self._lower_results_index = ["method", "basis", "program"]

        # disconnect from any active default connection
        # disconnect()

        # Connect to DB and create session
        self.engine = create_engine(
            uri,
            echo=sql_echo,  # echo for logging into python logging
            pool_size=5  # 5 is the default, 0 means unlimited
        )
        self.logger.info('Connected SQLAlchemy to DB dialect {} with driver {}'.format(
            self.engine.dialect.name, self.engine.driver))

        self.Session = sessionmaker(bind=self.engine)

        # actually create the tables
        Base.metadata.create_all(self.engine)

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
        return "<SQLAlchemy: address='{0:s}:{1:d}:{2:s}'>".format(str(self._url), self._port, str(self._project_name))

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
            session.query(TaskQueueORM).delete(synchronize_session=False)
            session.query(ServiceQueueORM).delete(synchronize_session=False)
            session.query(GridOptimizationProcedureORM).delete(synchronize_session=False)
            session.query(TorsionDriveProcedureORM).delete(synchronize_session=False)
            session.query(OptimizationProcedureORM).delete(synchronize_session=False)
            session.query(ResultORM).delete(synchronize_session=False)
            session.query(MoleculeORM).delete(synchronize_session=False)
            session.query(CollectionORM).delete(synchronize_session=False)

    def get_project_name(self) -> str:
        return self._project_name

    def get_limit(self, limit: Optional[int]) -> int:
        """Get the allowed limit on results to return in queries based on the
         given `limit`. If this number is greater than the
         mongoengine_soket.max_limit then the max_limit will be returned instead.
        """

        return limit if limit and limit < self._max_limit else self._max_limit

    def get_query_projection(self, className, query, projection, limit, skip, exclude=None):

        with self.session_scope() as session:
            if projection:
                proj = [getattr(className, i) for i in projection]
                data = session.query(*proj).filter(*query)
                n_found = get_count_fast(data)  # before iterating on the data
                data = data.limit(self.get_limit(limit)).offset(skip)
                rdata = [dict(zip(projection, row)) for row in data]
                # print('----------rdata before: ', rdata)
                # transform ids from int into str
                id_fields = className._get_fieldnames_with_DB_ids_()
                for d in rdata:
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
                rdata = [d.to_dict(exclude=exclude) for d in data]

        return rdata, n_found
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
                meta['n_inserted'] += 1

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

        rdata, meta['n_found'] = self.get_query_projection(KVStoreORM, query, None, limit, skip)

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
            for dmol in molecules:

                mol_dict = dmol.json_dict(exclude={"id"})

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

                doc = session.query(MoleculeORM).filter_by(molecule_hash=mol_dict['molecule_hash'])

                if doc.count() == 0:
                    doc = MoleculeORM(**mol_dict)
                    session.add(doc)
                    # Todo: commit at the end, but list itself might have duplicates
                    session.commit()
                    results.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:

                    id = str(doc.first().id)
                    meta['duplicates'].append(id)  # TODO
                    # If new or duplicate, add the id to the return list
                    results.append(id)

                    # We should make sure there was not a hash collision?
                    # new_mol.compare(old_mol)
                    # raise KeyError("!!! WARNING !!!: Hash collision detected")
        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def get_molecules(self, id=None, molecule_hash=None, molecular_formula=None, limit: int = None, skip: int = 0):

        meta = get_metadata_template()

        query = format_query(MoleculeORM, id=id, molecule_hash=molecule_hash, molecular_formula=molecular_formula)
        # query = [getattr(MoleculeORM, 'id') == id,
        #          MoleculeORM.molecule_hash == molecule_hash,
        #          MoleculeORM.molecular_formula == molecular_formula
        # ]

        # Don't include the hash or the molecular_formula in the returned result
        rdata, meta['n_found'] = self.get_query_projection(MoleculeORM,
                                                           query,
                                                           None,
                                                           limit,
                                                           skip,
                                                           exclude=['molecule_hash', 'molecular_formula'])

        meta["success"] = True

        # ret["meta"]["errors"].extend(errors)

        data = [Molecule(**d, validate=False) for d in rdata]

        return {'meta': meta, 'data': data}

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
            ret = session.query(MoleculeORM).filter(*query)\
                                            .delete(synchronize_session=False)

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

                kw_dict = kw.json_dict(exclude={"id"})

                # search by index keywords not by all keys, much faster
                found = session.query(KeywordsORM).filter_by(hash_index=kw_dict['hash_index']).first()
                if not found:
                    doc = KeywordsORM(**kw_dict)
                    session.add(doc)
                    session.commit()
                    keywords.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:
                    meta['duplicates'].append(str(found.id))  # TODO
                    keywords.append(str(found.id))
                meta["success"] = True

        ret = {"data": keywords, "meta": meta}

        return ret

    def get_keywords(self,
                     id: Union[str, list] = None,
                     hash_index: Union[str, list] = None,
                     limit: int = None,
                     skip: int = 0,
                     return_json: bool = False,
                     with_ids: bool = True) -> List[KeywordSet]:
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
            If this number is greater than the mongoengine_soket.max_limit then
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

        rdata, meta['n_found'] = self.get_query_projection(KeywordsORM,
                                                           query,
                                                           None,
                                                           limit,
                                                           skip,
                                                           exclude=[None if with_ids else 'id'])

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
            count = session.query(KeywordsORM).filter_by(id=id)\
                                              .delete(synchronize_session=False)

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

        update_fields = {}
        for field in CollectionORM.col().keys():
            if field in data:
                update_fields[field] = data.pop(field)

        update_fields['extra'] = data  # todo: check for sql injection

        with self.session_scope() as session:

            try:
                if overwrite:
                    col = session.query(CollectionORM).filter_by(collection=collection, lname=lname).first()
                    for key, value in update_fields.items():
                        setattr(col, key, value)
                else:
                    col = CollectionORM(collection=collection, lname=lname, **update_fields)

                session.add(col)
                session.commit()

                col_id = str(col.id)
                meta['success'] = True
                meta['n_inserted'] = 1

            except Exception as err:
                session.rollback()
                meta['error_description'] = str(err)

        ret = {'data': col_id, 'meta': meta}
        return ret

    # def get_collections(self, keys, projection=None):
    def get_collections(self,
                        collection: str = None,
                        name: str = None,
                        return_json: bool = True,
                        with_ids: bool = True,
                        limit: int = None,
                        projection: Dict[str, Any] = None,
                        skip: int = 0) -> Dict[str, Any]:
        """Get collection by collection and/or name

        Parameters
        ----------
        collection : str, optional
        name : str, optional
        return_json : bool
        with_ids : bool
        limit : int
        skip : int

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
        query = format_query(CollectionORM, lname=name, collection=collection)

        # try:
        rdata, meta['n_found'] = self.get_query_projection(CollectionORM,
                                                           query,
                                                           projection,
                                                           limit,
                                                           skip,
                                                           exclude=['lname'])

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
            count = session.query(CollectionORM).filter_by(collection=collection.lower(), lname=name.lower())\
                                                .delete(synchronize_session=False)
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

                doc = session.query(ResultORM).filter_by(program=result.program,
                                                         driver=result.driver,
                                                         method=result.method,
                                                         basis=result.basis,
                                                         keywords=result.keywords,
                                                         molecule=result.molecule)

                if get_count_fast(doc) == 0:
                    doc = ResultORM(**result.json_dict(exclude={"id"}))
                    session.add(doc)
                    session.commit()  # TODO: faster if done in bulk
                    result_ids.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:
                    id = str(doc.first().id)
                    meta['duplicates'].append(id)  # TODO
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

                data = result.json_dict(exclude={'id'})

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
        pass

    def get_results(self,
                    id: Union[str, List] = None,
                    program: str = None,
                    method: str = None,
                    basis: str = None,
                    molecule: str = None,
                    driver: str = None,
                    keywords: str = None,
                    task_id: Union[str, List] = None,
                    status: str = 'COMPLETE',
                    projection=None,
                    limit: int = None,
                    skip: int = 0,
                    return_json=True,
                    with_ids=True):
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
        status : bool, default is 'COMPLETE'
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
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

        query = format_query(ResultORM,
                             id=id,
                             program=program,
                             method=method,
                             basis=basis,
                             molecule=molecule,
                             driver=driver,
                             keywords=keywords,
                             status=status)

        data = []

        # try:
        data, meta['n_found'] = self.get_query_projection(ResultORM, query, projection, limit, skip)
        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

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
            data = session.query(BaseResultORM).filter(BaseResultORM.id == TaskQueueORM.base_result_id)\
                            .filter(TaskQueueORM.id.in_(task_id_list))
            meta['n_found'] = get_count_fast(data)
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

### Mongo procedure/service functions

    def add_procedures(self, record_list: List['BaseRecord']):
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
                    data = procedure.json_dict(exclude={"id"})
                    proc_db = procedure_class(**data)
                    session.add(proc_db)
                    session.commit()
                    proc_db.update_relations(**data)
                    session.commit()
                    procedure_ids.append(str(proc_db.id))
                    meta['n_inserted'] += 1
                else:
                    id = str(doc.first().id)
                    meta['duplicates'].append(id)  # TODO
                    procedure_ids.append(id)
        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

    def get_procedures(self,
                       id: Union[str, List] = None,
                       procedure: str = None,
                       program: str = None,
                       hash_index: str = None,
                       task_id: Union[str, List] = None,
                       status: str = 'COMPLETE',
                       projection=None,
                       limit: int = None,
                       skip: int = 0,
                       return_json=True,
                       with_ids=True):
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

        if procedure == 'optimization':
            className = OptimizationProcedureORM
        elif procedure == 'torsiondrive':
            className = TorsionDriveProcedureORM
        elif procedure == 'gridoptimization':
            className = GridOptimizationProcedureORM
        else:
            # raise TypeError('Unsupported procedure type {}. Id: {}, task_id: {}'
            #                 .format(procedure, id, task_id))
            className = BaseResultORM  # all classes, including those with 'selectin'
            program = None  # make sure it's not used
            if id is None:
                self.logger.error(f'Procedure type not specified({procedure}), and ID is not given.')
                raise KeyError('ID is required if procedure type is not specified.')

        query = format_query(className,
                             id=id,
                             procedure=procedure,
                             program=program,
                             hash_index=hash_index,
                             task_id=task_id,
                             status=status)

        data = []
        try:
            # TODO: decide a way to find the right type

            data, meta['n_found'] = self.get_query_projection(className, query, projection, limit, skip)
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        return {"data": data, "meta": meta}

    def update_procedures(self, records_list: List['BaseRecord']):
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
                    self.logger.error("No procedure id found on update (hash_index={}), skipping.".format(
                        procedure.hash_index))
                    continue

                proc_db = session.query(className).filter_by(id=procedure.id).first()

                data = procedure.json_dict(exclude={'id'})
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
            procedures = session.query(with_polymorphic(BaseResultORM,
                            [OptimizationProcedureORM, TorsionDriveProcedureORM,
                             GridOptimizationProcedureORM]))\
                           .filter(BaseResultORM.id.in_(ids)).all()
            # delete through session to delete correctly from base_result
            for proc in procedures:
                session.delete(proc)
            # session.commit()
            count = len(procedures)

        return count

    def add_services(self, service_list: List['BaseService']):
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
                    doc = ServiceQueueORM(**service.json_dict(include=ServiceQueueORM.__dict__.keys()))
                    doc.extra = service.json_dict(exclude=ServiceQueueORM.__dict__.keys())
                    session.add(doc)
                    session.commit()  # TODO
                    procedure_ids.append(proc_id)
                    meta['n_inserted'] += 1
                else:
                    procedure_ids.append(None)
                    meta["errors"].append((doc.id, "Duplicate service, but not caught by procedure."))

        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

    def get_services(self,
                     id: Union[List[str], str] = None,
                     procedure_id: Union[List[str], str] = None,
                     hash_index: Union[List[str], str] = None,
                     status: str = None,
                     projection=None,
                     limit: int = None,
                     skip: int = 0,
                     return_json=True):
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

        data = []
        # try:
        data, meta['n_found'] = self.get_query_projection(ServiceQueueORM, query, projection, limit, skip)
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
                self.logger.error("No service id found on update (hash_index={}), skipping.".format(
                    service.hash_index))
                continue

            with self.session_scope() as session:

                doc_db = session.query(ServiceQueueORM).filter_by(id=service.id).first()

                data = service.json_dict(include=ServiceQueueORM.__dict__.keys())
                data['extra'] = service.json_dict(exclude=ServiceQueueORM.__dict__.keys())

                data['id'] = int(data['id'])
                for attr, val in data.items():
                    setattr(doc_db, attr, val)

                session.add(doc_db)
                session.commit()

            updated_count += 1

        return updated_count

    def services_completed(self, records_list: List["BaseService"]) -> int:

        done = 0
        for service in records_list:
            if service.id is None:
                self.logger.error("No service id found on completion (hash_index={}), skipping.".format(
                    service.hash_index))
                continue

            # in one transaction
            with self.session_scope() as session:

                procedure = service.output
                procedure.id = service.procedure_id
                self.update_procedures([procedure])

                session.query(ServiceQueueORM)\
                        .filter_by(id=service.id)\
                        .delete() #synchronize_session=False)

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
                    task_dict = record.json_dict(exclude={"id"})
                    # # for compatibility with mongoengine
                    # if isinstance(task_dict['base_result'], dict):
                    #     task_dict['base_result'] = task_dict['base_result']['id']
                    task = TaskQueueORM(**task_dict)
                    session.add(task)
                    session.commit()
                    results.append(str(task.id))
                    meta['n_inserted'] += 1
                except IntegrityError as err:  # rare case
                    # print(str(err))
                    session.rollback()
                    # TODO: merge hooks
                    task = session.query(TaskQueueORM).filter_by(base_result_id=record.base_result.id).first()
                    self.logger.warning('queue_submit got a duplicate task: {}'.format(task.to_dict()))
                    results.append(str(task.id))
                    meta['duplicates'].append(task_num)
                # except Exception as err:
                #     self.logger.warning('queue_submit submission error: {}'.format(str(err)))
                #     meta["success"] = False
                #     meta["errors"].append(str(err))
                #     results.append(None)

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def queue_get_next(self, manager, available_programs, available_procedures, limit=100, tag=None,
                       as_json=True) -> List[TaskRecord]:
        """TODO: needs to be done in a transcation"""

        # Figure out query, tagless has no requirements
        query = format_query(TaskQueueORM, status=TaskStatusEnum.waiting, program=available_programs, tag=tag)

        proc_filt = TaskQueueORM.procedure.in_([p.lower() for p in available_procedures])
        none_filt = TaskQueueORM.procedure == None  # lgtm [py/test-equals-none]
        query.append(or_(proc_filt, none_filt))

        with self.session_scope() as session:
            query = session.query(TaskQueueORM).filter(*query)\
                   .order_by(TaskQueueORM.priority.desc(), TaskQueueORM.created_on)\
                   .limit(limit)

            # print(query.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
            found = query.all()

            ids = [x.id for x in found]
            update_fields = {'status': TaskStatusEnum.running, 'modified_on': dt.utcnow(), 'manager': manager}
            # Bulk update operation in SQL
            update_count = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(ids)).update(
                update_fields, synchronize_session=False)

            if as_json:
                # avoid another trip to the DB to get the updated values, set them here
                found = [TaskRecord(**task.to_dict(exclude=update_fields.keys()), **update_fields) for task in found]
            session.commit()

        if update_count != len(found):
            self.logger.warning("QUEUE: Number of found projects does not match the number of updated projects.")

        return found

    def get_queue(self,
                  id=None,
                  hash_index=None,
                  program=None,
                  status: str = None,
                  base_result: str = None,
                  projection=None,
                  limit: int = None,
                  skip: int = 0,
                  return_json=False,
                  with_ids=True):
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
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
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
        query = format_query(TaskQueueORM,
                             program=program,
                             id=id,
                             hash_index=hash_index,
                             status=status,
                             base_result_id=base_result)

        data = []
        try:
            data, meta['n_found'] = self.get_query_projection(TaskQueueORM, query, projection, limit, skip)
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

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
            found = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(id)).limit(
                self.get_limit(limit)).offset(skip)

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

        update_fields = dict(status=TaskStatusEnum.complete, modified_on=dt.utcnow())
        with self.session_scope() as session:
            # Update the base results
            session.query(BaseResultORM)\
                   .filter(BaseResultORM.id == TaskQueueORM.base_result_id)\
                   .filter(TaskQueueORM.id.in_(task_ids))\
                   .update(update_fields, synchronize_session=False)

            # delete completed tasks
            tasks_c = session.query(TaskQueueORM)\
                             .filter(TaskQueueORM.id.in_(task_ids))\
                             .delete(synchronize_session=False)

        return tasks_c

    def queue_mark_error(self, data: List[Tuple[int, str]]):
        """update the given tasks as errored
        Mark the corresponding result/procedure as Errored

        """

        task_ids = []
        with self.session_scope() as session:
            ids = [err[0] for err in data]
            task_objects = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(ids)).all()
            base_results = session.query(BaseResultORM)\
                                    .filter(BaseResultORM.id == TaskQueueORM.base_result_id) \
                                    .filter(TaskQueueORM.id.in_(ids)).all()
            for (task_id, msg), task_obj, base_result in zip(data, task_objects, base_results):

                task_ids.append(task_id)
                # update task
                task_obj.status = TaskStatusEnum.error
                task_obj.modified_on = dt.utcnow()

                # update result
                base_result.status = TaskStatusEnum.error
                base_result.modified_on = dt.utcnow()
                base_result.error_obj = KVStoreORM(value=msg)

                # session.add(task_obj)

            session.commit()

        return len(task_ids)

    def queue_reset_status(self, manager: str, reset_running: bool = True, reset_error: bool = False) -> int:
        """
        Reset the status of the tasks that a manager owns from Running to Waiting
        If reset_error is True, then also reset errored tasks AND its results/proc

        Parameters
        ----------
        manager : str
            The manager name to reset the status of
        reset_running : str (optional), default is True
            If True, reset running tasks to be waiting
        reset_error : str (optional), default is False
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

        with self.session_scope() as session:
            # Update results and procedures if reset_error
            if reset_error:
                task_ids = session.query(TaskQueueORM.id)\
                                  .filter_by(manager=manager, status=TaskStatusEnum.error)
                session.query(BaseResultORM)\
                          .filter(TaskQueueORM.base_result_id==BaseResultORM.id)\
                          .filter(TaskQueueORM.id.in_(task_ids))\
                          .update(dict(status='INCOMPLETE', modified_on=dt.utcnow()),
                                  synchronize_session=False)

            status = []
            if reset_running:
                status.append("RUNNING")
            if reset_error:
                status.append("ERROR")

            updated = session.query(TaskQueueORM)\
                             .filter(TaskQueueORM.status.in_(status))\
                             .filter_by(manager=manager)\
                             .update(dict(status=TaskStatusEnum.waiting, modified_on=dt.utcnow()),
                                     synchronize_session=False)

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
            count = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids))\
                                              .delete(synchronize_session=False)

        return count

### QueueManagerORMs

    def manager_update(self, name, **kwargs):

        inc_count = {
            # Increment relevant data
            "submitted": QueueManagerORM.submitted + kwargs.pop("submitted", 0),
            "completed": QueueManagerORM.completed + kwargs.pop("completed", 0),
            "returned": QueueManagerORM.returned + kwargs.pop("returned", 0),
            "failures": QueueManagerORM.failures + kwargs.pop("failures", 0)
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

        return num_updated == 1

    def get_managers(self, name: str = None, status: str = None, modified_before=None, limit=None, skip=0):

        meta = get_metadata_template()
        query = format_query(QueueManagerORM, name=name, status=status)

        if modified_before:
            query.append(QueueManagerORM.modified_on <= modified_before)

        data, meta['n_found'] = self.get_query_projection(QueueManagerORM, query, None, limit, skip, exclude=['id'])
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
                doc = session.query(QueueManagerORM).filter_by(name=manager['name'])

                if get_count_fast(doc) == 0:
                    doc = QueueManagerORM(**manager)
                    doc.created_on = datetime.datetime.fromtimestamp(doc.created_on / 1e3)
                    doc.modified_on = datetime.datetime.fromtimestamp(doc.modified_on / 1e3)
                    session.add(doc)
                    session.commit()  # TODO: faster if done in bulk
                    manager_names.append(doc.name)
                    meta['n_inserted'] += 1
                else:
                    name = doc.first().name
                    meta['duplicates'].append(name)  # TODO
                    # If new or duplicate, add the id to the return list
                    manager_names.append(id)
        meta["success"] = True

        ret = {"data": manager_names, "meta": meta}
        return ret


### UserORMs

    def add_user(self,
                 username: str,
                 password: Optional[str] = None,
                 permissions: List[str] = ["read"],
                 overwrite: bool = False) -> Union[bool, str]:
        """
        Adds a new user and associated permissions.

        Passwords are stored using bcrypt.

        Parameters
        ----------
        username : str
            New user's username
        password : str
            The user's password
        permissions : list of str, optional
            The associated permissions of a user ['read', 'write', 'compute', 'queue', 'admin']

        Returns
        -------
        tuple
            Successful insert or not
        """
        valid_permissions = {'read', 'write', 'compute', 'queue', 'admin'}

        # Make sure permissions are valid
        if not valid_permissions >= set(permissions):
            raise KeyError("Permissions settings not understood: {}".format(set(permissions) - valid_permissions))

        return_password = False
        if password is None:
            password = secrets.token_urlsafe(32)
            return_password = True

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

        if return_password and success:
            return password
        else:
            return success

    def verify_user(self, username, password, permission):
        """
        Verifies if a user has the requested permissions or not.

        Passwords are store and verified using bcrypt.

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

        >>> db.verify_user("george", "shortpw", "read")
        True

        >>> db.verify_user("george", "shortpw", "admin")
        False

        """

        if self._bypass_security or (self._allow_read and (permission == "read")):
            return (True, "Success")

        with self.session_scope() as session:
            data = session.query(UserORM).filter_by(username=username).first()

            if data is None:
                return (False, "User not found.")

            pwcheck = bcrypt.checkpw(password.encode("UTF-8"), data.password)
            if pwcheck is False:
                return (False, "Incorrect password.")

            # Admin has access to everything
            if (permission.lower() not in data.permissions) and ("admin" not in data.permissions):
                return (False, "User has insufficient permissions.")

        return (True, "Success")

    def remove_user(self, username):
        """Removes a user from the MongoDB Tables

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
            count = session.query(UserORM).filter_by(username=username)\
                                          .delete(synchronize_session=False)

        return count == 1

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
                current = VersionsORM(elemental_version=elemental_version,
                                      fractal_version=fractal_version,
                                      engine_version=engine_version)
                session.add(current)
                session.commit()
            else:
                current = db_ver.first()

        return current.to_dict(exclude=['id'])

    def get_total_count(self, className, **kwargs):

        with self.session_scope() as session:
            query = session.query(className).filter(**kwargs)
            count = get_count_fast(query)

        return count