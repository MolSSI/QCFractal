"""
Mongoengine Database class to handle access to mongoDB through ODM.
"""

try:
    import pymongo
except ImportError:
    raise ImportError(
        "Mongostorage_socket requires pymongo, please install this python module or try a different db_socket.")

try:
    import mongoengine
except ImportError:
    raise ImportError(
        "Mongoengine_socket requires mongoengine, please install this python module or try a different db_socket.")

import logging
import secrets
from datetime import datetime as dt
from typing import Any, Dict, List, Optional, Union

import bcrypt
import bson.errors
import mongoengine as db
import mongoengine.errors
from bson.objectid import ObjectId
from mongoengine.connection import disconnect, get_db

from .me_models import (CollectionORM, KeywordsORM, KVStoreORM, MoleculeORM, ProcedureORM, QueueManagerORM, ResultORM,
                        ServiceQueueORM, TaskQueueORM, UserORM)
from .storage_utils import add_metadata_template, get_metadata_template
from ..interface.models import KeywordSet, Molecule, ResultRecord, TaskRecord, prepare_basis


def _str_to_indices_with_errors(ids: List[Union[str, ObjectId]]):
    if isinstance(ids, (str, ObjectId)):
        ids = [ids]

    good = []
    bad = []
    for x in ids:
        if isinstance(x, str):
            try:
                good.append(ObjectId(x))
            except bson.errors.InvalidId:
                bad.append(x)
        elif isinstance(x, ObjectId):
            good.append(x)
        else:
            bad.append(x)
    return good, bad


_null_keys = {"basis", "keywords"}
_id_keys = {"id", "molecule", "keywords", "procedure_id"}
_lower_func = lambda x: x.lower()
_upper_func = lambda x: x.upper()
_prepare_keys = {"program": _lower_func, "basis": prepare_basis, "method": _lower_func, "procedure": _lower_func, "status": _upper_func}


def format_query(**query: Dict[str, Union[str, List[str]]]) -> Dict[str, Union[str, List[str]]]:
    """
    Formats a query into a MongoEngine description.
    """

    ret = {}
    errors = []
    for k, v in query.items():
        if v is None:
            continue

        # Handle None keys
        k = k.lower()
        if (k in _null_keys) and (v == 'null'):
            v = None

        # Handle ID conversions
        elif k in _id_keys:
            if isinstance(v, (list, tuple)):
                v, bad = _str_to_indices_with_errors(v)
                if bad:
                    errors.append((k, bad))
            else:
                v = ObjectId(v)

        if k in _prepare_keys:
            f = _prepare_keys[k]
            if isinstance(v, (list, tuple)):
                v = [f(x) for x in v]
            else:
                v = f(v)

        if isinstance(v, (list, tuple)):
            ret[k + "__in"] = v
        else:
            ret[k] = v

    return ret, errors


class MongoengineSocket:
    """
        Mongoengine QCDB wrapper class.
    """

    def __init__(self,
                 uri: str,
                 project: str="molssidb",
                 bypass_security: bool=False,
                 allow_read: bool=True,
                 authMechanism: str="SCRAM-SHA-1",
                 authSource: str=None,
                 logger: 'Logger'=None,
                 max_limit: int=1000):
        """
        Constructs a new socket where url and port point towards a Mongod instance.

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('MongoengineSocket')

        # Security
        self._bypass_security = bypass_security
        self._allow_read = allow_read

        self._lower_results_index = ["method", "basis", "program"]

        # disconnect from any active default connection
        disconnect()

        # Build MongoClient
        expanded_uri = pymongo.uri_parser.parse_uri(uri)
        if expanded_uri["password"] is not None:
            # connect to mongoengine
            self.client = db.connect(db=project, host=uri, authMechanism=authMechanism, authSource=authSource)
        else:
            # connect to mongoengine
            self.client = db.connect(db=project, host=uri)

        self._url, self._port = expanded_uri["nodelist"][0]

        try:
            version_array = self.client.server_info()['versionArray']

            if tuple(version_array) < (3, 2):
                raise RuntimeError
        except AttributeError:
            raise RuntimeError(
                "Could not detect MongoDB version at URL {}. It may be a very old version or installed incorrectly. "
                "Choosing to stop instead of assuming version is at least 3.2.".format(uri))
        except RuntimeError:
            # Trap low version
            raise RuntimeError("Connected MongoDB at URL {} needs to be at least version 3.2, found version {}.".
                               format(uri, self.client.server_info()['version']))

        # Isolate objects to this single project DB
        self._project_name = project
        self._tables = self.client[project]
        self._max_limit = max_limit

    ### Mongo meta functions

    def __str__(self) -> str:
        return "<MongoSocket: address='{0:s}:{1:d}:{2:s}'>".format(str(self._url), self._port, str(self._project_name))

    def _clear_db(self, db_name: str):
        """Dangerous, make sure you are deleting the right DB."""

        self.logger.warning("Clearing database '{}' and dropping all tables.".format(db_name))

        # make sure it's the right DB
        if get_db().name == db_name:
            self.logger.info('Clearing database: {}'.format(db_name))
            ResultORM.drop_collection()
            MoleculeORM.drop_collection()
            KeywordsORM.drop_collection()
            KVStoreORM.drop_collection()
            CollectionORM.drop_collection()
            TaskQueueORM.drop_collection()
            ServiceQueueORM.drop_collection()
            QueueManagerORM.drop_collection()
            ProcedureORM.drop_collection()
            UserORM.drop_collection()

            self.client.drop_database(db_name)

        QueueManagerORM.objects(name='').first()  # init

    def _delete_DB_data(self, db_name):
        self._clear_db(db_name)

    def get_project_name(self) -> str:
        return self._project_name

    def get_limit(self, limit: Optional[int]) -> int:
        """Get the allowed limit on results to return in queries based on the
         given `limit`. If this number is greater than the
         mongoengine_soket.max_limit then the max_limit will be returned instead.
        """

        return limit if limit and limit < self._max_limit else self._max_limit

### KV Functions

    def add_kvstore(self, blobs_list: List[Any]):
        """
        Adds to the key/value store table.

        Parameters
        ----------
        blobs_list : List[Any]
            A list of data blobs to add.

        Returns
        -------
        A dict with keys: 'data' and 'meta'
            (see get_metadata_template())
            The 'data' part is an object of the result or None if not found
        """

        meta = add_metadata_template()
        blob_ids = []
        for blob in blobs_list:
            if blob is None:
                blob_ids.append(None)
                continue

            doc = KVStoreORM(value=blob)
            doc.save()
            blob_ids.append(str(doc.id))
            meta['n_inserted'] += 1

        meta["success"] = True

        return {"data": blob_ids, "meta": meta}

    def get_kvstore(self, id: List[str]=None,  limit: int=None, skip: int=0):
        """
        Pulls from the key/value store table.

        Parameters
        ----------
        id : List[str]
            A list of ids to query

        Returns
        -------
        A dict with keys: 'data' and 'meta'
            (see get_metadata_template())
            The 'data' part is an object of the result or None if not found
        """

        meta = get_metadata_template()

        query, errors = format_query(id=id)

        data = KVStoreORM.objects(**query).limit(self.get_limit(limit))\
                                          .skip(skip)

        meta["success"] = True
        meta["n_found"] = data.count()  # all data count, can be > len(data)
        meta["errors"].extend(errors)

        data = [d.to_json_obj() for d in data]
        data = {d["id"]: d["value"] for d in data}
        return {"data": data, "meta": meta}

### Molecule functions

    def get_add_molecules_mixed(self, data: List[Union[str, Molecule]]) -> List[Molecule]:
        """
        Get or add the given molecules (if they don't exit).
        MoleculeORMs are given in a mixed format, either as a dict of mol data
        or as existing mol id.

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ordered_mol_dict = {indx: mol for indx, mol in enumerate(data)}
        new_molecules = {}
        id_mols = {}
        for idx, mol in ordered_mol_dict.items():
            if isinstance(mol, str):
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
        for dmol in molecules:

            mol_dict = dmol.json_dict(exclude={"id"})

            mol_dict["fix_com"] = True
            mol_dict["fix_orientation"] = True

            # Build fresh indices
            mol_dict["molecule_hash"] = dmol.get_hash()
            mol_dict["molecular_formula"] = dmol.get_molecular_formula()

            mol_dict["identifiers"] = {}
            mol_dict["identifiers"]["molecule_hash"] = mol_dict["molecule_hash"]
            mol_dict["identifiers"]["molecular_formula"] = mol_dict["molecular_formula"]

            # search by index keywords not by all keys, much faster
            doc = MoleculeORM.objects(molecule_hash=mol_dict['molecule_hash'])

            if doc.count() == 0:
                doc = MoleculeORM(**mol_dict).save()
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

    def get_molecules(self, id=None, molecule_hash=None, molecular_formula=None, limit: int=None, skip: int=0):

        ret = {"meta": get_metadata_template(), "data": []}

        query, errors = format_query(id=id, molecule_hash=molecule_hash, molecular_formula=molecular_formula)

        # Don't include the hash or the molecular_formula in the returned result
        # Make the query
        data = MoleculeORM.objects(**query).exclude("molecule_hash", "molecular_formula")\
                                        .limit(self.get_limit(limit))\
                                        .skip(skip)\

        ret["meta"]["success"] = True
        ret["meta"]["n_found"] = data.count()  # all data count, can be > len(data)
        ret["meta"]["errors"].extend(errors)

        # Data validated going in
        data = [Molecule(**d.to_json_obj(), validate=False) for d in data]
        ret["data"] = data

        return ret

    def del_molecules(self, id: List[str]=None, molecule_hash: List[str]=None):
        """
        Removes a molecule from the database from its hash.

        Parameters
        ----------
        values : str or list of strs
            The hash of a molecule.

        Returns
        -------
        int
            Number of deleted molecules.
        """

        query, errors = format_query(id=id, molecule_hash=molecule_hash)
        return MoleculeORM.objects(**query).delete()

    ### Mongo options functions

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
        for kw in keyword_sets:

            kw_dict = kw.json_dict(exclude={"id"})

            # search by index keywords not by all keys, much faster
            found = KeywordsORM.objects(hash_index=kw_dict['hash_index']).first()
            if not found:

                doc = KeywordsORM(**kw_dict).save()
                keywords.append(str(doc.id))
                meta['n_inserted'] += 1
            else:
                meta['duplicates'].append(str(found.id))  # TODO
                keywords.append(str(found.id))
            meta["success"] = True

        ret = {"data": keywords, "meta": meta}
        return ret

    def get_keywords(self,
                     id: Union[str, list]=None,
                     hash_index: Union[str, list]=None,
                     limit: int=None,
                     skip: int=0,
                     return_json: bool=True,
                     with_ids: bool=True) -> List[KeywordSet]:
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
        query, errors = format_query(id=id, hash_index=hash_index)

        data = []
        try:
            data = KeywordsORM.objects(**query).limit(self.get_limit(limit)).skip(skip)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:  # TODO: remove
            meta['error_description'] = str(err)

        if return_json:
            rdata = [KeywordSet(**d.to_json_obj(with_ids)) for d in data]
        else:
            rdata = data

        return {"data": rdata, "meta": meta}

    def get_add_keywords_mixed(self, data):
        """
        Get or add the given options (if they don't exit).
        KeywordsORM are given in a mixed format, either as a dict of mol data
        or as existing mol id.

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ids = []
        for idx, kw in enumerate(data):
            if isinstance(kw, str):
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
        Removes a option set from the database based on its keys.

        Parameters
        ----------
        program : str
            The program of the option set
        name : str
            The name of the option set

        Returns
        -------
        int
           number of deleted documents
        """

        # monogoengine
        count = 0
        kw = KeywordsORM.objects(id=ObjectId(id))
        if kw:
            count = kw.delete()

        return count

    ### Mongo database functions

    # def add_collection(self, data, overwrite=False):
    def add_collection(self, data: Dict[str, Any], overwrite: bool=False):
        """Add (or update) a collection to the database.

        Parameters
        ----------
        collection : str
        name : str
        data : dict
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
        try:

            if ("id" in data) and (data["id"] == "local"):
                data.pop("id", None)
            lname = data.get("name").lower()
            collection = data.pop("collection").lower()

            if overwrite:
                # may use upsert=True to add or update
                col = CollectionORM.objects(collection=collection, lname=lname).update_one(**data)
            else:
                col = CollectionORM(collection=collection, lname=lname, **data).save()

            meta['success'] = True
            meta['n_inserted'] = 1
            col_id = str(col.id)
        except Exception as err:
            meta['error_description'] = str(err)

        ret = {'data': col_id, 'meta': meta}
        return ret

    # def get_collections(self, keys, projection=None):
    def get_collections(self,
                        collection: str=None,
                        name: str=None,
                        return_json: bool=True,
                        with_ids: bool=True,
                        limit: int=None,
                        projection: Dict[str, Any]=None,
                        skip: int=0) -> Dict[str, Any]:
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
        query, errors = format_query(lname=name, collection=collection)

        data = []
        try:
            if projection:
                data = CollectionORM.objects(**query).only(*projection).limit(self.get_limit(limit)).skip(skip)
            else:
                data = CollectionORM.objects(**query).exclude("lname").limit(self.get_limit(limit)).skip(skip)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            rdata = [d.to_json_obj(with_ids) for d in data]
        else:
            rdata = data

        return {"data": rdata, "meta": meta}

    def del_collection(self, collection: str, name: str) -> bool:
        """
        Remove a collection from the database based on its keys.

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
        return CollectionORM.objects(collection=collection.lower(), lname=name.lower()).delete()

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
            Where molecule is the molecule ID in the DB
            In addition, it should have the other attributes that it needs
            to store

        Returns
        -------
        Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
        """

        meta = add_metadata_template()

        result_ids = []
        # try:
        for result in record_list:

            doc = ResultORM.objects(
                program=result.program,
                driver=result.driver,
                method=result.method,
                basis=result.basis,
                keywords=result.keywords,
                molecule=result.molecule)

            if doc.count() == 0:
                doc = ResultORM(**result.json_dict(exclude={"id"})).save()
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
        Update results from a given dict (replace existing).

        Parameters
        ----------
        id : list of str
            IDs of the results to update, must exist in the DB
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
                logger.error("Attempted update without ID, skipping")
                continue

            ResultORM(**result.json_dict()).save()
            updated_count += 1

        return updated_count

    def get_results_count(self):
        """
        TODO: just return the count, used for big queries.

        Returns
        -------

        """
        pass

    def get_results(self,
                    id: Union[str, List]=None,
                    program: str=None,
                    method: str=None,
                    basis: str=None,
                    molecule: str=None,
                    driver: str=None,
                    keywords: str=None,
                    task_id: Union[str, List]=None,
                    status: str='COMPLETE',
                    projection=None,
                    limit: int=None,
                    skip: int=0,
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
        task_id : List of str or str
            Task id that ran the results
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
            Return the results as a list of json instead of objects
        with_ids : bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()

        # Ignore status if Id or task_id is present
        if id is not None or task_id is not None:
            status = None

        query, error = format_query(
            id=id,
            program=program,
            method=method,
            basis=basis,
            molecule=molecule,
            driver=driver,
            keywords=keywords,
            status=status)

        q_limit = self.get_limit(limit)

        data = []
        try:
            if projection:
                data = ResultORM.objects(**query).only(*projection).limit(q_limit).skip(skip)
            else:
                data = ResultORM.objects(**query).limit(q_limit).skip(skip)

            meta["n_found"] = data.count()  # total number found, can be >len(data)
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def del_results(self, ids: List[str]):
        """
        Removes results from the database using their ids
        (Should be cautious! other tables maybe referencing results).

        Parameters
        ----------
        ids : list of str
            The Ids of the results to be deleted

        Returns
        -------
        int
            number of results deleted
        """

        obj_ids = [ObjectId(x) for x in ids]

        return ResultORM.objects(id__in=obj_ids).delete()

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

        procedure_ids = []
        for procedure in record_list:
            doc = ProcedureORM.objects(hash_index=procedure.hash_index)

            if doc.count() == 0:
                doc = doc.upsert_one(**procedure.json_dict(exclude={"id"}))
                procedure_ids.append(str(doc.id))
                meta['n_inserted'] += 1
            else:
                id = str(doc.first().id)
                meta['duplicates'].append(id)  # TODO
                procedure_ids.append(id)
        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

    def get_procedures(self,
                       id: Union[str, List]=None,
                       procedure: str=None,
                       program: str=None,
                       hash_index: str=None,
                       task_id: Union[str, List]=None,
                       status: str='COMPLETE',
                       projection=None,
                       limit: int=None,
                       skip: int=0,
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
            skip the first 'skip' results. Used to paginate
        return_json : bool, default is True
            Return the results as a list of json instead of objects
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

        query, error = format_query(
            id=id, procedure=procedure, program=program, hash_index=hash_index, task_id=task_id, status=status)

        q_limit = self.get_limit(limit)

        data = []
        try:
            if projection:
                data = ProcedureORM.objects(**query).only(*projection).limit(q_limit).skip(skip)
            else:
                data = ProcedureORM.objects(**query).limit(q_limit).skip(skip)

            meta["n_found"] = data.count()  # all data count
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def update_procedures(self, records_list: List['BaseRecord']):
        """
        TODO: to be updated with needed
        """

        updated_count = 0
        for procedure in records_list:

            # Must have ID
            if procedure.id is None:
                self.logger.error(
                    "No procedure id found on update (hash_index={}), skipping.".format(procedure.hash_index))
                continue

            ProcedureORM(**procedure.json_dict()).save()
            updated_count += 1

        return updated_count

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
            doc = ServiceQueueORM.objects(hash_index=service.hash_index)
            service.procedure_id = proc_id

            if doc.count() == 0:
                doc = ServiceQueueORM(**service.json_dict(exclude={"id"}))
                doc.save()
                procedure_ids.append(proc_id)
                meta['n_inserted'] += 1
            else:
                procedure_ids.append(None)
                meta["errors"].append((idx, "Duplicate service, but not caught by procedure."))

        meta["success"] = True

        ret = {"data": procedure_ids, "meta": meta}
        return ret

    def get_services(self,
                     id: Union[List[str], str]=None,
                     procedure_id: Union[List[str], str]=None,
                     hash_index: Union[List[str], str]=None,
                     status: str=None,
                     projection=None,
                     limit: int=None,
                     skip: int=0,
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
            skip the first 'skip' results. Used to paginate
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()
        query, error = format_query(id=id, hash_index=hash_index, procedure_id=procedure_id, status=status)

        q_limit = self.get_limit(limit)

        data = []
        # try:
        if projection:
            services = ServiceQueueORM.objects(**query).only(*projection).limit(q_limit).skip(skip)
        else:
            services = ServiceQueueORM.objects(**query).limit(q_limit).skip(skip)

        meta["n_found"] = services.count()
        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

        if services.count() and return_json:
            data = [d.to_json_obj() for d in services]

        return {"data": data, "meta": meta}

    def update_services(self, records_list: List["BaseService"]) -> int:
        """
        Replace existing service.

        Raises exception if the id is invalid.

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
                self.logger.error(
                    "No service id found on update (hash_index={}), skipping.".format(service.hash_index))
                continue

            ServiceQueueORM(**service.json_dict()).save()
            updated_count += 1

        return updated_count

    def services_completed(self, records_list: List["BaseService"]) -> int:

        done = 0
        for service in records_list:
            if service.id is None:
                self.logger.error(
                    "No service id found on completion (hash_index={}), skipping.".format(service.hash_index))
                continue

            procedure = service.output
            procedure.id = service.procedure_id
            self.update_procedures([procedure])

            ServiceQueueORM.objects(id=ObjectId(service.id)).delete()

            done += 1

        return done

### Mongo queue handling functions

    def queue_submit(self, data: List[TaskRecord]):
        """Submit a list of tasks to the queue.
        Tasks are unique by their base_result, which should be inserted into
        the DB first before submitting it's corresponding task to the queue
        (with result.status='INCOMPLETE' as the default).
        The default task.status is 'WAITING'.

        Duplicate tasks sould be a rare case.
        Hooks are merged if the task already exists.

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
        for task_num, record in enumerate(data):
            try:

                result_obj = None
                if record.base_result.ref == 'result':
                    result_obj = ResultORM(id=record.base_result.id)
                elif record.base_result.ref == 'procedure':
                    result_obj = ProcedureORM(id=record.base_result.id)
                else:
                    raise TypeError("Base_result type must be 'results' or 'procedure',"
                                    " {} is given.".format(record.base_result.ref))
                task = TaskQueueORM(**record.json_dict(exclude={"id", "base_result"}))
                task.base_result = result_obj
                task.save()

                result_obj.update(task_id=str(task.id))  # update bidirectional rel
                results.append(str(task.id))
                meta['n_inserted'] += 1
            except mongoengine.errors.NotUniqueError as err:  # rare case
                # If results is stored as DBRef, get it with:
                # task = TaskQueueORM.objects(__raw__={'base_result': base_result}).first()  # avoid

                # If base_result is stored as a ResultORM or ProcedureORM class, get it with:
                task = TaskQueueORM.objects(base_result=result_obj).first()
                self.logger.warning('queue_submit got a duplicate task: {}'.format(task.to_mongo()))
                results.append(str(task.id))
                meta['duplicates'].append(task_num)
            except Exception as err:
                self.logger.warning('queue_submit submission error: {}'.format(str(err)))
                meta["success"] = False
                meta["errors"].append(str(err))
                results.append(None)

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def queue_get_next(self, manager, available_programs, available_procedures, limit=100, tag=None,
                       as_json=True) -> List[TaskRecord]:
        """TODO: needs to be done in a transaction."""

        # Figure out query, tagless has no requirements
        query, error = format_query(
            status="WAITING",
            program=available_programs,
            procedure=available_procedures,  # Procedure can be none, explicitly include
            tag=tag)
        query["procedure__in"].append(None)

        found = TaskQueueORM.objects(**query).limit(limit).order_by('-priority', 'created_on')

        query = {"_id": {"$in": [x.id for x in found]}}

        # update_many using pymongo in one DB access
        upd = TaskQueueORM._get_collection().update_many(
            query, {"$set": {
                "status": "RUNNING",
                "modified_on": dt.utcnow(),
                "manager": manager,
            }})

        if as_json:
            found = [TaskRecord(**task.to_json_obj()) for task in found]

        if upd.modified_count != len(found):
            self.logger.warning("QUEUE: Number of found projects does not match the number of updated projects.")

        return found

    def get_queue(self,
                  id=None,
                  hash_index=None,
                  program=None,
                  status: str=None,
                  base_result: str=None,
                  projection=None,
                  limit: int=None,
                  skip: int=0,
                  return_json=False,
                  with_ids=True):
        """
        TODO: check what query keys are needed.
        Parameters
        ----------
        id : list
            Ids of the tasks
        Hash_index
        status : bool, default is None (find all)
            The status of the task: 'COMPLETE', 'RUNNING', 'WAITING', or 'ERROR'
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, default is None 0
            skip the first 'skip' results. Used to paginate
        return_json : bool, default is True
            Return the results as a list of json instead of objects
        with_ids : bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()
        query, error = format_query(program=program, id=id, hash_index=hash_index, status=status)

        if base_result:
            query['__raw__'] = {'base_result._ref.$id': ObjectId(base_result)}
        q_limit = self.get_limit(limit)

        data = []
        try:
            if projection:
                data = TaskQueueORM.objects(**query).only(*projection).limit(q_limit).skip(skip)
            else:
                data = TaskQueueORM.objects(**query).limit(q_limit).skip(skip)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)


        data = [TaskRecord(**task.to_json_obj()) for task in data]

        return {"data": data, "meta": meta}

    def queue_get_by_id(self, id: List[str], limit: int=None, skip: int=0, as_json: bool=True):
        """Get tasks by their IDs.

        Parameters
        ----------
        id : list of str
            List of the task IDs in the DB
        limit : int (optional)
            max number of returned tasks. If limit > max_limit, max_limit
            will be returned instead (safe query)
        as_json : bool
            Return tasks as JSON

        Returns
        -------
        list of the found tasks
        """

        found = TaskQueueORM.objects(id__in=id).limit(self.get_limit(limit)).skip(skip)

        if as_json:
            found = [TaskRecord(**task.to_json_obj()) for task in found]

        return found

    def queue_mark_complete(self, task_ids: List[str]) -> int:
        """Update the given tasks as complete.
        Note that each task is already pointing to its result location.
        Mark the corresponding result/procedure as complete.

        Parameters
        ----------
        task_ids : list
            IDs of the tasks to mark as COMPLETE

        Returns
        -------
        int
            Updated count
        """

        # If using replica sets, we can use as a transcation:
        # with self.client.start_session() as session:
        #     with session.start_transaction():
        #         # automatically calls ClientSession.commit_transaction().
        #         # If the block exits with an exception, the transaction
        #         # automatically calls ClientSession.abort_transaction().
        #           found = TaskQueueORM._get_collection().update_many(
        #                               {'id': {'$in': task_ids}},
        #                               {'$set': {'status': 'COMPLETE'}},
        #                               session=session)
        #         # next, Update results

        tasks = TaskQueueORM.objects(id__in=task_ids).update(status='COMPLETE', modified_on=dt.utcnow())
        results = ResultORM.objects(task_id__in=task_ids).update(status='COMPLETE', modified_on=dt.utcnow())
        procedures = ProcedureORM.objects(task_id__in=task_ids).update(status='COMPLETE', modified_on=dt.utcnow())

        # This should not happen unless there is data inconsistency in the DB
        if results + procedures < tasks:
            self.logger.error("Some tasks don't reference results or procedures correctly!"
                              "Tasks: {}, ResultORMs: {}, procedures: {}. ".format(tasks, results, procedures))
        return tasks

    def queue_mark_error(self, data):
        """update the given tasks as errored.
        Mark the corresponding result/procedure as complete.

        """

        bulk_commands = []
        bulk_commands_records = []
        task_ids = []
        for task_id, msg in data:
            task_ids.append(task_id)
            update = {
                "$set": {
                    "status": "ERROR",
                    "error": msg,
                    "modified_on": dt.utcnow(),
                }
            }
            bulk_commands.append(pymongo.UpdateOne({"_id": ObjectId(task_id)}, update))

            # Update the objects as well, different from mark complete as these are not processed the same way
            # This design should be overhauled...
            error_id = self.add_kvstore([msg])["data"][0]
            update = {
                "$set": {
                    "status": "ERROR",
                    "error": error_id,
                    "modified_on": dt.utcnow(),
                }
            }
            # Task id is held as a string here... don't move to ObjectId
            bulk_commands_records.append(pymongo.UpdateOne({"task_id": task_id}, update))

        if len(bulk_commands) == 0:
            return

        task_mod = TaskQueueORM._get_collection().bulk_write(bulk_commands, ordered=False).modified_count
        rec_mod = ResultORM._get_collection().bulk_write(bulk_commands_records, ordered=False).modified_count
        rec_mod += ProcedureORM._get_collection().bulk_write(bulk_commands_records, ordered=False).modified_count
        if task_mod != rec_mod:
            self.logger.error(
                "Queue Mark Error: Number of tasks updates {}, does not match the number of records updates {}.".
                format(task_mod, rec_mod))

        return task_mod

    def queue_reset_status(self, manager: str, reset_running: bool=True, reset_error: bool=False) -> int:
        """
        Reset the status of the tasks that a manager owns from Running to Waiting.
        If reset_error is True, then also reset errored tasks AND its results/proc.

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

        # Update results and procedures if reset_error
        if reset_error:
            task_objs = TaskQueueORM.objects(manager=manager, status="ERROR").only('id')
            task_ids = [x.id for x in task_objs]
            ResultORM.objects(task_id__in=task_ids).update(status='INCOMPLETE', modified_on=dt.utcnow())
            ProcedureORM.objects(task_id__in=task_ids).update(status='INCOMPLETE', modified_on=dt.utcnow())

        status = []
        if reset_running:
            status.append("RUNNING")
        if reset_error:
            status.append("ERROR")

        updated = TaskQueueORM.objects(
            manager=manager, status__in=status).update(
                status="WAITING", modified_on=dt.utcnow())

        return updated

    def del_tasks(self, id: Union[str, list]):
        """Delete a task from the queue. Use with caution.

        Parameters
        ----------
        id : str or list
            Ids of the tasks to delete
        Returns
        -------
        int
            Number of tasks deleted
        """

        return TaskQueueORM.objects(id__in=id).delete()

### QueueManagerORMs

    def manager_update(self, name, **kwargs):

        inc_count = {
            # Increment relevant data
            "inc__submitted": kwargs.pop("submitted", 0),
            "inc__completed": kwargs.pop("completed", 0),
            "inc__returned": kwargs.pop("returned", 0),
            "inc__failures": kwargs.pop("failures", 0)
        }

        upd = {key: kwargs[key] for key in QueueManagerORM._fields_ordered if key in kwargs}

        QueueManagerORM.objects()  # init
        manager = QueueManagerORM.objects(name=name)
        if manager:  # existing
            upd.update(inc_count)
            num_updated = manager.update(**upd, modified_on=dt.utcnow())
        else:  # create new, ensures defaults and validations
            QueueManagerORM(name=name, **upd).save()
            num_updated = 1

        return num_updated == 1

    def get_managers(self, name: str=None, status: str=None, modified_before=None):

        query, error = format_query(name=name, status=status)
        if modified_before:
            query["modified_on__lt"] = modified_before

        data = QueueManagerORM.objects(**query)

        meta = get_metadata_template()
        meta["success"] = True
        meta["n_found"] = data.count()

        data = [x.to_json_obj(with_id=False) for x in data]

        return {"data": data, "meta": meta}

### UserORMs

    def add_user(self,
                 username: str,
                 password: Optional[str]=None,
                 permissions: List[str]=["read"],
                 *,
                 overwrite: bool=False) -> Union[bool, str]:
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
        if overwrite:
            doc = UserORM.objects(username=username)
            doc.upsert_one(**blob)
            success = True

        else:
            try:
                UserORM(**blob).save()
                success = True
            except mongoengine.errors.NotUniqueError:
                success = False

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

        data = UserORM.objects(username=username).first()
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
        """Removes a user from the MongoDB Tables.

        Parameters
        ----------
        username : str
            The username to remove

        Returns
        -------
        bool
            If the operation was successful or not.
        """
        return UserORM.objects(username=username).delete() == 1

    def _get_users(self):

        data = UserORM.objects()

        return [x.to_json_obj(with_id=False) for x in data]

    def get_total_count(self, className, **kwargs):

        return className.objects(**kwargs).count()