"""
Mongoengine Database class to handle access to mongoDB through ODM
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
from datetime import datetime as dt
from typing import Dict, List, Sequence, Union

import bcrypt
import bson.errors
import mongoengine as db
import mongoengine.errors
from bson.objectid import ObjectId
from mongoengine.connection import disconnect, get_db

from .. import interface
from .models import Collection, Keywords, Molecule, Procedure, QueueManager, Result, ServiceQueue, TaskQueue, User
from .storage_utils import add_metadata_template, get_metadata_template, translate_molecule_index


def _str_to_indices(ids):
    for num, x in enumerate(ids):
        if isinstance(x, str):
            ids[num] = ObjectId(x)


def _str_to_indices_with_errors(ids):
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


class MongoengineSocket:
    """
        Mongoengine QCDB wrapper class.
    """

    def __init__(self,
                 uri,
                 project="molssidb",
                 bypass_security=False,
                 authMechanism="SCRAM-SHA-1",
                 authSource=None,
                 logger=None,
                 max_limit=1000):
        """
        Constructs a new socket where url and port points towards a Mongod instance.

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('MongoengineSocket')

        # Security
        self._bypass_security = bypass_security

        self._lower_results_index = ["method", "basis", "keywords", "program"]

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

    def __str__(self):
        return "<MongoSocket: address='{0:s}:{1:d}:{2:s}'>".format(str(self._url), self._port, str(self._tables_name))

    def _clear_db(self, db_name: str):
        """Dangerous, make sure you are deleting the right DB"""

        logging.warning("Clearing database '{}' and dropping all tables.".format(db_name))

        # make sure it's the right DB
        if get_db().name == db_name:
            logging.info('Clearing database: {}'.format(db_name))
            Result.drop_collection()
            Molecule.drop_collection()
            Keywords.drop_collection()
            Collection.drop_collection()
            TaskQueue.drop_collection()
            QueueManager.drop_collection()
            Procedure.drop_collection()
            User.drop_collection()

            self.client.drop_database(db_name)

        QueueManager.objects(name='').first()  # init

    def get_project_name(self):
        return self._project_name

    def get_add_molecules_mixed(self, data):
        """
        Get or add the given molecules (if they don't exit).
        Molecules are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ordered_mol_dict = {indx: mol for indx, mol in enumerate(data)}
        dict_mols = {}
        id_mols = {}
        for idx, mol in ordered_mol_dict.items():
            if isinstance(mol, str):
                id_mols[idx] = mol
            elif isinstance(mol, dict):
                mol.pop("id", None)
                dict_mols[idx] = mol
            elif isinstance(mol, interface.models.common_models.Molecule):
                mol_json = mol.json_dict()
                mol_json.pop("id", None)
                dict_mols[idx] = mol_json
            else:
                meta["errors"].append((idx, "Data type not understood"))

        ret_mols = {}

        # Add all new molecules
        flat_mols = []
        flat_mol_keys = []
        for k, v in dict_mols.items():
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
            ret_mols[inv_id_mols[mol["id"]]] = mol

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

    def _del_by_index(self, table, hashes, index="_id"):
        """
        Helper function that facilitates deletion based on hash.
        """

        if isinstance(hashes, str):
            hashes = [hashes]

        if index == "_id":
            _str_to_indices(hashes)

        return (self._tables[table].delete_many({index: {"$in": hashes}})).deleted_count

### Mongo molecule functions

    def add_molecules(self, data):
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
        # try:
        for dmol in data:
            try:
                dmol = dmol.dict()
            except AttributeError:
                pass

            # All molecules must be fixed
            dmol["fix_com"] = True
            dmol["fix_orientation"] = True

            mol = interface.Molecule(**dmol, orient=False)
            mol_dict = mol.json_dict(exclude={"id"})
            mol_dict.pop("id", None)  # Paranoia

            # Build fresh indices
            mol_dict["molecule_hash"] = mol.get_hash()
            mol_dict["molecular_formula"] = mol.get_molecular_formula()

            mol_dict["identifiers"] = {}
            mol_dict["identifiers"]["molecule_hash"] = mol_dict["molecule_hash"]
            mol_dict["identifiers"]["molecular_formula"] = mol_dict["molecular_formula"]

            # search by index keywords not by all keys, much faster
            doc = Molecule.objects(molecule_hash=mol_dict['molecule_hash'])

            if doc.count() == 0:
                doc = Molecule(**mol_dict).save()
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

    def get_molecules(self, molecule_ids=None, index="id"):

        ret = {"meta": get_metadata_template(), "data": []}

        try:
            index = translate_molecule_index(index)
        except KeyError as e:
            ret["meta"]["error_description"] = repr(e)
            return ret

        if not isinstance(molecule_ids, (list, tuple)):
            molecule_ids = [molecule_ids]

        bad_ids = []
        if index == "id":
            molecule_ids, bad_ids = _str_to_indices_with_errors(molecule_ids)

        # Don't include the hash or the molecular_formula in the returned result
        # Make the query
        query = {index + '__in': molecule_ids}
        data = Molecule.objects(**query).exclude("molecule_hash", "molecular_formula").as_pymongo()

        if data is None:
            data = []
        else:
            data = list(data)

        ret["meta"]["success"] = True
        ret["meta"]["n_found"] = len(data)
        if len(bad_ids):
            ret["meta"]["errors"].append(("Bad Ids", bad_ids))

        # Translate ID's back
        for r in data:
            r["id"] = str(r["_id"])
            del r["_id"]

        ret["data"] = data

        return ret

    def del_molecules(self, values, index="id"):
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

        index = translate_molecule_index(index)

        if isinstance(values, str):
            values = [values]

        query = {index + '__in': values}

        return Molecule.objects(**query).delete()

    ### Mongo options functions

    def add_keywords(self, data: Union[Dict, List[Dict]]):
        """Add one option uniqely identified by 'program' and the 'name'.

        Parameters
        ----------
         data : dict or List[dict]
            The attribites of the 'option' or options to be inserted.
            Must include for each 'option':
                program : str, program name
                name : str, option name

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

        if not isinstance(data, Sequence):
            data = [data]

        meta = add_metadata_template()

        keywords = []
        try:
            for d in data:
                # search by index keywords not by all keys, much faster
                found = Keywords.objects(program=d['program'], hash_index=d['hash_index']).first()
                if not found:

                    # Make sure ID does not exists and is generated by Mongo
                    d = d.copy()
                    d.pop("id", None)

                    doc = Keywords(**d).save()
                    keywords.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:
                    meta['duplicates'].append(d["hash_index"])  # TODO
                    keywords.append(str(found.id))
            meta["success"] = True
        except (mongoengine.errors.ValidationError, KeyError) as err:
            meta["validation_errors"].append(str(err))
            keywords.append(None)
        except Exception as err:
            meta['error_description'] = err
            keywords.append(None)

        ret = {"data": keywords, "meta": meta}
        return ret

    def get_keywords(self,
                     id: str=None,
                     program: str=None,
                     hash_index: str=None,
                     return_json: bool=True,
                     with_ids: bool=True,
                     limit=None):
        """Search for one (unique) option based on the 'program'
        and the 'name'. No overwrite allowed.

        Parameters
        ----------
        program : str
            program name
        return_json : bool, optional
            Return the results as a json object
            Default is True
        with_ids : bool, optional
            Include the DB ids in the returned object (names 'id')
            Default is True
        limit : int, optional
            Maximum number of resaults to return.
            If this number is greater than the mongoengine_soket.max_limit then
            the max_limit will be returned instead.
            Default is to return the socket's max_limit (when limit=None or 0)

        Returns
        -------
            A dict with keys: 'data' and 'meta'
            (see get_metadata_template())
            The 'data' part is an object of the result or None if not found
        """

        meta = get_metadata_template()
        query = {}
        if program is not None:
            query['program'] = program
        if hash_index is not None:
            query['hash_index'] = hash_index
        if id is not None:
            query['id'] = ObjectId(id)
        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            data = Keywords.objects(**query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            rdata = [d.to_json_obj(with_ids) for d in data]
        else:
            rdata = data

        return {"data": rdata, "meta": meta}

    def get_add_keywords_mixed(self, data):
        """
        Get or add the given options (if they don't exit).
        Keywords are given in a mixed format, either as a dict of mol data
        or as existing mol id

        TODO: to be split into get by_id and get_by_data
        """

        meta = get_metadata_template()

        ids = []
        for idx, kw in enumerate(data):
            if isinstance(kw, str):
                ids.append(kw)

            # New dictionary construct and add
            elif isinstance(kw, dict):

                kw = interface.models.common_models.KeywordSet(**kw)

                new_id = self.add_keywords([kw.json_dict()])["data"][0]
                ids.append(new_id)

            elif isinstance(kw, interface.models.common_models.KeywordSet):
                new_id = self.add_keywords([kw.json_dict()])["data"][0]
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

            try:
                ret.append(self.get_keywords(id=id)["data"][0])
            except bson.errors.InvalidId:
                ret.append(None)
                missing.append(idx)

        meta["success"] = True
        meta["n_found"] = len(ret) - len(missing)
        meta["missing"] = missing

        return {"meta": meta, "data": ret}

    def del_keywords(self, id):
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
        kw = Keywords.objects(id=ObjectId(id))
        if kw:
            count = kw.delete()

        return count

    ### Mongo database functions

    # def add_collection(self, data, overwrite=False):
    def add_collection(self, collection: str, name: str, data, overwrite: bool=False):
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

            if overwrite:
                # may use upsert=True to add or update
                col = Collection.objects(collection=collection, name=name).update_one(**data)
            else:
                col = Collection(collection=collection, name=name, **data).save()

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
                        limit: int=None):
        """Get collection by collection and/or name

        Parameters
        ----------
        collection : str, optional
        name : str, optional
        return_json : bool
        with_ids : bool
        limit : int

        Returns
        -------
        A dict with keys: 'data' and 'meta'
            The data is a list of the collections found
        """

        meta = get_metadata_template()
        query = {}
        if collection:
            query['collection'] = collection
        if name:
            query['name'] = name
        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            data = Collection.objects(**query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            rdata = [d.to_json_obj(with_ids) for d in data]
        else:
            rdata = data

        return {"data": rdata, "meta": meta}

    def del_collection(self, collection: str, name: str):
        """
        Remove a collection from the database from its keys.

        Parameters
        ----------
        collection: str
            Collection type
        name : str
            Collection name

        Returns
        -------
        int
            Number of documents deleted
        """

        return Collection.objects(collection=collection, name=name).delete()

    # -------------------------- Results functions ----------------------------
    #
    # def add_result(
    #         self,
    #         program: str,
    #         method: str,
    #         driver: str,
    #         molecule: str,  # Molecule id
    #         basis: str,
    #         options: str,
    #         data: dict,
    #         return_json=True,
    #         with_ids=True):
    #     """ Add one result
    #     """

    def add_results(self, data: List[dict], update_existing: bool=False, return_json=True):
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
        update_existing : bool (default False)
            Update existing results

        Returns
        -------
            Dict with keys: data, meta
            Data is the ids of the inserted/updated/existing docs
        """

        for d in data:
            for i in self._lower_results_index:
                if d[i] is None:
                    continue

                d[i] = d[i].lower()

        meta = add_metadata_template()

        results = []
        # try:
        for d in data:

            d.pop("id", None)
            # search by index keywords not by all keys, much faster
            doc = Result.objects(
                program=d['program'],
                name=d['driver'],
                method=d['method'],
                basis=d['basis'],
                keywords=d['keywords'],
                molecule=d['molecule'])

            if doc.count() == 0 or update_existing:
                if not isinstance(d['molecule'], ObjectId):
                    d['molecule'] = ObjectId(d['molecule'])
                # d['basis'] = d['basis'] if d['basis'] is not None else ""
                # d['keywords'] = d['keywords'] if d['keywords'] is not None else ""
                doc = doc.upsert_one(**d)
                results.append(str(doc.id))
                meta['n_inserted'] += 1
            else:
                id = str(doc.first().id)
                meta['duplicates'].append(id)  # TODO
                # If new or duplicate, add the id to the return list
                results.append(id)
        meta["success"] = True
        # except (mongoengine.errors.ValidationError, KeyError) as err:
        #     meta["validation_errors"].append(err)
        # except Exception as err:
        #     meta['error_description'] = err

        ret = {"data": results, "meta": meta}
        return ret

    def get_results_by_id(self, id: List[str], projection=None, return_json=True, with_ids=True):
        """
        Get list of Results using the given list of Ids

        Parameters
        ----------
        id : List of str
            Ids of the results in the DB
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        return_json : bool, default is True
            Return the results as a list of json inseated of objects
        with_ids: bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()

        data = []
        # try:
        if projection:
            data = Result.objects(id__in=id).only(*projection).limit(self._max_limit)
        else:
            data = Result.objects(id__in=id).limit(self._max_limit)

        meta["n_found"] = data.count()
        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def get_results_count(self):
        """
        TODO: just return the count, used for big queries

        Returns
        -------

        """
        pass

    def get_results(self,
                    program: str=None,
                    method: str=None,
                    basis: str=None,
                    molecule: str=None,
                    driver: str=None,
                    keywords: str=None,
                    status: str='COMPLETE',
                    projection=None,
                    limit: int=None,
                    skip: int=None,
                    return_json=True,
                    with_ids=True):
        """

        Parameters
        ----------
        program : str
        method : str
        basis : str
        molecule : str
            Molecule id in the DB
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
        skip : int, default is None TODO
            skip the first 'skip' resaults. Used to paginate
        return_json : bool, default is True
            Return the results as a list of json inseated of objects
        with_ids : bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()
        query = {}
        parsed_query = {}
        if program is not None:
            query['program'] = program
        if method is not None:
            query['method'] = method
        if basis is not None:
            query['basis'] = basis if basis != 'null' else None
        if molecule is not None:
            query['molecule'], _ = _str_to_indices_with_errors(molecule)
        if driver is not None:
            query['driver'] = driver
        if keywords is not None:
            query['keywords'] = keywords if keywords != 'null' else None
        if status:
            query['status'] = status

        for key, value in query.items():
            if key == "molecule":
                parsed_query[key + "__in"] = query[key]
            elif key == "status":
                parsed_query[key] = value
            elif isinstance(value, (list, tuple)):
                parsed_query[key + "__in"] = [v.lower() for v in value]
            elif isinstance(value, str):
                parsed_query[key] = value.lower()
            else:
                parsed_query[key] = value

        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            if projection:
                data = Result.objects(**parsed_query).only(*projection).limit(q_limit)
            else:
                data = Result.objects(**parsed_query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def get_results_by_task_id(self,
                               task_id: Union[List[str], str],
                               projection=None,
                               limit: int=None,
                               return_json=True):
        """

        Parameters
        ----------
        task_id : List of str or str
            Task id that ran the results
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()
        query = {}

        if isinstance(task_id, (list, tuple)):
            query['task_id__in'] = task_id
        else:
            query['task_id'] = task_id

        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            if projection:
                data = Result.objects(**query).only(*projection).limit(q_limit)
            else:
                data = Result.objects(**query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj() for d in data]

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

        obj_ids = [ObjectId(x) for x in ids]

        return Result.objects(id__in=obj_ids).delete()

### Mongo procedure/service functions

    def add_procedures(self, data: List[dict], update_existing: bool=False, return_json=True):
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

        results = []
        # try:
        for d in data:
            # search by hash index
            d.pop("id", None)
            doc = Procedure.objects(hash_index=d['hash_index'])

            if doc.count() == 0 or update_existing:
                # d['keywords'] = d['keywords'] if d['keywords'] is not None else ""
                doc = doc.upsert_one(**d)
                results.append(str(doc.id))
                meta['n_inserted'] += 1
            else:
                id = str(doc.first().id)
                meta['duplicates'].append(id)  # TODO
                # If new or duplicate, add the id to the return list
                results.append(id)
        meta["success"] = True
        # except (mongoengine.errors.ValidationError, KeyError) as err:
        #     meta["validation_errors"].append(err)
        # except Exception as err:
        #     meta['error_description'] = err

        ret = {"data": results, "meta": meta}
        return ret

    def get_procedures(self,
                       procedure: str=None,
                       program: str=None,
                       hash_index: str=None,
                       ids: List[str]=None,
                       status: str='COMPLETE',
                       projection=None,
                       limit: int=None,
                       skip: int=None,
                       return_json=True,
                       with_ids=True):
        """

        Parameters
        ----------
        procedure : str
        program : str
        hash_index : str
        ids : str
        status : bool, default is 'COMPLETE'
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, default is None TODO
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
        query = {}
        parsed_query = {}
        if procedure:
            query['procedure'] = procedure
        if program:
            query['program'] = program
        if hash_index:
            query['hash_index'] = hash_index
        if ids:
            query['ids'] = ids
        if status:
            query['status'] = status

        for key, value in query.items():
            if key == "status":
                parsed_query[key] = value
            elif isinstance(value, (list, tuple)):
                parsed_query[key + "__in"] = [v.lower() for v in value]
            else:
                parsed_query[key] = value.lower()

        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            if projection:
                data = Procedure.objects(**parsed_query).only(*projection).limit(q_limit)
            else:
                data = Procedure.objects(**parsed_query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def get_procedures_by_id(self,
                             id: List[str]=None,
                             hash_index: List[str]=None,
                             projection=None,
                             return_json=True,
                             with_ids=True):
        """
        Get list of Procedures using the given list of Ids

        Parameters
        ----------
        id : List of str
            Ids of the results in the DB
        hash_index: List or str
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        return_json : bool, default is True
            Return the results as a list of json instead of objects
        with_ids: bool, default is True
            Include the ids in the returned objects/dicts

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()

        query, parsed_query = {}, {}
        if id:
            query['id'] = id
        if hash_index:
            query['hash_index'] = hash_index

        for key, value in query.items():
            if isinstance(value, (list, tuple)):
                parsed_query[key + "__in"] = value
            else:
                parsed_query[key] = value

        data = []
        # try:
        if projection:
            data = Procedure.objects(**parsed_query).only(*projection).limit(self._max_limit)
        else:
            data = Procedure.objects(**parsed_query).limit(self._max_limit)

        meta["n_found"] = data.count()
        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def get_procedures_by_task_id(self,
                                  task_id: Union[List[str], str],
                                  projection=None,
                                  limit: int=None,
                                  return_json=True):
        """

        Parameters
        ----------
        task_id : List of str or str
            Task id that ran the procedure
        projection : list/set/tuple of keys, default is None
            The fields to return, default to return all
        limit : int, default is None
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()
        query = {}

        if isinstance(task_id, (list, tuple)):
            query['task_id__in'] = task_id
        else:
            query['task_id'] = task_id

        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            if projection:
                data = Procedure.objects(**query).only(*projection).limit(q_limit)
            else:
                data = Procedure.objects(**query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            data = [d.to_json_obj() for d in data]

        return {"data": data, "meta": meta}

    def update_procedure(self, hash_index, data):
        """
        TODO: to be updated with needed
        """

        if 'id' in data and data['id'] is None:
            data.pop("id", None)

        update = {}
        not_allowed_keys = []
        # create safe query with allowed keys only
        # shouldn't be allowed to update status manually
        for key, value in data.items():
            if key not in ['procedure', 'program', 'status', 'task_id']:  # FIXME: what else?
                update[key] = value
            else:
                not_allowed_keys.append(key)

        if not_allowed_keys:
            logging.warning('Trying to update Procedure immutable keywords ' +
                            '"{}", skipping'.format(not_allowed_keys))

        modified_count = Procedure.objects(hash_index=hash_index).update(**update, modified_on=dt.utcnow())

        return modified_count

    def add_services(self, data: List[dict], return_json=True):
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

        procedures = []
        for idx, d in enumerate(data):

            # Add the underlying procedure
            new_procedure = self.add_procedures([d["output"]])

            # Procedure already exists
            procedure_id = new_procedure["data"][0]
            if new_procedure["meta"]["duplicates"]:
                procedures.append(procedure_id)
                meta["duplicates"].append(procedure_id)
                continue

            # search by hash index
            d.pop("id", None)
            doc = ServiceQueue.objects(hash_index=d['hash_index'])

            if doc.count() == 0:
                # create stub procedure
                doc = ServiceQueue(**d)
                doc.procedure_id = ObjectId(procedure_id)
                doc.save()
                procedures.append(procedure_id)
                meta['n_inserted'] += 1
            else:
                procedures.append(None)
                meta["errors"].append((idx, "Duplicate service, but not caught by procedure."))

        meta["success"] = True
        # except (mongoengine.errors.ValidationError, KeyError) as err:
        #     meta["validation_errors"].append(err)
        # except Exception as err:
        #     meta['error_description'] = err

        ret = {"data": procedures, "meta": meta}
        return ret

    def get_services(self,
                     id: Union[List[str], str]=None,
                     procedure_id: Union[List[str], str]=None,
                     hash_index: Union[List[str], str]=None,
                     status: str=None,
                     projection=None,
                     limit: int=None,
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
        return_json : bool, deafult is True
            Return the results as a list of json instead of objects

        Returns
        -------
        Dict with keys: data, meta
            Data is the objects found
        """

        meta = get_metadata_template()
        query = {}

        if isinstance(id, (list, tuple)):
            query['id__in'], _ = _str_to_indices_with_errors(id)
        elif id:
            query['id'] = ObjectId(id)

        if isinstance(hash_index, (list, tuple)):
            query['hash_index__in'] = hash_index
        elif hash_index:
            query['hash_index'] = hash_index

        if isinstance(procedure_id, (list, tuple)):
            query['procedure_id__in'], _ = _str_to_indices_with_errors(procedure_id)
        elif procedure_id:
            query['procedure_id'] = ObjectId(procedure_id)

        if status:
            query['status'] = status

        q_limit = int(limit if limit and limit < self._max_limit else self._max_limit)

        data = []
        # try:
        if projection:
            services = ServiceQueue.objects(**query).only(*projection).limit(q_limit)
        else:
            services = ServiceQueue.objects(**query).limit(q_limit)

        meta["n_found"] = services.count()
        meta["success"] = True
        # except Exception as err:
        #     meta['error_description'] = str(err)

        if services.count() and return_json:
            data = [d.to_json_obj() for d in services]

        return {"data": data, "meta": meta}

    def update_services(self, id: str, updates: dict):
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

        # Make sure Id exists and valid for updates
        updates_dict = updates.copy()
        updates_dict.pop("id", None)
        updates_dict.pop('procedure_id', None)
        # ServiceQueue(id=ObjectId(id), **updates_dict).save()
        ServiceQueue.objects(id=ObjectId(id)).update(**updates_dict)

        return True

    def services_completed(self, completed_procedures):

        done = 0

        for service_id, data in completed_procedures:
            procedure_id = ServiceQueue.objects(id=service_id).only("procedure_id").as_pymongo()[0]
            data['status'] = "COMPLETE"
            data.pop('id', None)
            Procedure.objects(id=procedure_id['procedure_id']).update(**data)

            ServiceQueue.objects(id=service_id).delete()

            done += 1

        return done

### Mongo queue handling functions

    def queue_submit(self, data: List[Dict]):
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
            - hooks: list of any objects representing listeners (for now)
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
        for task_num, d in enumerate(data):
            try:
                if not isinstance(d['base_result'], tuple):
                    raise Exception("base_result must be a tuple not {}.".format(type(d['base_result'])))

                # If saved as DBRef, then use raw query to retrieve (avoid this)
                # if d['base_result'][0] in ('results', 'procedure'):
                #     base_result = DBRef(d['base_result'][0], d['base_result'][1])

                d.pop("id", None)
                result_obj = None
                if d['base_result'][0] == 'results':
                    result_obj = Result(id=d['base_result'][1])
                elif d['base_result'][0] == 'procedure':
                    result_obj = Procedure(id=d['base_result'][1])
                else:
                    raise TypeError("Base_result type must be 'results' or 'procedure',"
                                    " {} is given.".format(d['base_result'][0]))
                task = TaskQueue(**d)
                task.base_result = result_obj
                task.save()
                result_obj.update(task_id=str(task.id))  # update bidirectional rel
                results.append(str(task.id))
                meta['n_inserted'] += 1
            except mongoengine.errors.NotUniqueError as err:  # rare case
                # If results is stored as DBRef, get it with:
                # task = TaskQueue.objects(__raw__={'base_result': base_result}).first()  # avoid

                # If base_result is stored as a Result or Procedure class, get it with:
                task = TaskQueue.objects(base_result=result_obj).first()
                self.logger.warning('queue_submit got a duplicate task: ', task.to_mongo())
                if d['hooks']:  # merge hooks
                    task.hooks.extend(d['hooks'])
                    task.save()
                results.append(str(task.id))
                meta['duplicates'].append(task_num)
            except Exception as err:
                meta["success"] = False
                meta["errors"].append(str(err))
                results.append(None)

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

    def queue_get_next(self, manager, limit=100, tag=None, as_json=True):
        """TODO: needs to be done a transcation"""

        # Figure out query, tagless has no requirements
        query = {"status": "WAITING"}
        if tag is not None:
            query["tag"] = tag

        found = TaskQueue.objects(**query).limit(limit).order_by('created_on')

        query = {"_id": {"$in": [x.id for x in found]}}

        # update_many using pymongo in one DB access
        upd = TaskQueue._get_collection().update_many(
            query, {"$set": {
                "status": "RUNNING",
                "modified_on": dt.utcnow(),
                "manager": manager,
            }})

        if as_json:
            found = [task.to_json_obj() for task in found]

        if upd.modified_count != len(found):
            self.logger.warning("QUEUE: Number of found projects does not match the number of updated projects.")

        return found

    # def get_queue_(self, query, projection=None):
    #     """TODO: to be replaced with a specific query, add limit"""
    #
    #     return self._get_generic(query, "task_queue", allow_generic=True, projection=projection)

    def get_queue(self,
                  id=None,
                  hash_index=None,
                  program=None,
                  status: str=None,
                  projection=None,
                  limit: int=None,
                  skip: int=None,
                  return_json=True,
                  with_ids=True):
        """
        TODO: check what query keys are needs
        Parameters
        ----------
        id : list or str
            Id of the task
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
        skip : int, default is None TODO
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
        query = {}
        parsed_query = {}
        if program:
            query['program'] = program
        if id is not None:
            query['id'] = id
        if hash_index:
            query['hash_index'] = hash_index
        if status:
            query['status'] = status

        for key, value in query.items():
            if isinstance(value, (list, tuple)):
                parsed_query[key + "__in"] = value
            else:
                parsed_query[key] = value

        q_limit = limit if limit and limit < self._max_limit else self._max_limit

        data = []
        try:
            if projection:
                data = TaskQueue.objects(**parsed_query).only(*projection).limit(q_limit)
            else:
                data = TaskQueue.objects(**parsed_query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            if return_json:
                data = [d.to_json_obj(with_ids) for d in data]

        return {"data": data, "meta": meta}

    def queue_get_by_id(self, ids: List[str], limit: int=100, as_json: bool=True):
        """Get tasks by their IDs

        Parameters
        ----------
        ids : list of str
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

        q_limit = limit if limit and limit < self._max_limit else self._max_limit
        found = TaskQueue.objects(id__in=ids).limit(q_limit)

        if as_json:
            found = [task.to_json_obj() for task in found]

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

        # If using replica sets, we can use as a transcation:
        # with self.client.start_session() as session:
        #     with session.start_transaction():
        #         # automatically calls ClientSession.commit_transaction().
        #         # If the block exits with an exception, the transaction
        #         # automatically calls ClientSession.abort_transaction().
        #           found = TaskQueue._get_collection().update_many(
        #                               {'id': {'$in': task_ids}},
        #                               {'$set': {'status': 'COMPLETE'}},
        #                               session=session)
        #         # next, Update results

        tasks = TaskQueue.objects(id__in=task_ids).update(status='COMPLETE', modified_on=dt.utcnow())
        results = Result.objects(task_id__in=task_ids).update(status='COMPLETE', modified_on=dt.utcnow())
        procedures = Procedure.objects(task_id__in=task_ids).update(status='COMPLETE', modified_on=dt.utcnow())

        # This should not happen unless there is data inconsistency in the DB
        if results + procedures < tasks:
            logging.error("Some tasks don't reference results or procedures correctly!"
                          "Tasks: {}, Results: {}, procedures: {}. ".format(tasks, results, procedures))
        return tasks

    def queue_mark_error(self, data):
        """update the given tasks as errored
        Mark the corresponding result/procedure as complete

        """

        bulk_commands = []
        task_ids = []
        for task_id, msg in data:
            update = {
                "$set": {
                    "status": "ERROR",
                    "error": msg,
                    "modified_on": dt.utcnow(),
                }
            }
            bulk_commands.append(pymongo.UpdateOne({"_id": ObjectId(task_id)}, update))
            task_ids.append(task_id)

        if len(bulk_commands) == 0:
            return

        ret = TaskQueue._get_collection().bulk_write(bulk_commands, ordered=False).modified_count
        Result.objects(task_id__in=task_ids).update(status='ERROR', modified_on=dt.utcnow())
        Procedure.objects(task_id__in=task_ids).update(status='ERROR', modified_on=dt.utcnow())

        return ret

    def queue_reset_status(self, manager: str, reset_running: bool=True, reset_error: bool=False) -> int:
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

        # Update results and procedures if reset_error
        if reset_error:
            task_ids = TaskQueue.objects(manager=manager, status="ERROR").only('id')
            Result.objects(task_id__in=task_ids).update(status='INCOMPLETE', modified_on=dt.utcnow())
            Procedure.objects(task_id__in=task_ids).update(status='INCOMPLETE', modified_on=dt.utcnow())

        status = []
        if reset_running:
            status.append("RUNNING")
        if reset_error:
            status.append("ERROR")

        updated = TaskQueue.objects(
            manager=manager, status__in=status).update(
                status="WAITING", modified_on=dt.utcnow())

        return updated

    def handle_hooks(self, hooks):

        # Very dangerous, we need to modify this substatially
        # Does not currently handle multiple identical commands
        # Only handles service updates

        bulk_commands = []
        for hook_list in hooks:
            for hook in hook_list:
                commands = {}
                for com in hook["updates"]:
                    commands["$" + com[0]] = {com[1]: com[2]}

                upd = pymongo.UpdateOne({"_id": ObjectId(hook["document"][1])}, commands)
                bulk_commands.append(upd)

        if len(bulk_commands) == 0:
            return

        ret = self._tables["service_queue"].bulk_write(bulk_commands, ordered=False)
        return ret

### QueueManagers

    def manager_update(self, name, **kwargs):

        inc_count = {
            # Increment relevant data
            "inc__submitted": kwargs.pop("submitted", 0),
            "inc__completed": kwargs.pop("completed", 0),
            "inc__returned": kwargs.pop("returned", 0),
            "inc__failures": kwargs.pop("failures", 0)
        }

        upd = {key: kwargs[key] for key in QueueManager._fields_ordered if key in kwargs}

        QueueManager.objects()  # init
        manager = QueueManager.objects(name=name)
        if manager:  # existing
            upd.update(inc_count)
            num_updated = manager.update(**upd, modified_on=dt.utcnow())
        else:  # create new, ensures defaults and validations
            QueueManager(name=name, **upd).save()
            num_updated = 1

        return num_updated == 1

    def get_managers(self, name: str=None, status: str=None, modified_before=None):

        query = {}
        if name:
            query["name"] = name
        if modified_before:
            query["modified_on__lt"] = modified_before
        if status:
            query["status"] = status

        data = QueueManager.objects(**query)

        meta = get_metadata_template()
        meta["success"] = True
        meta["n_found"] = data.count()

        data = [x.to_json_obj(with_id=False) for x in data]

        return {"data": data, "meta": meta}

### Users

    def add_user(self, username, password, permissions=["read"]):
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

        hashed = bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6))
        try:
            User(username=username, password=hashed, permissions=permissions).save()
            return True
        except mongoengine.errors.NotUniqueError:
            return False

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

        if self._bypass_security:
            return (True, "Success")

        data = User.objects(username=username).first()
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
        return User.objects(username=username).delete() == 1
