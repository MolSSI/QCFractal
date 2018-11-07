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

import collections
import copy
import datetime
import logging

import bcrypt
import bson.errors
import pandas as pd
from bson.objectid import ObjectId
import json
from typing import List, Union, Dict, Optional

from . import storage_utils
# Pull in the hashing algorithms from the client
from .. import interface

# import models
import mongoengine as db
from qcfractal.storage_sockets.models import Options, Collection

import mongoengine.errors



def _translate_id_index(index):
    if index in ["id", "ids"]:
        return "_id"
    else:
        raise KeyError("Id Index alias '{}' not understood".format(index))


def _str_to_indices(ids):
    for num, x in enumerate(ids):
        if isinstance(x, str):
            ids[num] = ObjectId(x)


def _str_to_indices_with_errors(ids):
    if isinstance(ids, str):
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

        # Static data
        self._table_indices = {
            "collections": interface.schema.get_table_indices("collection"),
            "options": interface.schema.get_table_indices("options"),
            "results": interface.schema.get_table_indices("result"),
            "molecules": interface.schema.get_table_indices("molecule"),
            "procedures": interface.schema.get_table_indices("procedure"),
            "service_queue": interface.schema.get_table_indices("service_queue"),
            "task_queue": interface.schema.get_table_indices("task_queue"),
            "users": ("username", ),
            "queue_managers": ("name", )
        }
        self._valid_tables = set(self._table_indices.keys())
        self._table_unique_indices = {
            "collections": True,
            "options": True,
            "results": True,
            "molecules": False,
            "procedures": False,
            "service_queue": False,
            "task_queue": False,
            "users": True,
            "queue_managers": True,
        }

        self._lower_results_index = ["method", "basis", "options", "program"]

        # Build MongoClient
        self.client = pymongo.MongoClient(uri)
        expanded_uri = pymongo.uri_parser.parse_uri(uri)
        if expanded_uri["password"] is not None:
            self.client = pymongo.MongoClient(uri, authMechanism=authMechanism, authSource=authSource)

            # connect to mongoengine
            self.mongoengine_client = db.connect(
                db=project, host=uri, authMechanism=authMechanism, authSource=authSource)
        else:
            self.client = pymongo.MongoClient(uri)

            # connect to mongoengine
            self.mongoengine_client = db.connect(db=project, host=uri)

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

        new_table = self.init_database()
        for k, v in new_table.items():
            if v:
                self.logger.info("Add '{}' table to the database!".format(k))

    ### Mongo meta functions

    def __str__(self):
        return "<MongoSocket: address='{0:s}:{1:d}:{2:s}'>".format(str(self._url), self._port, str(self._tables_name))

    def init_database(self):
        """
        Builds out the initial project structure.

        This is the Mongo definition of "Database"
        """
        # Try to create a collection for each entry
        table_creation = {}
        for table in self._valid_tables:
            try:
                # MongoDB "Collection" -> QCFractal "Table"
                self._tables.create_collection(table)
                table_creation[table] = True

            except pymongo.errors.CollectionInvalid:
                table_creation[table] = False

        # Build the indices
        for table, indices in self._table_indices.items():
            idx = [(x, pymongo.ASCENDING) for x in indices if x != "hash_index"]
            self._tables[table].create_index(idx, unique=self._table_unique_indices[table])

        # Special queue index, hash_index should be unique
        for table in ["task_queue", "service_queue"]:
            self._tables[table].create_index([("hash_index", pymongo.ASCENDING)], unique=True)

        # Return the success array
        return table_creation

    def get_project_name(self):
        return self._project_name

    def mixed_molecule_get(self, data):
        return storage_utils.mixed_molecule_get(self, data)

    def _add_generic(self, data, table, return_map=True):
        """
        Helper function that facilitates adding a record.
        """

        meta = {"errors": [], "n_inserted": 0, "success": False, "duplicates": [], "error_description": False}

        if len(data) == 0:
            ret = {}
            meta["success"] = True
            ret["meta"] = meta
            ret["data"] = {}
            return ret

        # Try/except for fully successful/partially unsuccessful adds
        error_skips = []
        try:
            tmp = self._tables[table].insert_many(data, ordered=False)
            meta["success"] = tmp.acknowledged
            meta["n_inserted"] = len(tmp.inserted_ids)
        except pymongo.errors.BulkWriteError as tmp:
            meta["success"] = False
            meta["n_inserted"] = tmp.details["nInserted"]
            for error in tmp.details["writeErrors"]:
                ukey = tuple(data[error["index"]][key] for key in self._table_indices[table])
                # Duplicate key errors, add to meta
                if error["code"] == 11000:
                    meta["duplicates"].append(ukey)
                else:
                    meta["errors"].append({"id": str(error["op"]["_id"]), "code": error["code"], "key": ukey})

                error_skips.append(error["index"])

            # Only duplicates, no true errors
            if len(meta["errors"]) == 0:
                meta["success"] = True
                meta["error_description"] = "Found duplicates"
            else:
                meta["error_description"] = "unknown"

        # Convert id in-place
        for d in data:
            d["id"] = str(d["_id"])
            del d["_id"]

        # Add id's of new keys
        skips = set(error_skips)
        rdata = []
        if return_map:
            for x in range(len(data)):
                if x in skips:
                    rdata.append(None)
                else:
                    rdata.append(data[x]["id"])

        ret = {"data": rdata, "meta": meta}

        return ret

    def _del_by_index(self, table, hashes, index="_id"):
        """
        Helper function that facilitates deletion based on hash.
        """

        if isinstance(hashes, str):
            hashes = [hashes]

        if index == "_id":
            _str_to_indices(hashes)

        return (self._tables[table].delete_many({index: {"$in": hashes}})).deleted_count

    def _get_generic(self, query, table, projection=None, allow_generic=False, limit=0):

        # TODO parse duplicates
        meta = storage_utils.get_metadata()

        data = []

        # Assume we want to lookup via unique key tuple
        if isinstance(query, (tuple, list)):
            keys = self._table_indices[table]
            len_key = len(keys)

            for q in query:
                if (len(q) == len_key) and isinstance(q, (list, tuple)):
                    q = {k: v for k, v in zip(keys, q)}
                else:
                    meta["errors"].append({"query": q, "error": "Malformed query"})
                    continue

                d = self._tables[table].find_one(q, projection=projection)
                if d is None:
                    meta["missing"].append(q)
                else:
                    data.append(d)

        elif isinstance(query, dict):

            # Handle specific ID query
            if "id" in query:
                ids, bad_ids = _str_to_indices_with_errors(query["id"])
                if bad_ids:
                    meta["errors"].append(("Bad Ids", bad_ids))

                query["_id"] = ids
                del query["id"]

            for k, v in query.items():
                if isinstance(v, (list, tuple)):
                    query[k] = {"$in": v}

            data = list(self._tables[table].find(query, projection=projection, limit=limit))
        else:
            meta["errors"] = "Malformed query"

        meta["n_found"] = len(data)
        if len(meta["errors"]) == 0:
            meta["success"] = True

        # Convert ID
        for d in data:
            d["id"] = str(d.pop("_id"))

        ret = {"meta": meta, "data": data}
        return ret

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

        # Build a dictionary of new molecules
        new_mols = {}
        for key, dmol in data.items():
            mol = interface.Molecule(dmol, dtype="json", orient=False)
            new_mols[key] = mol

        new_kv_hash = {k: v.get_hash() for k, v in new_mols.items()}
        new_vk_hash = collections.defaultdict(list)
        for k, v in new_kv_hash.items():
            new_vk_hash[v].append(k)

        # We need to filter out what is already in the database
        old_mols = self.get_molecules(list(new_kv_hash.values()), index="hash")["data"]

        # If we have hash matches check to for duplicates
        key_mapper = {}
        for old_mol in old_mols:

            # This is the user provided key
            new_mol_keys = new_vk_hash[old_mol["identifiers"]["molecule_hash"]]
            new_mol = new_mols[new_mol_keys[0]]

            if new_mol.compare(old_mol):
                for x in new_mol_keys:
                    del new_mols[x]
                    key_mapper[x] = old_mol["id"]
            else:
                # If this happens, we need to think a bit about what to do
                # Effectively our molecule hash index now has duplicates.
                # This is *sort of* ok as we use uuid's for all internal projects.
                raise KeyError("!!! WARNING !!!: Hash collision detected")

        # Carefully make this flat
        new_hashes = set()
        new_inserts = []
        new_keys = []
        for new_key, new_mol in new_mols.items():
            data = new_mol.to_json()
            data["identifiers"] = {}

            # Build new molecule hash
            data["molecule_hash"] = new_mol.get_hash()
            data["identifiers"]["molecule_hash"] = data["molecule_hash"]

            if data["molecule_hash"] in new_hashes:
                continue

            # Build chemical identifiers
            data["identifiers"]["molecular_formula"] = new_mol.get_molecular_formula()
            data["molecular_formula"] = data["identifiers"]["molecular_formula"]

            new_hashes |= set([data["molecule_hash"]])
            new_inserts.append(data)
            new_keys.append(new_key)

        ret = self._add_generic(new_inserts, "molecules", return_map=True)
        ret["meta"]["duplicates"].extend(list(key_mapper.keys()))
        ret["meta"]["validation_errors"] = []

        # If something went wrong, we cannot generate the full key map
        # Success should always be True as we are parsing duplicate above and *not* here.
        if ret["meta"]["success"] is False:
            ret["meta"]["error_description"] = "Major insert error."
            ret["data"] = key_mapper
            return ret

        # Add the new keys to the key map
        for mol in new_inserts:
            for x in new_vk_hash[mol["molecule_hash"]]:
                key_mapper[x] = mol["id"]

        ret["data"] = key_mapper

        return ret

    def get_molecules(self, molecule_ids, index="id"):

        ret = {"meta": storage_utils.get_metadata(), "data": []}

        try:
            index = storage_utils.translate_molecule_index(index)
        except KeyError as e:
            ret["meta"]["error_description"] = repr(e)
            return ret

        if not isinstance(molecule_ids, (list, tuple)):
            molecule_ids = [molecule_ids]

        bad_ids = []
        if index == "_id":
            molecule_ids, bad_ids = _str_to_indices_with_errors(molecule_ids)

        # Project out the duplicates we use for top level keys
        proj = {"molecule_hash": False, "molecular_formula": False}

        # Make the query
        data = self._tables["molecules"].find({index: {"$in": molecule_ids}}, projection=proj)

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
            Whether the operation was successful.
        """

        index = storage_utils.translate_molecule_index(index)

        return self._del_by_index("molecules", values, index=index)

    def _doc_to_tuples(self, doc: db.Document, with_ids=True):
        """
        Todo: to be removed
        """
        if not doc:
            return

        table = doc._get_collection_name()

        d_json = json.loads(doc.to_json())
        d_json["id"] = str(doc.id)
        del d_json["_id"]
        ukey = tuple(doc[key] for key in self._table_indices[table])
        if with_ids:
            rdata = (ukey, str(doc.id))
        else:
            rdata = ukey
        return rdata

    def _doc_to_json(self, doc: db.Document, with_ids=True):
        """Rename _id to id, or remove it altogether"""

        if not doc:
            return

        d_json = json.loads(doc.to_json())
        if with_ids:
            d_json["id"] = str(doc.id)

        del d_json["_id"]

        return d_json

    ### Mongo options functions

    def add_options(self, data: Union[Dict, List[Dict]]):
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
            (see storage_utils.add_metadata())
            The 'data' part is a list of ids of the inserted options
            data['duplicates'] has the duplicate entries

        Notes
        ------
            Duplicates are not considered errors.

        """

        if isinstance(data, dict):
            data = [data]

        meta = storage_utils.add_metadata()

        options = []
        try:
            for d in data:
                # search by index keywords not by all keys, much faster
                found = Options.objects(program=d['program'], name=d['name']).first()
                if not found:
                    doc = Options(**d).save()
                    options.append(str(doc.id))
                    meta['n_inserted'] += 1
                else:
                    meta['duplicates'].append(self._doc_to_tuples(found, with_ids=False))  # TODO
            meta["success"] = True
        except (mongoengine.errors.ValidationError, KeyError) as err:
            meta["validation_errors"].append(err)
        except Exception as err:
            meta['error_description'] = err

        ret = {"data": options, "meta": meta}
        return ret

    def get_options(self, program: str=None, name: str=None, return_json: bool=True,
                    with_ids: bool=True, limit=None):
        """Search for one (unique) option based on the 'program'
        and the 'name'.

        Parameters
        ----------
        program : str
            program name
        name : str
            option name
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
            (see storage_utils.get_metadata())
            The 'data' part is an object of the result or None if not found
        """

        meta = storage_utils.get_metadata()
        query = {}
        if program:
            query['program'] = program
        if name:
            query['name'] = name
        q_limit = self._max_limit
        if limit and limit < q_limit:
            q_limit = limit

        data = []
        try:
            data = Options.objects(**query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            rdata = [self._doc_to_json(d, with_ids) for d in data]
        else:
            rdata = data

        return {"data": rdata, "meta": meta}


    def del_option(self, program, name):
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
        option = Options.objects(program=program, name=name)
        if option:
            count = option.delete()

        return count


    ### Mongo database functions

    # def add_collection(self, data, overwrite=False):
    def add_collection(self, collection: str, name: str, data, overwrite: bool=False):
        """Add a collection to the database.

        Parameters
        ----------
        collection : st
        name : st
        data : dict
        overwrite : bool
            Update existing collection

        Returns
        -------
        A dict with keys: 'data' and 'meta'
            (see storage_utils.add_metadata())
            The 'data' part is the id of the inserted document or none

        Notes
        -----
        ** Change: The data doesn't have to include the ID, the document
        is identified by the (collection, name) pairs.
        ** Change: New fields will be added to the collection, but existing won't
            be removed.
        """

        meta = storage_utils.add_metadata()
        col_id = None
        try:
            if overwrite:
                col = Collection.objects(collection=collection, name=name).update_one(**data)
            else:
                col = Collection(collection=collection, name=name, **data).save()

            meta['success'] = True
            meta['n_inserted'] = 1
            col_id = str(col.id)
        except Exception as err:
            meta['error_description'] = err

        ret = {'data': col_id, 'meta': meta}
        return ret

    # def get_collections(self, keys, projection=None):
    def get_collections(self, collection: str=None, name: str=None, return_json: bool=True,
                        with_ids: bool=True, limit: int=None):
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

        meta = storage_utils.get_metadata()
        query = {}
        if collection:
            query['collection'] = collection
        if name:
            query['name'] = name
        q_limit = self._max_limit
        if limit and limit < q_limit:
            q_limit = limit

        data = []
        try:
            data = Collection.objects(**query).limit(q_limit)

            meta["n_found"] = data.count()
            meta["success"] = True
        except Exception as err:
            meta['error_description'] = str(err)

        if return_json:
            rdata = [self._doc_to_json(d, with_ids) for d in data]
        else:
            rdata = data

        return {"data": rdata, "meta": meta}

    def del_collection(self, collection: str, name: str):
        """
        Remove a collection from the database from its keys.

        Parameters
        ----------

        Returns
        -------
        int
            Number of documents deleted
        """

        return Collection.objects(collection=collection, name=name).delete()

### Mongo database functions

    def add_results(self, data):
        """
        Adds a page to the database.

        Parameters
        ----------
        data : dict
            Structured instance of the page.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        for d in data:
            for i in self._lower_results_index:
                d[i] = d[i].lower()

        ret = self._add_generic(data, "results", return_map=True)
        ret["meta"]["validation_errors"] = []  # TODO

        return ret

    # Do a lookup on the results collection using a <molecule, method> key.
    def get_results(self, query, projection=None):

        parsed_query = {}
        ret = {"meta": storage_utils.get_metadata(), "data": []}

        # We are querying via id
        if ("_id" in query) or ("id" in query):
            if len(query) > 1:
                ret["error_description"] = "ID index was provided, cannot use other indices"
                return ret

            if "id" in query:
                query["_id"] = query["id"]

            if not isinstance(query, (list, tuple)):
                parsed_query["_id"] = {"$in": query["_id"]}
                _str_to_indices(parsed_query["_id"]["$in"])
            else:
                parsed_query["_id"] = [query["_id"]]
                _str_to_indices(parsed_query["_id"])

        else:
            # Check if there are unknown keys
            remain = set(query) - set(self._table_indices["results"])
            if remain:
                ret["error_description"] = "Results query found unknown keys {}".format(list(remain))
                return ret

            for key, value in query.items():
                if isinstance(value, (list, tuple)):
                    if key in self._lower_results_index:
                        value = [v.lower() for v in value]
                    parsed_query[key] = {"$in": value}
                else:
                    parsed_query[key] = value.lower()

        # Manipulate the projection
        if projection is None:
            proj = {}
        else:
            proj = copy.deepcopy(projection)

        proj["_id"] = False

        data = self._tables["results"].find(parsed_query, projection=proj)
        if data is None:
            data = []
        else:
            data = list(data)

        ret["meta"]["n_found"] = len(data)
        ret["meta"]["success"] = True

        ret["data"] = data

        return ret

    def del_results(self, values, index="id"):
        """
        Removes a page from the database from its hash.

        Parameters
        ----------
        hash_val : str or list of strs
            The hash of a page.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        index = _translate_id_index(index)

        return self._del_by_index("results", values, index=index)

### Mongo procedure/service functions

    def add_procedures(self, data):

        ret = self._add_generic(data, "procedures")
        ret["meta"]["validation_errors"] = []  # TODO

        return ret

    def get_procedures(self, query, projection=None):

        return self._get_generic(query, "procedures", allow_generic=True, projection=projection)

    def add_services(self, data):

        ret = self._add_generic(data, "service_queue", return_map=True)
        ret["meta"]["validation_errors"] = []  # TODO

        # Right now services expect hash return
        # This and bad and should be fixed
        serv = self.get_services({"id": ret["data"]})
        ret["data"] = [x["hash_index"] for x in serv["data"]]

        # Means we have duplicates in the queue, massage results
        if len(ret["meta"]["duplicates"]):
            ret["meta"]["duplicates"] = [x[2] for x in ret["meta"]["duplicates"]]
            ret["meta"]["error_description"] = False

        return ret

    def get_services(self, query, projection=None, limit=0):

        return self._get_generic(query, "service_queue", projection=projection, allow_generic=True, limit=limit)

    def update_services(self, updates):

        match_count = 0
        modified_count = 0
        for uid, data in updates:
            result = self._tables["service_queue"].replace_one({"_id": ObjectId(uid)}, data)
            match_count += result.matched_count
            modified_count += result.modified_count
        return (match_count, modified_count)

    def del_services(self, values, index="id"):

        index = _translate_id_index(index)

        return self._del_by_index("service_queue", values, index=index)

### Mongo queue handling functions

    def queue_submit(self, data, tag=None):

        dt = datetime.datetime.utcnow()
        for x in data:
            x["status"] = "WAITING"
            x["tag"] = tag
            x["created_on"] = dt
            x["modified_on"] = dt

        # Find duplicates
        ret = self._add_generic(data, "task_queue", return_map=True)

        # Update hooks on duplicates
        dup_inds = set(x[2] for x in ret["meta"]["duplicates"])
        if dup_inds:
            hook_updates = []

            for x in data:
                # No hooks, skip
                if len(x["hooks"]) == 0:
                    continue

                if x["hash_index"] in dup_inds:
                    upd = pymongo.UpdateOne({
                        "hash_index": x["hash_index"]
                    }, {"$push": {
                        "hooks": {
                            "$each": x["hooks"]
                        }
                    }})
                    hook_updates.append(upd)

            # If no hook updates, continue
            if hook_updates:
                tmp = self._tables["task_queue"].bulk_write(hook_updates)
                if tmp.modified_count != len(hook_updates):
                    self.logger.warning("QUEUE: Hook duplicate found does not match hook triggers")

        # Since we did an add generic we get ((status, tag, hashindex), queue_id)
        # Move this to (queue_id)
        ret["data"] = [x for x in ret["data"]]

        # Means we have duplicates in the queue, massage results
        if len(ret["meta"]["duplicates"]):
            # print(ret["meta"]["duplicates"])
            # queue
            hash_indices = [x[2] for x in ret["meta"]["duplicates"]]
            ids = self._get_generic({"hash_index": hash_indices}, "task_queue")
            ret["meta"]["duplicates"] = [x["id"] for x in ids["data"]]
            ret["meta"]["error_description"] = False

        ret["meta"]["validation_errors"] = []
        return ret

    def queue_get_next(self, limit=100, tag=None):

        # Figure out query, tagless has no requirements
        query = {"status": "WAITING"}
        if tag is not None:
            query["tag"] = tag

        found = list(self._tables["task_queue"].find(
            query,
            sort=[("created_on", -1)],
            limit=limit,
            projection={"_id": True,
                        "spec": True,
                        "hash_index": True,
                        "parser": True,
                        "hooks": True}))

        query = {"_id": {"$in": [x["_id"] for x in found]}}

        upd = self._tables["task_queue"].update_many(
            query, {"$set": {
                "status": "RUNNING",
                "modified_on": datetime.datetime.utcnow()
            }})

        for f in found:
            f["id"] = str(f["_id"])
            del f["_id"]

        if upd.modified_count != len(found):
            self.logger.warning("QUEUE: Number of found projects does not match the number of updated projects.")

        return found

    def get_queue(self, query, projection=None):

        return self._get_generic(query, "task_queue", allow_generic=True, projection=projection)

    def queue_get_by_id(self, ids, n=100):

        return list(self._tables["task_queue"].find({"_id": ids}, limit=n))

    def queue_mark_complete(self, updates):

        bulk_commands = []

        now = datetime.datetime.utcnow()
        for queue_id, result_location in updates:
            update = {"$set": {"status": "COMPLETE", "modified_on": now, "result_location": result_location}}
            bulk_commands.append(pymongo.UpdateOne({"_id": ObjectId(queue_id)}, update))

        if len(bulk_commands) == 0:
            return

        ret = self._tables["task_queue"].bulk_write(bulk_commands, ordered=False)
        return ret.modified_count

    def queue_mark_error(self, data):
        bulk_commands = []
        dt = datetime.datetime.utcnow()
        for queue_id, msg in data:
            update = {
                "$set": {
                    "status": "ERROR",
                    "error": msg,
                    "modified_on": dt,
                }
            }
            bulk_commands.append(pymongo.UpdateOne({"_id": ObjectId(queue_id)}, update))

        if len(bulk_commands) == 0:
            return

        ret = self._tables["task_queue"].bulk_write(bulk_commands, ordered=False)
        return ret

    def queue_reset_status(self, data):
        bulk_commands = []
        dt = datetime.datetime.utcnow()
        for queue_id in data:
            update = {
                "$set": {
                    "status": "WAITING",
                    "modified_on": dt,
                }
            }
            bulk_commands.append(pymongo.UpdateOne({"_id": ObjectId(queue_id)}, update))

        if len(bulk_commands) == 0:
            return

        ret = self._tables["task_queue"].bulk_write(bulk_commands, ordered=False)
        return ret

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

    def manager_update(self, name, tag=None, submitted=0, completed=0, failures=0, returned=0):
        dt = datetime.datetime.utcnow()

        r = self._tables["queue_managers"].update_one(
            {
                "name": name
            },
            {
                # Provide base data
                "$setOnInsert": {
                    "name": name,
                    "created_on": dt,
                    "tag": tag,
                },
                # Set the date
                "$set": {
                    "modifed_on": dt,
                },
                # Incremement relevant data
                "$inc": {
                    "submitted": submitted,
                    "completed": completed,
                    "returned": returned,
                    "failures": failures
                }
            },
            upsert=True)
        return r.matched_count == 1

    def get_managers(self, query, projection=None):

        return self._get_generic(query, "queue_managers", allow_generic=True, projection=projection)

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
            self._tables["users"].insert_one({"username": username, "password": hashed, "permissions": permissions})
            return True
        except pymongo.errors.DuplicateKeyError:
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

        data = self._tables["users"].find_one({"username": username})
        if data is None:
            return (False, "User not found.")

        pwcheck = bcrypt.checkpw(password.encode("UTF-8"), data["password"])
        if pwcheck is False:
            return (False, "Incorrect password.")

        # Admin has access to everything
        if (permission.lower() not in data["permissions"]) and ("admin" not in data["permissions"]):
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
        return self._tables["users"].delete_one({"username": username}).deleted_count == 1

### Complex parsers

    def search_qc_variable(self, hashes, field):
        """
        Displays the first `field` value for each molecule in `hashes`.

        Parameters
        ----------
        hashes : list
            A list of molecules hashes.
        field : str
            A page field.

        Returns
        -------
        dataframe
            Returns a dataframe with your results. The rows will have the
            molecule hashes and the column will contain the name. Each cell
            contains the field value for the molecule in that row.

        """
        d = {}
        for mol in hashes:
            command = [{"$match": {"molecule_hash": mol}}, {"$group": {"_id": {}, "value": {"$push": "$" + field}}}]
            results = list(self.project["results"].aggregate(command))
            if len(results) == 0 or len(results[0]["value"]) == 0:
                d[mol] = None
            else:
                d[mol] = results[0]["value"][0]
        return pd.DataFrame(data=d, index=[field]).transpose()
