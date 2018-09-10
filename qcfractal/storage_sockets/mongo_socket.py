"""
Database connection class which directly calls the PyMongo API to capture
common subroutines.
"""

try:
    import pymongo
except ImportError:
    raise ImportError(
        "Mongostorage_socket requires pymongo, please install this python module or try a different db_socket.")

import collections
import copy
import datetime
import logging
import bcrypt

import pandas as pd
from bson.objectid import ObjectId
import bson.errors

from . import storage_utils
# Pull in the hashing algorithms from the client
from .. import interface


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


class MongoSocket:
    """
    This is a Mongo QCDB socket class.
    """

    def __init__(self,
                 url,
                 port,
                 project="molssidb",
                 username=None,
                 password=None,
                 bypass_security=False,
                 authMechanism="SCRAM-SHA-1",
                 authSource=None,
                 logger=None):
        """
        Constructs a new socket where url and port points towards a Mongod instance.

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('MongoSocket')

        # Secuity
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
            "users": ("username", )
        }
        self._valid_tables = set(self._table_indices.keys())
        self._table_unique_indices = {
            "collections": True,
            "options": True,
            "results": True,
            "molecules": False,
            "procedures": False,
            "service_queue": False,
            "task_queue": True,
            "users": True,
        }

        self._lower_results_index = ["method", "basis", "options", "program"]

        self._url = url
        self._port = port

        # Are we authenticating?
        if username:
            self.client = pymongo.MongoClient(
                url, port, username=username, password=password, authMechanism=authMechanism, authSource=authSource)
        else:
            self.client = pymongo.MongoClient(url, port)

        try:
            version_array = self.client.server_info()['versionArray']

            if tuple(version_array) < (3, 2):
                raise RuntimeError
        except AttributeError:
            raise RuntimeError(
                "Could not detect MongoDB version at URL {}. It may be a very old version or installed incorrectly. "
                "Choosing to stop instead of assuming version is at least 3.2.".format(url)
            )
        except RuntimeError:
            # Trap low version
            raise RuntimeError(
                "Connected MongoDB at URL {} needs to be at least version 3.2, found version {}.".format(
                    url,
                    self.client.server_info()['version']
                )
            )

        # Isolate objects to this single project DB
        self._project_name = project
        self._tables = self.client[project]

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
        for tbl in self._valid_tables:
            try:
                # MongoDB "Collection" -> QCFractal "Table"
                self._tables.create_collection(tbl)
                table_creation[tbl] = True

            except pymongo.errors.CollectionInvalid:
                table_creation[tbl] = False

        # Build the indices
        for tbl, indices in self._table_indices.items():
            idx = [(x, pymongo.ASCENDING) for x in indices]
            self._tables[tbl].create_index(idx, unique=self._table_unique_indices[tbl])

        # Special queue index, hash_index should be unique
        self._tables["task_queue"].create_index([("hash_index", pymongo.ASCENDING)], unique=True)

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
            # print(data[0])
            # print([id(x) for x in data])
            tmp = self._tables[table].insert_many(data, ordered=False)
            # print(data[0])
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
        rdata = []
        if return_map:
            for x in (set(range(len(data))) - set(error_skips)):
                d = data[x]
                ukey = tuple(d[key] for key in self._table_indices[table])
                rdata.append((ukey, d["id"]))

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

    def _get_generic_by_id(self, ids, table, projection=None):

        # TODO parse duplicates
        meta = storage_utils.get_metadata()
        _str_to_indices(ids)

        data = list(self._tables[table].find({"_id": {"$in": ids}}, projection=projection))

        for d in data:
            d["id"] = str(d["_id"])
            del d["_id"]

        meta["n_found"] = len(data)
        meta["success"] = True

        return {"meta": meta, "data": data}

    def _get_generic(self, query, table, projection=None, allow_generic=False):

        # TODO parse duplicates
        meta = storage_utils.get_metadata()

        keys = self._table_indices[table]
        len_key = len(keys)

        data = []
        for q in query:
            if allow_generic and isinstance(q, dict):
                pass
            elif (len(q) == len_key) and isinstance(q, (list, tuple)):
                q = {k: v for k, v in zip(keys, q)}
            else:
                meta["errors"].append({"query": q, "error": "Malformed query"})
                continue

            d = self._tables[table].find_one(q, projection=projection)
            if d is None:
                meta["missing"].append(q)
            else:
                data.append(d)

        meta["n_found"] = len(data)
        if len(meta["errors"]) == 0:
            meta["success"] = True

        for d in data:
            d["id"] = str(d["_id"])
            del d["_id"]

        ret = {"meta": meta, "data": data}
        return ret

    def _find_many_generic(self, query, table, projection=None):
        meta = storage_utils.get_metadata()

        data = self._tables[table].find(query, projection=projection)
        meta["success"] = True
        meta["n_found"] = len(data)
        ret = {"meta": meta, "data": data}
        return ret

### Mongo molecule functions

    def add_molecules(self, data):
        """
        Adds molecules to the database.

        Parameters
        ----------
        data : dict or list of dict
            Structured instance of the molecule.

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

### Mongo options functions

    def add_options(self, data):
        """
        Adds options to the database.

        Parameters
        ----------
        data : dict or list of dict
            Structured instance of the options.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        # If only a single promote it to a list
        if isinstance(data, dict):
            data = [data]

        new_options = []
        validation_errors = []
        for dopt in data:

            error = interface.schema.validate(dopt, "options", return_errors=True)
            if error is True:
                new_options.append(dopt)
            else:
                validation_errors.append((dopt, error))

        ret = self._add_generic(new_options, "options")
        ret["meta"]["validation_errors"] = validation_errors
        return ret

    def get_options(self, keys, projection=None):

        # Check for Nones
        blanks = []
        add_keys = []
        for num, (program, name) in enumerate(keys):
            if name.lower() == "none":
                blanks.append((num, {"program": program, "name": name}))
            else:
                add_keys.append((program, name))

        # if (len(data) == 2) and isinstance(data[0], str):
        ret = self._get_generic(add_keys, "options", projection=projection)
        for d in ret["data"]:
            del d["id"]

        for pos, options in blanks:
            ret["data"].insert(pos, options)

        return ret

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
        bool
            Whether the operation was successful.
        """

        return (self._tables["options"].delete_one({"program": program, "name": name})).deleted_count

### Mongo database functions

    def add_collection(self, data, overwrite=False):
        """
        Adds a database to the database.

        Parameters
        ----------
        data : dict
            Structured instance of the database.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        if overwrite:
            ret = {
                "meta": {
                    "errors": [],
                    "n_inserted": 0,
                    "success": False,
                    "duplicates": [],
                    "error_description": False
                },
                "data": [((data["collection"], data["name"]), data["id"])]
            }
            r = self._tables["collections"].replace_one({"_id": ObjectId(data["id"])}, data)
            if r.modified_count == 1:
                ret["meta"]["success"] = True
                ret["meta"]["n_inserted"] = 1

        else:
            ret = self._add_generic([data], "collections")
        ret["meta"]["validation_errors"] = []  # TODO
        return ret

    def get_collections(self, keys):

        return self._get_generic(keys, "collections")

    def del_collection(self, collection, name):
        """
        Removes a database from the database from its hash.

        Parameters
        ----------
        hash_val : str or list of strs
            The hash of a database.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return (self._tables["collections"].delete_one({"collection": collection, "name": name})).deleted_count

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
        if "_id" in query:
            if len(query) > 1:
                ret["error_description"] = "ID index was provided, cannot use other indices"
                return ret

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

    def get_procedures(self, query, by_id=False, projection=None):

        if by_id:
            return self._get_generic_by_id(query, "procedures", projection=projection)
        else:
            return self._get_generic(query, "procedures", allow_generic=True)

    def add_services(self, data):

        ret = self._add_generic(data, "service_queue", return_map=True)
        ret["meta"]["validation_errors"] = []  # TODO

        return ret

    def get_services(self, query, by_id=False, projection=None):

        if by_id:
            return self._get_generic_by_id(query, "service_queue", projection=projection)
        else:
            return self._get_generic(query, "service_queue", projection=projection, allow_generic=True)

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

        ret = self._add_generic(data, "task_queue", return_map=True)

        # Update hooks on duplicates
        dup_inds = set(x[1] for x in ret["meta"]["duplicates"])
        if dup_inds:
            hook_updates = []

            for x in data:
                if x["hash_index"] in dup_inds:
                    upd = pymongo.UpdateOne({
                        "hash_index": x["hash_index"]
                    }, {"$push": {
                        "hooks": {
                            "$each": x["hooks"]
                        }
                    }})
                    hook_updates.append(upd)

            tmp = self._tables["task_queue"].bulk_write(hook_updates)
            if tmp.modified_count != len(dup_inds):
                self.logger.warning("QUEUE: Hook duplicate found does not match hook triggers")

        ret["meta"]["validation_errors"] = []
        return ret

    def queue_get_next(self, n=100, tag=None):

        found = list(self._tables["task_queue"].find(
            {
                "status": "WAITING",
                "tag": tag
            },
            sort=[("created_on", -1)],
            limit=n,
            projection={
                "_id": True,
                "spec": True,
                "hash_index": True,
                "parser": True,
                "hooks": True
            }))

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

    def get_queue(self, query, by_id=False, projection=None):

        if by_id:
            return self._get_generic_by_id(query, "task_queue", projection=projection)
        else:
            return self._get_generic(query, "task_queue", allow_generic=True)

    def queue_get_by_id(self, ids, n=100):

        return list(self._tables["task_queue"].find({"_id": ids}, limit=n))

    def queue_mark_complete(self, ids):
        query = {"_id": {"$in": [ObjectId(x) for x in ids]}}

        rm = self._tables["task_queue"].delete_many(query)
        if rm.deleted_count != len(ids):
            self.logger.warning("QUEUE: Number of complete projects does not match the number of removed projects.")

        # We need to log these for history

        return rm.deleted_count

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
            The associated permissions of a user ['read', 'write', 'compute', 'admin']

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
            The associated permissions of a user ['read', 'write', 'compute', 'admin']

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

        if permission.lower() not in data["permissions"]:
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
