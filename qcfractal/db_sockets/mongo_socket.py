"""
Database connection class which directly calls the PyMongo API to capture
cammon subroutines.
"""



try:
    import pymongo
except ImportError:
    raise ImportError("Mongo db_socket requires pymongo, please install this python module or try a different db_socket.")

import logging
import pandas as pd
from bson.objectid import ObjectId
import copy

# Pull in the hashing algorithms from the client
from .. import interface
from . import db_utils

def _translate_id_index(index):
    if index in ["id", "ids"]:
        return "_id"
    else:
        raise KeyError("Id Index alias '{}' not understood".format(index))

def _str_to_indices(ids):
    for num, x in enumerate(ids):
        if isinstance(x, str):
            ids[num] = ObjectId(x)

class MongoSocket:
    """
    This is a Mongo QCDB socket class.
    """

    def __init__(self, url, port, project="molssidb", username=None, password=None, authMechanism="SCRAM-SHA-1", authSource=None, logger=None):
        """
        Constructs a new socket where url and port points towards a Mongod instance.

        """

        # Logging data
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('MongoSocket')

        # Static data
        self._valid_collections = {"molecules", "databases", "results", "options", "procedures", "services"}
        self._collection_indices = {
            "databases": interface.schema.get_indices("database"),
            "options": interface.schema.get_indices("options"),
            "results": interface.schema.get_indices("result"),
            "molecules": interface.schema.get_indices("molecule"),
            "procedures": interface.schema.get_indices("procedure"),
            "services": interface.schema.get_indices("service"),
        }
        self._collection_unique_indices = {
            "databases": True,
            "options": True,
            "results": True,
            "molecules": False,
            "procedures": False,
            "services": False,
        }

        self._lower_results_index = ["method", "basis", "options", "program"]

        self._url = url
        self._port = port

        # Are we authenticating?
        if username:
            self.client = pymongo.MongoClient(url, port, username=username, password=password, authMechanism=authMechanism, authSource=authSource)
        else:
            self.client = pymongo.MongoClient(url, port)

        # Isolate objects to this single project DB
        self._project_name = project
        self._project = self.client[project]

        new_collections = self.init_database()
        for k, v in new_collections.items():
            if v:
                self.logger.info("New collection '%s' for database!" % k)

### Mongo meta functions

    def __repr__(self):
        return "<MongoSocket: address='%s:%d:%s'>" % (self._url, self._port, self._project_name)

    def init_database(self):
        """
        Builds out the initial project structure.
        """
        # Try to create a collection for each entry
        collection_creation = {}
        for col in self._valid_collections:
            try:
                self._project.create_collection(col)
                collection_creation[col] = True

            except pymongo.errors.CollectionInvalid:
                collection_creation[col] = False

        # Build the indices
        for col, indices in self._collection_indices.items():
            idx = [(x, pymongo.ASCENDING) for x in indices]
            self._project[col].create_index(idx, unique=self._collection_unique_indices[col])

        # Return the success array
        return collection_creation

    def get_project_name(self):
        return self._project_name

    def mixed_molecule_get(self, data):
        return db_utils.mixed_molecule_get(self, data)

### Mongo add functions

    def _add_generic(self, data, collection, keep_id=False):
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
            tmp = self._project[collection].insert_many(data, ordered=False)
            meta["success"] = tmp.acknowledged
            meta["n_inserted"] = len(tmp.inserted_ids)
        except pymongo.errors.BulkWriteError as tmp:
            meta["success"] = False
            meta["n_inserted"] = tmp.details["nInserted"]
            for error in tmp.details["writeErrors"]:
                ukey = tuple(data[error["index"]][key] for key in self._collection_indices[collection])
                # Duplicate key errors, add to meta
                if error["code"] == 11000:
                    meta["duplicates"].append(ukey)
                else:
                    meta["errors"].append({"id": str(x["op"]["_id"]), "code": x["code"], "key": ukey})

                error_skips.append(error["index"])


            # Only duplicates, no true errors
            if len(meta["errors"]) == 0:
                meta["success"] = True
                meta["error_description"] = "Found duplicates"
            else:
                meta["error_description"] = "unknown"

        # Add id's of new keys
        rdata = []
        if keep_id is False:
            for x in (set(range(len(data))) - set(error_skips)):
                d = data[x]
                ukey = tuple(d[key] for key in self._collection_indices[collection])
                rdata.append((ukey, str(d["_id"])))
                if keep_id is False:
                    del d["_id"]

            for x in error_skips:
                del data[x]["_id"]

        ret = {"data": rdata, "meta": meta}

        return ret

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
        new_vk_hash = {v: k for k, v in new_kv_hash.items()}

        # We need to filter out what is already in the database
        old_mols = self.get_molecules(list(new_kv_hash.values()), index="hash")["data"]

        # If we have hash matches check to for duplicates
        key_mapper = {}
        if old_mols:

            for old_mol in old_mols:

                # This is the user provided key
                new_mol_key = new_vk_hash[old_mol["molecule_hash"]]

                new_mol = new_mols[new_mol_key]

                if new_mol.compare(old_mol):
                    del new_mols[new_mol_key]
                    key_mapper[new_mol_key] = old_mol["id"]
                else:
                    # If this happens, we need to think a bit about what to do
                    # Effectively our molecule hash index now has duplicates.
                    # This is *sort of* ok as we use uuid's for all internal projects.
                    raise KeyError("!!! WARNING !!!: Hash collision detected")

        # Carefully make this flat
        new_inserts = []
        new_keys = []
        for new_key, new_mol in new_mols.items():
            data = new_mol.to_json()
            data["molecule_hash"] = new_mol.get_hash()

            new_inserts.append(data)
            new_keys.append(new_key)

        ret = self._add_generic(new_inserts, "molecules", keep_id=True)
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
            key_mapper[new_vk_hash[mol["molecule_hash"]]] = str(mol["_id"])
            del mol["_id"]

        ret["data"] = key_mapper

        return ret

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

    def add_database(self, data):
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

        ret = self._add_generic([data], "databases")
        ret["meta"]["validation_errors"] = [] # TODO
        return ret

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

        ret = self._add_generic(data, "results")
        ret["meta"]["validation_errors"] = [] # TODO

        return ret

    def add_procedures(self, data):

        ret = self._add_generic(data, "procedures")
        ret["meta"]["validation_errors"] = [] # TODO

        return ret

    def add_services(self, data, keep_id=False):

        ret = self._add_generic(data, "services", keep_id=keep_id)
        ret["meta"]["validation_errors"] = [] # TODO

        return ret

### Mongo Delete Functions

    def _del_by_index(self, collection, hashes, index="_id"):
        """
        Helper function that facilitates deletion based on hash.
        """

        if isinstance(hashes, str):
            hashes = [hashes]

        if index == "_id":
            _str_to_indices(hashes)

        return (self._project[collection].delete_many({index: {"$in" : hashes}})).deleted_count


    def del_molecules(self, values, index="id"):
        """
        Removes a molecule from the database from its hash.

        Parameters
        ----------
        hash_val : str or list of strs
            The hash of a molecule.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        index = db_utils.translate_molecule_index(index)

        return self._del_by_index("molecules", values, index=index)

    def del_option(self, program, name):
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

        return (self._project["options"].delete_one({"program": program, "name": name})).deleted_count

    def del_database(self, category, name):
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

        return (self._project["databases"].delete_one({"category": category, "name": name})).deleted_count


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

### Mongo get functions

    def _get_generic_by_id(self, ids, collection, projection=None):

        # TODO parse duplicates
        meta = db_utils.get_metadata()
        _str_to_indices(ids)

        # if projection is None:
        #     projection = {}

        _str_to_indices(ids)
        data = list(self._project[collection].find({"_id": {"$in": ids}}, projection=projection))
        for d in data:
            d["id"] = str(d["_id"])
            del d["_id"]

        meta["n_found"] = len(data)
        meta["success"] = True

        return {"meta": meta, "data": data}


    def _get_generic(self, query, collection, projection=None, allow_generic=False):

        # TODO parse duplicates
        meta = db_utils.get_metadata()

        if projection is None:
            projection = {"_id": False}

        keys = self._collection_indices[collection]
        len_key = len(keys)

        data = []
        for q in query:
            if allow_generic and isinstance(q, dict):
                pass
            elif (len(q) == len_key) and isinstance(q, (list, tuple)):
                q = {k : v for k, v in zip(keys, q)}
            else:
                meta["errors"].append({"query": q, "error": "Malformed query"})
                continue

            d = self._project[collection].find_one(q, projection=projection)
            if d is None:
                meta["missing"].append(q)
            else:
                data.append(d)

        meta["n_found"] = len(data)
        if len(meta["errors"]) == 0:
            meta["success"] = True

        ret = {"meta": meta, "data": data}
        return ret

    # Do a lookup on the results collection using a <molecule, method> key.
    def get_results(self, query, projection=None):

        parsed_query = {}
        ret = {"meta": db_utils.get_metadata(), "data": []}

        # We are querying via id
        if "_id" in query:
            if len(query) > 1:
                ret["error_description"] = "ID index was provided, cannot use other indices"
                return ret

            if not isinstance(parsed_query, (list, tuple)):
                parsed_query["_id"] = {"$in": query["_id"]}
                _str_to_indices(parsed_query["_id"]["$in"])
            else:
                parsed_query["_id"] = [query["_id"]]
                _str_to_indices(parsed_query["_id"])

        else:
            # Check if there are unknown keys
            remain = set(query) - set(self._collection_indices["results"])
            if remain:
                ret["error_description"] = "Results query found unkown keys {}".format(list(remain))
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

        data = self._project["results"].find(parsed_query, projection=proj)
        if data is None:
            data = []
        else:
            data = list(data)

        ret["meta"]["n_found"] = len(data)
        ret["meta"]["success"] = True

        ret["data"] = data

        return ret

    def get_databases(self, keys):

        return self._get_generic(keys, "databases")

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

        for pos, options in blanks:
            ret["data"].insert(pos, options)

        return ret


    def get_molecules(self, molecule_ids, index="id"):

        ret = {"meta": db_utils.get_metadata(), "data": []}

        try:
            index = db_utils.translate_molecule_index(index)
        except KeyError as e:
            ret["meta"]["error_description"] = repr(e)
            return ret

        if not isinstance(molecule_ids, (list, tuple)):
            molecule_ids = [molecule_ids]

        if index == "_id":
            _str_to_indices(molecule_ids)

        data = self._project["molecules"].find({index: {"$in": molecule_ids}})

        if data is None:
            data = []
        else:
            data = list(data)

        ret["meta"]["success"] = True
        ret["meta"]["n_found"] = len(data)

        # Translate ID's back
        for r in data:
            r["id"] = str(r["_id"])
            del r["_id"]

        ret["data"] = data

        return ret

    def get_procedures(self, keys):

        return self._get_generic(keys, "procedures", allow_generic=True)

    def get_services(self, query, by_id=False, projection=None):

        if by_id:
            return self._get_generic_by_id(query, "services", projection=projection)
        else:
            return self._get_generic(query, "services", projection=projection, allow_generic=True)

    def update_services(self, updates):

        for uid, data in updates:
            d = self._project["services"].replace_one({"_id": ObjectId(uid)}, data)
        return

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
            command = [{
                "$match": {
                    "molecule_hash": mol
                }
            }, {
                "$group": {
                    "_id": {},
                    "value": {
                        "$push": "$" + field
                    }
                }
            }]
            results = list(self.project["results"].aggregate(command))
            if len(results) == 0 or len(results[0]["value"]) == 0:
                d[mol] = None
            else:
                d[mol] = results[0]["value"][0]
        return pd.DataFrame(data=d, index=[field]).transpose()
