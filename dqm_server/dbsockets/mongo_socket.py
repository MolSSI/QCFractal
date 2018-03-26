"""
Database connection class which directly calls the PyMongo API to capture
cammon subroutines.
"""

import pymongo
import pandas as pd
import hashlib
import json
import numpy as np
from dqm_client import fields


class MongoSocket(object):
    """
    This is a Mongo QCDB socket class.
    """

    def __init__(self, url, port, project=None, username=None, password=None, authMechanism="SCRAM-SHA-1", authSource=None, globalAuth=False):
        """
        Constructs a new socket where url and port points towards a Mongod instance.

        """
        self.url = url
        self.port = port
        self.username = username
        self.password = password


        # Are we authenticating?
        # config_path = os.path.expanduser("~/.mdbconfig.json")
        # if (username or password) or os.path.exists(config_path):

        #     # Read from config file
        #     if not (username or password):
        #         with open(config_path) as json_file:
        #             data = json.load(json_file)
        #         username = data["username"]
        #         password = data["password"]
        #         if "authMechanism" in list(data):
        #             authMechanism = data["authMechanism"]
        #         print("Using authentication from ~/.mdbconfig.json %s with %s authentication" % (username, authMechanism))
        #     else:
        #         print("Using supplied authentication %s with %s authentication" % (username, authMechanism))

        #     url = 'mongodb://%s:%s@%s:%s/?authMechanism=%s' % (username, password, url, port, authMechanism)
        #     print(url)
        #     self.client = pymongo.MongoClient(url)
        #     #self.client = pymongo.MongoClient(url, port, user=username, password=password, authMechanism=authMechanism, authSource="admin")

        # # No authentication required
        # else:
        self.globalAuth = False
        if username:
            if globalAuth:
                self.globalAuth = True
            self.client = pymongo.MongoClient(url, port, username=username, password=password, authMechanism=authMechanism, authSource="admin")
        else:
            self.client = pymongo.MongoClient(url, port)

        if (project != None):
            self.set_project(project)
        else:
            self.set_project("default")

    def __repr__(self):
        return "<MongoSocket: address='%s:%d'>" % (self.url, self.port)

    def set_project(self, project, username=None, password=None):
        # Success dictionary and collections to create
        success = {}
        collection_creation = {}

        # Create DB
        self.project = self.client[project]
        if self.globalAuth:
            pass
        elif username:
            self.project.authenticate(username, password)
        elif self.username is not None:
            self.project.authenticate("admin." + self.username, self.password)
        success["project"] = self.project

        # Try to create a collection for each entry
        collections = {"molecules", "databases", "pages"}
        for stri in collections:
            try:
                self.project.create_collection(stri)
                collection_creation[stri] = 1
            except pymongo.errors.CollectionInvalid:
                collection_creation[stri] = 0
        success["collection_creation"] = collection_creation

        # Return the success array, where a value of 1 means successful
        return success

    def get_project(self, project):
        new_project = self.client[project]
        collections = {"molecules", "databases", "pages"}
        for stri in collections:
            try:
                new_project.create_collection(stri)
            except pymongo.errors.CollectionInvalid:
                pass
        return new_project

    def add_molecule(self, data, project=None):
        """
        Adds a molecule to the database.

        Parameters
        ----------
        data : dict
            Structured instance of the molecule.
        project : str
            Name of project to write to.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return self.add_generic(data, "molecules", project)

    def add_database(self, data, project=None):
        """
        Adds a database to the database.

        Parameters
        ----------
        data : dict
            Structured instance of the database.
        project : str
            Name of project to write to.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return self.add_generic(data, "databases", project)

    def add_page(self, data, project=None):
        """
        Adds a page to the database.

        Parameters
        ----------
        data : dict
            Structured instance of the page.
        project : str
            Name of project to write to.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return self.add_generic(data, "pages", project)

    def add_generic(self, data, collection, project):
        """
        Helper function that facilitates adding a record.
        """
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
        sha1 = fields.get_hash(data, collection)
        try:
            data["_id"] = sha1
            selected_project[collection].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    def del_by_hash(self, collection, hashes, project):
        """
        Helper function that facilitates deletion based on hash.
        """
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
        if isinstance(hashes, str):
            return (selected_project[collection].delete_one({"_id": hashes})).deleted_count == 1
        elif isinstance(hashes, list):
            ret = (selected_project[collection].delete_many({"_id": {"$in" : hashes}})).deleted_count
            return ret


    def del_by_data(self, collection, data, project):
        """
        Helper function that facilitates deletion based on structured dict.
        """
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
        if isinstance(data, dict):
            return self.del_by_hash(collection, fields.get_hash(data, collection), project)
        elif isinstance(data, list):
            arr = []
            for item in data:
                arr.append(fields.get_hash(item, collection))
            return self.del_by_hash(collection, arr, project)

    def del_molecule_by_data(self, data, project=None):
        """
        Removes a molecule from the database from its raw data.

        Parameters
        ----------
        data : dict or list of dicts
            Structured instance of the molecule.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return self.del_by_data("molecules", data, project)

    def del_molecule_by_hash(self, hash_val, project=None):
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

        return self.del_by_hash("molecules", hash_val, project)

    def del_database_by_data(self, data, project=None):
        """
        Removes a database from the database from its raw data.

        Parameters
        ----------
        data : dict or list of dicts
            Structured instance of the database.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return self.del_by_data("databases", data, project)

    def del_database_by_hash(self, hash_val, project=None):
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

        return self.del_by_hash("databases", hash_val, project)

    def del_page_by_data(self, data, project=None):
        """
        Removes a page from the database from its raw data.

        Parameters
        ----------
        data : dict or list of dicts
            Structured instance of the page.

        Returns
        -------
        bool
            Whether the operation was successful.
        """

        return self.del_by_data("pages", data, project)

    def del_page_by_hash(self, hash_val, project=None):
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

        return self.del_by_hash("pages", hash_val, project)

    def evaluate(self, hashes, methods, field="return_value", project=None):
        """
        Queries monogod for all pages containing a molecule specified in
        `hashes` and a method specified in `methods`. For all matches, finds
        their `field` value and populates the relevant dataframe cell.

        Parameters
        ----------
        hashes : list
            A list of molecules hashes.
        methods : list
            A list of methods (modelchems).
        field : "return_value", optional
            A page field.

        Returns
        -------
        dataframe
            Returns a dataframe with your results. The rows will have the
            molecule hashes and the columns will have the method names. Each
            dataframe[molecule][method] cell contains the respective field
            value.

        Notes
        -----
        Empty cells will contain NaN.

        """
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
        hashes = list(hashes)
        methods = list(methods)
        command = [{"$match": {"molecule_hash": {"$in": hashes}, "modelchem": {"$in": methods}}}]
        pages = list(selected_project["pages"].aggregate(command))
        d = {}
        for mol in hashes:
            for method in methods:
                d[mol] = {}
                d[mol][method] = np.nan
        for item in pages:
            scope = item
            try:
                for name in field.split("."):
                    scope = scope[name]
                d[item["molecule_hash"]][item["modelchem"]] = scope
            except KeyError:
                pass
        if isinstance(methods, str):
            methods = [methods]
        return pd.DataFrame(data=d, index=methods).transpose()

    def evaluate_2(self, hashes, fields, method, project=None):
        """
        Queries monogod for all pages containing a molecule specified in
        `hashes` of method `method`. For all matches, finds the values in each
        of their `fields` populates the relevant dataframe cell.

        Parameters
        ----------
        hashes : list
            A list of molecules hashes.
        fields : list
            A list of page fields.
        method : str
            A method (modelchem).

        Returns
        -------
        dataframe
            Returns a dataframe with your results. The rows will have the
            molecule hashes and the columns will have the field names. Each
            dataframe[molecule][field] cell contains the respective field
            value.

        Notes
        -----
        Empty cells will contain NaN.

        """
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
        hashes = list(hashes)
        command = [{"$match": {"molecule_hash": {"$in": hashes}, "modelchem": method}}]
        pages = list(selected_project["pages"].aggregate(command))
        d = {}
        for mol in hashes:
            for field in fields:
                d[mol] = {}
                d[mol][field] = np.nan
        for item in pages:
            for field in fields:
                scope = item
                try:
                    for name in field.split("."):
                        scope = scope[name]
                    d[item["molecule_hash"]][field] = scope
                except KeyError:
                    pass
        return pd.DataFrame(data=d, index=[fields]).transpose()

    def list_methods(self, hashes, project=None):
        """
        Displays all methods that are used by each molecule in `hashes`.

        Parameters
        ----------
        hashes : list
            A list of molecules hashes.

        Returns
        -------
        dataframe
            Returns a dataframe with your results. The rows will have the
            molecule hashes and the columns will be numbered. Each cell contains
            a method used by the molecule in that row.

        """
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
        d = {}
        for mol in hashes:
            records = list(selected_project["pages"].find({"molecule_hash": mol}))
            d[mol] = []
            for rec in records:
                d[mol].append(rec["modelchem"])

        df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in d.items()])).transpose()
        return df

    def search_qc_variable(self, hashes, field, project=None):
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
        selected_project = self.project
        if (project != None):
            selected_project = self.get_project(project)
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
            pages = list(selected_project["pages"].aggregate(command))
            if (len(pages) == 0 or len(pages[0]["value"]) == 0):
                d[mol] = None
            else:
                d[mol] = pages[0]["value"][0]
        return pd.DataFrame(data=d, index=[field]).transpose()

    def list_projects(self):
        """
        Lists the databases in this mongod instance

        Returns
        -------
        projects : list
            List of database names.
        """
        projects = []
        for project_name in self.client.database_names():
            projects.append(project_name)
        return projects

    def push_to(self, url, port, remote_project, project=None):
        """
        Inserts all documents from the local project into the remote one.

        Parameters
        ----------
        url : str
            Connection string.
        port : str
            Connection port.
        remote_project : str
            Name of remote project.
        """
        self.generic_copy(url, port, remote_project, False, project)

    def clone_to(self, url, port, remote_project, project=None):
        """
        Replaces the remote project with the local one.

        Parameters
        ----------
        url : str
            Connection string.
        port : str
            Connection port.
        remote_project : str
            Name of remote project.
        """
        self.generic_copy(url, port, remote_project, True, project)

    def generic_copy(self, url, port, remote_project, delete, local_project):
        """
            Helper function for facilitating syncing.
        """
        selected_project = self.project
        if (local_project != None):
            selected_project = self.get_project(local_project)
        remote = MongoSocket(url, port)
        if (delete):
            remote.client.drop_database(remote_project)
        for col in ["molecules", "databases", "pages"]:
            cursor = selected_project[col].find({})
            for item in cursor:
                remote.add_generic(item, col, local_project)

    def get_value(self, field, db, rxn, stoich, method, do_stoich=True, debug_level=1):
        command = [{
            "$match": {
                "name": db
            }
        }, {
            "$project": {
                "reactions": 1
            }
        }, {
            "$unwind": "$reactions"
        }, {
            "$match": {
                "reactions.name": rxn
            }
        }, {
            "$group": {
                "_id": {},
                "stoich": {
                    "$push": "$reactions.stoichiometry." + stoich
                }
            }
        }]
        records = list(self.project["databases"].aggregate(command))

        if (len(records) > 0):
            success = True
            molecules = records[0]["stoich"][0]
            res = []
            stoich_encoding = []

            for mol in molecules:
                stoich_encoding.append(molecules[mol])
                command = [{
                    "$match": {
                        "molecule_hash": mol,
                        "modelchem": method
                    }
                }, {
                    "$group": {
                        "_id": {},
                        "value": {
                            "$push": "$" + field
                        }
                    }
                }]
                page = list(self.project["pages"].aggregate(command))
                if (len(page) == 0 or len(page[0]["value"]) == 0):
                    success = False
                    break
                res.append(page[0]["value"][0])
                # debug.log(debug_level, 2, (stoich_encoding))

            if (success):
                if (do_stoich):
                    acc = 0
                    for i in range(0, len(stoich_encoding)):
                        acc += float(res[i] * stoich_encoding[i])
                    return acc
                return res

        # debug.log(debug_level, 2, ("Fallback attempt"))
        if (field == "return_value"):
            command = [{
                "$match": {
                    "name": db
                }
            }, {
                "$project": {
                    "reactions": 1
                }
            }, {
                "$unwind": "$reactions"
            }, {
                "$match": {
                    "reactions.name": rxn
                }
            }, {
                "$group": {
                    "_id": {},
                    "reaction_results": {
                        "$push": "$reactions.reaction_results." + stoich
                    }
                }
            }]
            page = list(self.project["databases"].aggregate(command))
            if (len(page) > 0 and method in page[0]["reaction_results"][0]):
                return page[0]["reaction_results"][0][method]
        return None

    def get_series(self, field, db, stoich, method, do_stoich=True, debug_level=1):
        database = self.project["databases"].find_one({"name": db})
        if (database == None):
            return None
        res = []
        index = []
        for item in database["reactions"]:
            res.append(
                self.get_value(field, db, item["name"], stoich, method, do_stoich, debug_level))
            index.append(item["name"])
        print("I am getting methods", method)
        return pd.DataFrame(data={method: res}, index=index)

    def get_dataframe(self, field, db, stoich, methods, do_stoich=True, debug_level=1):
        database = self.project["databases"].find_one({"name": db})
        if (database == None):
            return None

        names = []
        for item in database["reactions"]:
            names.append(item["name"])

        count = 0
        res = []

        for name in names:
            res.append([])
            for m in methods:
                val = self.get_value(field, db, name, stoich, m, do_stoich, debug_level)
                res[count].append(val)
            count += 1

        return pd.DataFrame(data=res, index=names, columns=methods)


    # Do a lookup on the pages collection using a <molecule, method> key.
    def get_page(self, molecule_hash, method):
        return self.project["pages"].find_one({"molecule_hash": molecule_hash, "modelchem": method})

    def get_database(self, name):
        return self.project["databases"].find_one({"name": name})

    def get_molecule(self, molecule_hash):
        return self.project["molecules"].find_one({"_id": molecule_hash})

    def json_query(self, json_data):
        """
        Wraps the MongoSocket in a JSON query.

        Parameters
        ----------
        json_data : dict
            Dictionary of data has function, args, and kwargs arguments

        Returns
        -------
        result : anytime
            Return the requested MongoSocket call.

        """

        keys = list(json_data)
        if "function" not in keys:
            raise KeyError("MongoSocket:json_query: 'funciton' are not found in keys")

        function = getattr(self, json_data["function"])

        if "args" in keys:
            args = json_data["args"]
        else:
            args = []

        if "kwargs" in keys:
            kwargs = json_data["kwargs"]
        else:
            kwargs = {}

        return function(*args, **kwargs)

    def mongod_query(self, *args, **kwargs):
        """
        Bad hack to make inserting a MongoSocket or Client transparent.
        Should revisit this!
        """

        json_data = {}
        json_data["function"] = args[0]
        json_data["args"] = args[1:]
        json_data["kwargs"] = kwargs

        return self.json_query(json_data)
