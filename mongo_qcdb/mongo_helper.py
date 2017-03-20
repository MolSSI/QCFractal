"""
Database connection class which directly calls the PyMongo API to capture
cammon subroutines.
"""

import pymongo
import pandas as pd
import hashlib
import json
import numpy as np
from . import fields


class MongoSocket(object):
    """
    This is a Mongo QCDB socket class.
    """

    def __init__(self, url, port, project=None):
        """
        Constructs a new socket where url and port points towards a Mongod instance.

        """
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(url, port)
        if (project != None):
            self.set_project(project)

    def __repr__(self):
        return "<MongoSocket: address='%s:%d'>" % (self.url, self.port)

    def set_project(self, project):
        # Success dictionary and collections to create
        success = {}
        collection_creation = {}

        # Create DB
        self.project = self.client[project]
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

    def add_molecule(self, data):
        """
        Adds a molecule to the database.

        Parameters
        ----------
        data : dict
            Structured instance of the molecule.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.add_generic(data, "molecules")

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
        return self.add_generic(data, "databases")

    def add_page(self, data):
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
        return self.add_generic(data, "pages")

    def add_generic(self, data, collection):
        """
        Helper function that facilitates adding a record.
        """
        sha1 = fields.get_hash(data, collection)
        try:
            data["_id"] = sha1
            self.project[collection].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    def del_by_hash(self, collection, hashes):
        """
        Helper function that facilitates deletion based on hash.
        """
        if isinstance(hashes, str):
            return (self.project[collection].delete_one({"_id": hashes})).deleted_count == 1
        elif isinstance(hashes, list):
            ret = (self.project[collection].delete_many({"_id": {"$in" : hashes}})).deleted_count
            return ret


    def del_by_data(self, collection, data):
        """
        Helper function that facilitates deletion based on structured dict.
        """
        if isinstance(data, dict):
            return self.del_by_hash(collection, fields.get_hash(data, collection))
        elif isinstance(data, list):
            arr = []
            for item in data:
                arr.append(fields.get_hash(item, collection))
            return self.del_by_hash(collection, arr)

    def del_molecule_by_data(self, data):
        """
        Removes a molecule from the database from its raw data.

        Parameters
        ----------
        data : dict
            Structured instance of the molecule.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.del_by_data("molecules", data)

    def del_molecule_by_hash(self, hash_val):
        """
        Removes a molecule from the database from its hash.

        Parameters
        ----------
        hash_val : str
            The hash of a molecule.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.del_by_hash("molecules", hash_val)

    def del_database_by_data(self, data):
        """
        Removes a database from the database from its raw data.

        Parameters
        ----------
        data : dict
            Structured instance of the database.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.del_by_data("databases", data)

    def del_database_by_hash(self, hash_val):
        """
        Removes a database from the database from its hash.

        Parameters
        ----------
        hash_val : str
            The hash of a database.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.del_by_hash("databases", hash_val)

    def del_page_by_data(self, data):
        """
        Removes a page from the database from its raw data.

        Parameters
        ----------
        data : dict
            Structured instance of the page.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.del_by_data("pages", data)

    def del_page_by_hash(self, hash_val):
        """
        Removes a page from the database from its hash.

        Parameters
        ----------
        hash_val : str
            The hash of a page.

        Returns
        -------
        bool
            Whether the operation was successful.
        """
        return self.del_by_hash("pages", hash_val)

    def evaluate(self, hashes, methods, field="return_value"):
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
        hashes = list(hashes)
        methods = list(methods)
        command = [{"$match": {"molecule_hash": {"$in": hashes}, "modelchem": {"$in": methods}}}]
        pages = list(self.project["pages"].aggregate(command))
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
        return pd.DataFrame(data=d, index=[methods]).transpose()

    def evaluate_2(self, hashes, fields, method):
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
        hashes = list(hashes)
        command = [{"$match": {"molecule_hash": {"$in": hashes}, "modelchem": method}}]
        pages = list(self.project["pages"].aggregate(command))
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

    def list_methods(self, hashes):
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
        d = {}
        for mol in hashes:
            records = list(self.project["pages"].find({"molecule_hash": mol}))
            d[mol] = []
            for rec in records:
                d[mol].append(rec["modelchem"])

        df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in d.items()])).transpose()
        return df

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
            pages = list(self.project["pages"].aggregate(command))
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

    def push_to(self, url, port, remote_project):
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
        self.generic_copy(url, port, remote_project, False)

    def clone_to(self, url, port, remote_project):
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
        self.generic_copy(url, port, remote_project, True)

    def generic_copy(self, url, port, remote_project, delete):
        """
            Helper function for facilitating syncing.
        """
        remote = MongoSocket(url, port)
        remote.set_project(remote_project)
        if (delete):
            remote.client.drop_database(remote.project)
        for col in ["molecules", "databases", "pages"]:
            cursor = self.project[col].find({})
            for item in cursor:
                remote.add_generic(item, col)

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
