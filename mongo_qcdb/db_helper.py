import pymongo
import pandas as pd
import hashlib
import json
# import debug as debug
import numpy as np

from . import fields

class MongoSocket(object):

    # Constructor
    def __init__(self, url, port, db):
        self.db_name = db
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(url, port)
        self.setup = self.init_db(db)


    # Adds a molecule to the DB. Returns True on success.
    def add_molecule(self, data):
        sha1 = fields.get_hash(data, "molecule")
        try:
            data["_id"] = sha1;
            self.db["molecules"].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    # Given the hash ID of a molecule, delete it. Return true on success,
    # otherwise false.
    def del_molecule(self, hash_val):
        return (self.db["molecules"].delete_one({"_id" : hash_val})).deleted_count == 1

    # Adds a database to the DB. Returns True on success.
    def add_database(self, data):
        sha1 = fields.get_hash(data, "database")
        try:
            data["_id"] = sha1;
            self.db["databases"].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    def del_database(self, name):
        hash_bal = fields.get_hash(name, "database")
        return (self.db["databases"].delete_one({"_id" : hash_val})).deleted_count == 1

    # Adds a page to the DB. Returns True on success.
    def add_page(self, data):
        sha1 = fields.get_hash(data, "page")
        try:
            data["_id"] = sha1;
            self.db["pages"].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    # Given the hash ID of a page, delete it. Return true on success, otherwise
    # false.
    def del_page(self, hash_val):
        return (self.db["pages"].delete_one({"_id" : hash_val})).deleted_count == 1

    # Given mol hashes, methods, and a field, populate a mol by method matrix
    # with respective fields
    def evaluate(self, hashes, methods, field="return_value"):
        d = {}
        for mol in hashes:
            d[mol] = []
            for method in methods:
                command = [
                {"$match" : {"molecule_hash":mol, "modelchem":method}},
                {"$group" : {
                    "_id" : {}, "value" : {"$push" : "$" + field}
                }}
                ]
                pages = list(self.db["pages"].aggregate(command))
                if (len(pages) == 0 or len(pages[0]["value"]) == 0):
                    d[mol].append(None)
                else:
                    d[mol].append(pages[0]["value"][0])
        return pd.DataFrame(data=d, index=[methods]).transpose()

    # Given mol hashes, fields, and a method, populate a mol by field matrix
    # with the respective field values for that method
    def evaluate_2(self, hashes, fields, method):
        d = {}
        for mol in hashes:
            d[mol] = []
            for field in fields:
                command = [
                {"$match" : {"molecule_hash":mol, "modelchem":method}},
                {"$group" : {
                    "_id" : {}, "value" : {"$push" : "$" + field}
                }}
                ]
                pages = list(self.db["pages"].aggregate(command))
                if (len(pages) == 0 or len(pages[0]["value"]) == 0):
                    d[mol].append(None)
                else:
                    d[mol].append(pages[0]["value"][0])
        return pd.DataFrame(data=d, index=[fields]).transpose()


    # Displays all available model chems for the provided list of molecule hashes.
    def list_methods(self, hashes):
        d = {}
        for mol in hashes:
            records = list(self.db["pages"].find({"molecule_hash" : mol}))
            d[mol] = []
            for rec in records:
                d[mol].append(rec["modelchem"])

        df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in d.items() ])).transpose()
        return df

    # Returns series containing the first match of field in a page for all hashes.
    def search_qc_variable(self, hashes, field):
        d = {}
        for mol in hashes:
            command = [
            {"$match" : {"molecule_hash": mol}},
            {"$group" : {
                "_id" : {}, "value" : {"$push" : "$" + field}
            }}
            ]
            pages = list(self.db["pages"].aggregate(command))
            if (len(pages) == 0 or len(pages[0]["value"]) == 0):
                d[mol] = None
            else:
                d[mol] = pages[0]["value"][0]
        return pd.DataFrame(data=d, index=[field]).transpose()

    def get_value(self, field, db, rxn, stoich, method, do_stoich=True, debug_level=1):
        command = [
        { "$match": { "name" : db } },
        { "$project": { "reactions" : 1 } },
        { "$unwind": "$reactions" },
        { "$match": { "reactions.name" : rxn } },
        { "$group": {
            "_id" : {}, "stoich" : {"$push": "$reactions.stoichiometry." + stoich}
        }}
        ]
        records = list(self.db["databases"].aggregate(command))

        if (len(records) > 0):
            success = True
            molecules = records[0]["stoich"][0]
            res = []
            stoich_encoding = []

            for mol in molecules:
                stoich_encoding.append(molecules[mol])
                command = [
                {"$match" : {"molecule_hash": mol, "modelchem": method}},
                {"$group" : {
                    "_id" : {}, "value" : {"$push" : "$" + field}
                }}
                ]
                page = list(self.db["pages"].aggregate(command))
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
            command = [
            { "$match": { "name" : db } },
            { "$project": { "reactions" : 1 } },
            { "$unwind": "$reactions" },
            { "$match": { "reactions.name" : rxn } },
            { "$group": {
                "_id" : {}, "reaction_results" : {"$push": "$reactions.reaction_results." + stoich}
            }}
            ]
            page = list(self.db["databases"].aggregate(command))
            if (len(page) > 0 and method in page[0]["reaction_results"][0]):
                return page[0]["reaction_results"][0][method]
        return None


    def get_series(self, field, db, stoich, method, do_stoich=True, debug_level=1):
        database = self.db["databases"].find_one({"name": db})
        if (database == None):
            return None
        res = []
        index = []
        for item in database["reactions"]:
            res.append(self.get_value(field, db, item["name"], stoich, method,
            do_stoich, debug_level))
            index.append(item["name"])
        return pd.DataFrame(data={method:res}, index=index)


    def get_dataframe(self, field, db, stoich, methods, do_stoich=True, debug_level=1):
        database = self.db["databases"].find_one({"name": db})
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
    def get_page(self, molecule, method):
        return self.db["pages"].find_one({"molecule_hash": molecule, "modelchem": method})

    def get_database(self, name):
        return self.db["databases"].find_one({"name": name})

    def get_molecule(self, molecule_hash):
        return self.db["molecules"].find_one({"_id" : molecule_hash})


    def init_db(self, db):
        # Success dictionary and collections to create
        success = {}
        collection_creation = {}

        # Create DB
        self.db = self.client[db]
        success["db"] = self.db

        # Try to create a collection for each entry
        collections = {"molecules", "databases", "pages"}
        for stri in collections:
            try:
                self.db.create_collection(stri)
                collection_creation[stri] = 1
            except pymongo.errors.CollectionInvalid:
                collection_creation[stri] = 0
        success["collection_creation"] = collection_creation

        # Return the success array, where a value of 1 means successful
        return success
