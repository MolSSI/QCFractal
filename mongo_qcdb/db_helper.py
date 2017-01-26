import pymongo
import pandas as pd
import hashlib
import json
import debug as debug

class db_helper:

    # Constructor
    def __init__(self, url, port, db):
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(url, port)
        self.setup = self.init_db(db)


    # Adds a molecule to the DB. Returns True on success.
    def add_molecule(self, data):
        hash_fields = ["symbols", "masses", "name", "charge", "multiplicity", "real", "geometry", "fragments",
                            "fragment_charges", "fragment_multiplicities"]
        m = hashlib.sha1()
        concat = ""
        for field in hash_fields:
            concat += json.dumps(data[field])
        m.update(concat.encode("utf-8"))
        sha1 = m.hexdigest()
        try:
            data["_id"] = sha1;
            self.db["molecules"].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False


    # Adds a database to the DB. Returns True on success.
    def add_database(self, data):
        hash_fields = ["name"]
        m = hashlib.sha1()
        concat = ""
        for field in hash_fields:
            concat += json.dumps(data[field])
        m.update(concat.encode("utf-8"))
        sha1 = m.hexdigest()
        try:
            data["_id"] = sha1;
            self.db["databases"].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False


    # Adds a page to the DB. Returns True on success.
    def add_page(self, data):
        hash_fields = ["molecule_hash", "modelchem"]
        m = hashlib.sha1()
        concat = ""
        for field in hash_fields:
            concat += json.dumps(data[field])
        m.update(concat.encode("utf-8"))
        sha1 = m.hexdigest()
        try:
            data["_id"] = sha1;
            self.db["pages"].insert_one(data)
            return True
        except pymongo.errors.DuplicateKeyError:
            return False


    # Returns a single data value from database
    # Field: The name of the field in Page that you retrieve
    # DB: The name of the DB you want to query
    # rxn: The name of the reacition to query
    # Method: Method to use to query a Page
    # do_stoich: Flag whether or not to sum based on stoich
    # debug_level: 0-2 verbosity
    def get_value(self, field, db, rxn, stoich, method, do_stoich=True, debug_level=1):
        debug.log(debug_level, 2, "Running get_data for db=" + db + " rxn=" + rxn
        + " stoich=" + stoich + " method=" + method)
        database = self.db["databases"].find_one({"name": db})

        if (database == None):
            debug.log(debug_level, 1, "Invalid database")
            return None

        reaction = None
        for item in database["reactions"]:
            if (item["name"] == rxn and reaction == None):
                reaction = item
            elif (item["name"] == rxn and reaction != None):
                debug.log(debug_level, 1, "Reaction is ambiguous (more than one reaction has this name).")
                return None

        if (reaction == None):
            debug.log(debug_level, 1, "Specified reaction " + rxn + " does not exist.")
            return None

        # Go through each molecule in the stoich dictionary
        if (not stoich in reaction["stoichiometry"]):
            debug.log(debug_level, 1, "Unknown stoichiometry " + stoich + ".")
            return None;

        stoich_dict = reaction["stoichiometry"][stoich]
        valid = True
        sum = 0
        for entry in stoich_dict:
            page = self.get_page(entry, method)
            if (page == None or not page["success"]):
                valid = False
                break
            sum += int(stoich_dict[entry]) * page["return_value"]
        if (valid):
            return sum

        # Fallback
        rxn_dict = reaction["reaction_results"][stoich]
        if (method in rxn_dict):
            return rxn_dict[method]
        else:
            return None


    def get_series(self, field, db, stoich, method, do_stoich=True, debug_level=1):
        debug.log(debug_level, 2, "Running get_series for db=" + db + " stoich="
         + stoich + " method=" + method)
        database = self.db["databases"].find_one({"name": db})
        if (database == None):
            debug.log(debug_level, 1, "Invalid database")
            return None
        res = []
        index = []
        for item in database["reactions"]:
            res.append(self.get_value(field, db, item["name"], stoich, method,
            do_stoich, debug_level))
            index.append(item["name"])
        return pd.DataFrame(data=res, index=index, columns=[method])


    def get_dataframe(self, field, db, rxn, stoich, methods, do_stoich=True, debug_level=1):
        debug.log(debug_level, 2, "Running get_dataframe for db=" + db + " rxn="
        + rxn + " stoich=" + stoich + " methods=" + str(methods))
        database = self.db["databases"].find_one({"name": db})
        if (database == None):
            debug.log(debug_level, 1, "Invalid database.")
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
