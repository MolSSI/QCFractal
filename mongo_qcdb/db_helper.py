import pymongo
import pandas as pd
import hashlib
import json

class db_helper:

    # Constructor
    def __init__(self, url, port, db):
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(url, port)
        self.setup = self.init_db(db)

    # Adds a molecule to the DB. Returns True on success.
    def add_molecule(self, data):
        hash_fields = ["symbols", "masses", "name", "charge", "multiplicity", "ghost", "geometry", "fragments",
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
        hash_fields = ["molecule", "method"]
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
    def get_data_value(self, db, rxn, stoich, method):
        database = self.db["databases"].find_one({"name": db})

        if (database == None):
            print("Invalid database")
            return None

        reaction = None
        for item in database["reactions"]:
            if (item["name"] == rxn and reaction == None):
                reaction = item
            elif (item["name"] == rxn and reaction != None):
                print("Reaction is ambiguous (more than one reaction has this name).")
                return None

        if (reaction == None):
            print("Specified reaction " + rxn + " does not exist.")
            return None

        # Go through each molecule in the stoich dictionary
        if (not stoich in reaction["stoichiometry"]):
            print("Unknown stoichiometry " + stoich + ".")
            return None;

        stoich_dict = reaction["stoichiometry"][stoich]
        valid = True
        sum = 0
        for entry in stoich_dict:
            page = self.get_page(entry, method)
            if (page == None):
                valid = False
                break
            sum += int(stoich_dict[entry]) * page["value"][0]
        if (valid):
            return sum

        # Fallback
        rxn_dict = reaction["reaction_results"][stoich]
        if (method in rxn_dict):
            return rxn_dict[method]
        else:
            return None

    def get_data_series(self, db, stoich, method):
        database = self.db["databases"].find_one({"name": db})
        if (database == None):
            print("Invalid database")
            return None
        res = []
        index = []
        for item in database["reactions"]:
            res.append(self.get_data_value(db, item["name"], stoich, method))
            index.append(item["name"])
        return pd.DataFrame(data=res, index=index, columns=[method])

    '''
        def get_data_frame(self, db, rxn, stoich):
            database = self.db["databases"].find_one({"name": db})

            if (database == None):
                print("Invalid database")
                return None

            reaction = None
            for item in database["reactions"]:
                if (item["name"] == rxn and reaction == None):
                    reaction = item
                elif (item["name"] == rxn and reaction != None):
                    print("Reaction is ambiguous (more than one reaction has this name).")
                    return None

            if (reaction == None):
                print("Specified reaction " + rxn + " does not exist.")
                return None
    '''


    # Do a lookup on the pages collection using a <molecule, method> key.
    def get_page(self, molecule, method):
        return self.db["pages"].find_one({"molecule": molecule, "method": method})

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
