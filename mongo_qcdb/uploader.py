import pymongo
import hashlib
import json

class uploader:

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
