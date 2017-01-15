import pymongo

class MongoQCDB:

    # Constructor
    def __init__(self, url, port, db):
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(url, port)
        self.setup = self.init_db(db)

    # Adds a document to the DB. Returns True on success.
    def add_json(self, collection, json, sha1):
        try:
            json["_id"] = sha1;
            self.db[collection].insert_one(json)
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
