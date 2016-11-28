import pymongo

class MongoQCDB:

    def __init__(self, url, port, db):
        self.url = url
        self.port = port
        client = pymongo.MongoClient(url, port)
        self.db = client[db]

    def find_one(self, coll):
        return self.db[coll].find_one()

    def make_collections(self):
        # Success dictionary and collections to create
        success ={}
        collection_creation = {}
        collections = {"molecules", "databases", "pages"}

        # Try to create a collection for each entry
        for stri in collections:
            try:
                self.db.create_collection(stri)
                collection_creation[stri] = 1
            except pymongo.errors.CollectionInvalid:
                collection_creation[stri] = 0
        success["collection_creation"] = collection_creation

        # Create a unique index on the "symbol" field for the "molecules" collection
        success["create_index"] = self.db["molecules"].create_index("symbol", unique=True, background=True)

        # Return the success array, where a value of 1 means successful
        return success
