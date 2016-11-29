import pymongo

class MongoQCDB:

    # Constructor
    def __init__(self, url, port, db):
        self.url = url
        self.port = port
        self.client = pymongo.MongoClient(url, port)
        self.setup = self.init_db(db)

    # Adds a molecule to the DB. Returns True on success, False on failure.
    def add_molecule(self, symbol, name, geometry):
        try:
            self.db["molecules"].insert_one(
                {
                    "symbol" : symbol,
                    "name" : name,
                    "geometry" : geometry
                    }
                )
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    # Adds a database to the DB. Returns True on success.
    def add_database(self, name, reactions, citation, link):
        try:
            self.db["databases"].insert_one(
                {
                    "name" : name,
                    "reactions" : reactions,
                    "citation" : citation,
                    "link" : link
                }
            )
            return True
        except pymongo.errors.DuplicateKeyError:
            return False

    # Adds a page to the DB. Returns True on success.
    def add_page(self, molecule, method, value1, value2, value3, citation, link):
        try:
            self.db["pages"].insert_one(
                {
                    "molecule" : molecule,
                    "method" : method,
                    "value1" : value1,
                    "value2" : value2,
                    "value3" : value3,
                    "citation" : citation,
                    "link" : link
                }
            )
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

        # Create a unique index on the "symbol" field for the "molecules" collection
        success["molecules_index"] = self.db["molecules"].create_index("symbol", unique=True, background=True)
        # Create a unique index on the "name" field for the "datbases" collection
        success["databases_index"] = self.db["databases"].create_index("name", unique=True, background=True)
        # Create a compound unique index on the "molecule"-"method" field for the "pages" collection
        success["pages_index"] = self.db["pages"].create_index([("molecule", 1), ("method", 1)],
                                                                unique=True, background=True)

        # Return the success array, where a value of 1 means successful
        return success
