import pymongo

class MongoQCDB:

    def __init__(self, url, port, db):
        self.url = url
        self.port = port
        client = pymongo.MongoClient(url, port)
        self.db = client[db]

    def find_one(self, coll):
        return self.db[coll].find_one()
