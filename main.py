from MongoQCDB import MongoQCDB

mongo = MongoQCDB("localhost", 27017, "local")

print(mongo.make_collections())
