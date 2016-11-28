from MongoQCDB import MongoQCDB

mongo = MongoQCDB("localhost", 27017, "local")

print(mongo.find_one("temp"))
