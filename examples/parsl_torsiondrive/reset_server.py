import fireworks
import pymongo

# Reset database
client = pymongo.MongoClient("mongodb://localhost")
client.drop_database("qca_parsl_testing")
