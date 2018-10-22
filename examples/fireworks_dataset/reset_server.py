import fireworks
import pymongo

# Reset fireworks queue
lpad = fireworks.LaunchPad.from_file("fw_lpad.yaml")
lpad.reset(None, require_password=False)

# Reset database
client = pymongo.MongoClient("mongodb://localhost")
client.drop_database("qca_fw_testing")
