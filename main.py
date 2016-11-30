from MongoQCDB import MongoQCDB
from collections import OrderedDict
import hashlib
import json
import os

mongo = MongoQCDB("localhost", 27017, "local")
print(mongo.setup)

m = hashlib.sha1()
colls = ["molecules", "databases", "pages"]

# Add all JSON
for col in colls:
    fn = os.path.dirname(os.path.abspath(__file__)) + "/sample-json/" + col + "/"
    for filename in os.listdir(fn):
        json_data = open(fn + filename).read()
        # Load JSON from file into OrderedDict
        data = json.loads(json_data, object_pairs_hook=OrderedDict)
        # Load structured string into hashlib
        m.update(json.dumps(data).encode("utf-8"))
        digest = m.hexdigest()
        inserted = mongo.add_json(col, data, digest)
        print("Added " + digest + " to " + col + ". Success=" + str(inserted) + ".")
