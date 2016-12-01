from MongoQCDB import MongoQCDB
from collections import OrderedDict
import hashlib
import json
import os

mongo = MongoQCDB("localhost", 27017, "local")
print(mongo.setup)

collections = ["molecules", "databases", "pages"]

# For each collection, how to hash its JSON.
hash_fields = {}
hash_fields["molecules"] = ["symbols", "masses", "name", "charge", "multiplicity", "ghost", "geometry", "fragments"]
hash_fields["databases"] = ["name"]
hash_fields["pages"] = ["molecule", "method"]

# Define the descriptor field for each collection. Used for logging.
descriptor = {"molecules": "name", "databases": "name", "pages": "method"}

# Add all JSON
for col in collections:
    fn = os.path.dirname(os.path.abspath(__file__)) + "/sample-json/" + col + "/"
    for filename in os.listdir(fn):
        # Make hashlib
        m = hashlib.sha1()
        json_data = open(fn + filename).read()
        # Load JSON from file into OrderedDict
        data = json.loads(json_data, object_pairs_hook=OrderedDict)

        # Update hashlib object based on hased fields
        concat = ""
        for field in hash_fields[col]:
            concat += json.dumps(data[field])
        m.update(concat.encode("utf-8"))

        # Calculate SHA1 with hashlib
        digest = m.hexdigest()
        inserted = mongo.add_json(col, data, digest)
        print("[" + col + "] Added " + data[descriptor[col]] + " to " + col + " at _id " + digest + ". Success=" + str(inserted) + ".")
