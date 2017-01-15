from mongo_qcdb import uploader
from collections import OrderedDict
import sys
import json
import os

uploader = uploader.uploader
mongo = uploader("localhost", 27017, "local")
print(mongo.setup)

collections = ["molecules", "databases", "pages"]

# Define the descriptor field for each collection. Used for logging.
descriptor = {"molecules": "name", "databases": "name", "pages": "method"}

# Add all JSON
for col in collections:
    fn = os.path.dirname(os.path.abspath(__file__)) + "/" + sys.argv[1] + "/" + col + "/"
    for filename in os.listdir(fn):
        json_data = open(fn + filename).read()
        # Load JSON from file into OrderedDict
        data = json.loads(json_data, object_pairs_hook=OrderedDict)
        if (col == "molecules"):
                inserted = mongo.add_molecule(data)
        if (col == "databases"):
                inserted = mongo.add_database(data)
        if (col == "pages"):
                inserted = mongo.add_page(data)
        print("[" + col + "] Added " + data[descriptor[col]] + " to " + col + ". Success=" + str(inserted) + ".")
