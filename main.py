from MongoQCDB import MongoQCDB
import json
import os

mongo = MongoQCDB("localhost", 27017, "local")
print(mongo.setup)

# Add all JSON molecules
fn = os.path.dirname(os.path.abspath(__file__)) + "/sample-json/molecules/"
for filename in os.listdir(fn):
    json_data = open(fn + filename).read()
    data = json.loads(json_data)
    inserted = mongo.add_molecule(data["symbol"], data["name"], data["geometry"])
    print("Added molecule: " + data["symbol"] + ". Success=" + str(inserted))

# Add all JSON databases
fn = os.path.dirname(os.path.abspath(__file__)) + "/sample-json/databases/"
for filename in os.listdir(fn):
    json_data = open(fn + filename).read()
    data = json.loads(json_data)
    inserted = mongo.add_database(data["name"], data["reactions"], data["citation"], data["link"])
    print("Added database: " + data["name"] + ". Success=" + str(inserted))

# Add all JSON pages
fn = os.path.dirname(os.path.abspath(__file__)) + "/sample-json/pages/"
for filename in os.listdir(fn):
    json_data = open(fn + filename).read()
    data = json.loads(json_data)
    inserted = mongo.add_page(data["molecule"], data["method"], data["value_1"],
                       data["value_2"], data["value_3"], data["citation"],
                       data["link"])
    print("Added page: " + data["molecule"] + "-" + data["method"] + ". Success=" + str(inserted))
