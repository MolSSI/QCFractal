import mongo_qcdb as mdb
import pandas as pd
import numpy as np
import os
import time
import subprocess
import pytest
import shlex

@pytest.fixture(scope="module")
def client_service():

    server_path = os.path.dirname(__file__) + "/../qcdb_server/server.py"
    run_string = "python \"" + server_path + "\""

    # Boot up the process and give it a second
    p = subprocess.Popen(shlex.split(run_string), shell=False)
    time.sleep(2.0)

    # Kill on exit
    yield
    print("Exiting...")
    p.terminate()


def test_client1(client_service):
    client = mdb.Client("http://localhost:8888", "client1_project")

    # Clear out previous mongo
    mongo = client.get_MongoSocket()
    mongo.client.drop_database("client1_project")
    mongo.del_database_by_data({"name": "H2"})

    # Add a new blank test set and submit
    db = mdb.Database("H2", client)
    db.add_rxn(
        "He 2 - 5", [("""He 0 0 5\n--\nHe 0 0 -5""", 1.0), ("He 0 0 0", -2.0)],
        reaction_results={"Benchmark": -1.0})
    db.add_rxn(
        "He 4 - 5",
        [("""He 0 0 5\n--\nHe 0 0 -5\n--\nHe 0 4 0\n--\nHe 3 0 0""", 1.0), ("""He 0 0 0""", -4.0)],
        reaction_results={"Benchmark": -3.0})
    db.save()

    # Re initialize the DB from JSON
    db = mdb.Database("H2", client)
    assert db.data["name"] == "H2"
    assert len(db.data["reactions"]) == 2

    # Compute a hypothetical set
    ret = db.compute("BP/aug-cc-pVDZ", other_fields={"return_value": 1}, program="pass")
    assert ret["submit"]["success"] == True
    assert ret["nsubmit"] == 3

    time.sleep(2.0)

    # Ensure everything has flushed through
    queue = client.get_queue()
    assert len(queue["error"]) == 0
    assert len(queue["queue"]) == 0

    # Check a few queries
    db.query("BP/aug-cc-pVDZ")
    db.query("Benchmark", reaction_results=True)
    assert np.allclose(db.df["Benchmark"], db.df["BP/aug-cc-pVDZ"])


def test_client_ie(client_service):

    client = mdb.Client("http://localhost:8888", "client2_project")

    # Clear out previous mongo
    mongo = client.get_MongoSocket()
    mongo.client.drop_database("client2_project")
    mongo.del_database_by_data({"name": "H2_IE"})

    db = mdb.Database("H2_IE", client, db_type="IE")
    db.add_ie_rxn("he 2 - 5", """he 0 0 5\n--\nhe 0 0 -5""", reaction_results={"Benchmark": -1.0})
    db.add_ie_rxn(
        "CHNO",
        """C 0 0 5\n--\nH 0 0 -5\n--\nN 0 4 0\n--\nO 3 0 0""",
        reaction_results={"Benchmark": -3.0})
    db.save()

    # Re initialize the DB from JSON
    db = mdb.Database("H2_IE", client)
    assert db.data["name"] == "H2_IE"
    assert len(db.data["reactions"]) == 2
    assert db.data["db_type"] == "IE"

    # Compute a hypothetical set
    ret = db.compute("BP/aug-cc-pVDZ", other_fields={"return_value": 1}, program="pass")
    assert ret["submit"]["success"] == True
    assert ret["nsubmit"] == 7

    # Wait for this to pass through
    time.sleep(2.0)

    # Compute a cp results, complexes should already be computed!
    ret = db.compute(
        "BP/aug-cc-pVDZ", stoich='cp', other_fields={"return_value": 1}, program="pass")
    assert ret["submit"]["success"] == True
    assert ret["nsubmit"] == 5

    time.sleep(2.0)

    # Ensure everything has flushed through
    queue = client.get_queue()
    assert len(queue["error"]) == 0
    assert len(queue["queue"]) == 0

    # Check a few queries
    db.query("BP/aug-cc-pVDZ")
    db.query("BP/aug-cc-pVDZ", stoich="cp", prefix="cp-")
    db.query("Benchmark", reaction_results=True)
    assert np.allclose(db["Benchmark"], db["BP/aug-cc-pVDZ"])
    assert np.allclose(db["Benchmark"], db["cp-BP/aug-cc-pVDZ"])
