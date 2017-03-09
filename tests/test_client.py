import mongo_qcdb as mdb
import pandas as pd
import numpy as np
import os
import time
import contextlib
import subprocess

qcdb_test_name = "QCDB_TEST_DB"

@contextlib.contextmanager
def client_service():
    
    server_path = os.path.dirname(__file__) + "/../qcdb_server/server.py"
    run_string = "python " + server_path + " --mongo_project=" + qcdb_test_name

    # Boot up the process and give it a second
    p = subprocess.Popen(run_string, shell=True)
    time.sleep(2.0)

    # Kill on exit
    yield
    p.terminate()


def test_client1():
    with client_service():
        client = mdb.Client("http://localhost:8888")

        mongo = client.get_MongoSocket()
        mongo.client.drop_database(qcdb_test_name)
        mongo.del_database_by_data({"name": "H2"})
        
        # Add a new blank test set and submit
        db = mdb.Database("H2", client)
        db.add_rxn("He 2 - 5", [("""He 0 0 5\n--\nHe 0 0 -5""", 1.0), ("He 0 0 0", -2.0)], return_values={"Benchmark": -1.0})
        db.add_rxn("He 4 - 5", [("""He 0 0 5\n--\nHe 0 0 -5\n--\nHe 0 4 0\n--\nHe 3 0 0""", 1.0), ("""He 0 0 0""", -4.0)], return_values={"Benchmark": -3.0})
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

