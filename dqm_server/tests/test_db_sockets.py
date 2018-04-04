"""
Tests the database wrappers
"""

import pytest
import numpy as np

# Import the DQM collection
import dqm_server as dserver
import dqm_client as dclient


@pytest.fixture(scope="module", params=["mongo"])
def db_socket(request):
    db_name = "dqm_local_values_test"
    db = dserver.db_socket_factory("127.0.0.1", 27017, db_name, db_type=request.param)

    # IP/port/drop table is specific to build
    if request.param == "mongo":
        if db_name in db.client.database_names():
            db.client.drop_database(db_name)
    else:
        raise KeyError("DB type %s not understood" % request.param)

    return db


def test_molecule_add(db_socket):
    
    water = dclient.data.get_molecule("water_dimer_minima.psimol")
    water2 = dclient.data.get_molecule("water_dimer_stretch.psimol")
    
    ret = db_socket.add_molecules(water.to_json()) 
    assert ret["nInserted"] == 1
    ret = db_socket.add_molecules(water.to_json()) 
    assert ret["nInserted"] == 0
    assert ret["errors"][0] == (water.get_hash(), 11000)
