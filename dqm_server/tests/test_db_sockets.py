"""
Tests the database wrappers
"""

import pytest
import numpy as np

# Import the DQM collection
import dqm_server as dserver
import dqm_server.interface as dclient


@pytest.fixture(scope="module", params=["mongo"])
def db_socket(request):
    print("")
    db_name = "dqm_local_values_test"

    # IP/port/drop table is specific to build
    if request.param == "mongo":
        db = dserver.db_socket_factory("127.0.0.1", 27017, db_name, db_type=request.param)

        # Clean and re-init the databse
        db.client.drop_database(db._project_name)
        db.init_database()
    else:
        raise KeyError("DB type %s not understood" % request.param)

    yield db

    if request.param == "mongo":
        db.client.drop_database(db_name)
    else:
        raise KeyError("DB type %s not understood" % request.param)


def test_molecule_add(db_socket):

    water = dclient.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = db_socket.add_molecules(water.to_json())
    assert ret["nInserted"] == 1

    # Try duplicate adds
    ret = db_socket.add_molecules(water.to_json())
    assert ret["nInserted"] == 0
    assert ret["errors"][0] == (water.get_hash(), 11000)

    # Pull molecule from the DB for tests
    db_json = db_socket.get_molecules(water.get_hash(), index="hash")[0]
    water_db = dclient.Molecule.from_json(db_json)
    water_db.compare(water)

    # Cleanup adds
    ret = db_socket.del_molecules(water.get_hash(), index="hash")
    assert ret == 1



# def test_molecule_add_many(db_socket):
#     water = dclient.data.get_molecule("water_dimer_minima.psimol")
#     water2 = dclient.data.get_molecule("water_dimer_stretch.psimol")

#     ret = db_socket.add_molecules([water.to_json(), water2.to_json()])
#     assert ret["nInserted"] == 2

#     ret = db_socket.get_molecules([water.get_hash(), water2.get_hash(), "something"])
#     assert len(list(ret)) == 2

#     # Cleanup adds
#     ret = db_socket.del_molecule_by_hash([water.get_hash(), water2.get_hash()])
#     assert ret == 2

def test_molecule_get(db_socket):

    water = dclient.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    ret = db_socket.add_molecules(water.to_json())
    assert ret["nInserted"] == 1
    water_id = ret["ids"][0]

    # Pull molecule from the DB for tests
    db_json = db_socket.get_molecules(water_id, index="id")[0]
    water_db = dclient.Molecule.from_json(db_json)
    water_db.compare(water)

    # Cleanup adds
    ret = db_socket.del_molecules(water_id, index="id")
    assert ret == 1


def test_options_add(db_socket):

    opts = dclient.data.get_options("psi_default")

    ret = db_socket.add_options(opts)
    assert ret["nInserted"] == 1

    ret = db_socket.add_options(opts)
    assert ret["nInserted"] == 0

    assert opts == db_socket.get_option(opts["name"], opts["program"])


def test_options_error(db_socket):
    opts = dclient.data.get_options("psi_default")

    del opts["name"]
    ret = db_socket.add_options(opts)
    assert ret["nInserted"] == 0
    assert len(ret["validation_errors"]) == 1
