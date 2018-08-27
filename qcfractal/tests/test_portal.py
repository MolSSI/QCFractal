"""
Tests the interface portal adapter to the REST API
"""

import qcfractal.interface as portal
from qcfractal.testing import test_server

# All tests should import test_server, but not use it
# Make PyTest aware that this module needs the server


def test_molecule_portal(test_server):

    client = portal.FractalClient(test_server.get_address(""))

    water = portal.data.get_molecule("water_dimer_minima.psimol")

    # Test add
    ret = client.add_molecules({"water": water})

    # Test get
    get_mol = client.get_molecules(ret["water"], index="id")

    assert water.compare(get_mol[0])


def test_options_portal(test_server):

    client = portal.FractalClient(test_server.get_address(""))

    opts = portal.data.get_options("psi_default")

    # Test add
    ret = client.add_options(opts)

    # Test get
    get_opt = client.get_options([(opts["program"], opts["name"])])

    assert opts == get_opt[0]


def test_database_portal(test_server):

    db = {"category": "OpenFF", "name": "Torsion123", "something": "else", "array": ["54321"]}

    client = portal.FractalClient(test_server.get_address(""))

    # Test add
    ret = client.add_database(db)

    # Test get
    get_db = client.get_databases([(db["category"], db["name"])])
    del get_db[0]["id"]

    assert db == get_db[0]
