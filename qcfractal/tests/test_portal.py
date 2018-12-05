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

    # Test molecular_formula get
    get_mol = client.get_molecules(["H4O2"], index="molecular_formula")
    assert water.compare(get_mol[0])


def test_options_portal(test_server):

    client = portal.FractalClient(test_server.get_address(""))

    opts = portal.data.get_options("psi_default")

    # Test add
    ret = client.add_options(opts)

    # Test get
    get_opt = client.get_options({'program': opts["program"], 'name': opts["name"]})

    assert opts == get_opt[0]


def test_collection_portal(test_server):

    db = {"collection": "torsiondrive", "name": "Torsion123", "something": "else", "array": ["54321"]}

    client = portal.FractalClient(test_server.get_address(""))

    # Test add
    ret = client.add_collection(db)

    # Test get
    get_db = client.get_collection(db["collection"], db["name"], full_return=True)
    del get_db["data"][0]["id"]

    assert db == get_db["data"][0]
