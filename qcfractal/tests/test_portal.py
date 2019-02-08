"""
Tests the interface portal adapter to the REST API
"""

import qcfractal.interface as portal
from qcfractal.testing import test_server
import pytest


# All tests should import test_server, but not use it
# Make PyTest aware that this module needs the server


def test_molecule_portal(test_server):

    client = portal.FractalClient(test_server)

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

    client = portal.FractalClient(test_server)

    opt = portal.models.Option(program="psi4", options={"one": "fish", "two": "fish"})

    # Test add
    ret = client.add_options([opt])

    # Test get
    get_opt = client.get_options({'id': ret[0]})
    assert opt == get_opt[0]

    get_opt = client.get_options({"program": "psi4", "hash_index": opt.hash_index})
    assert opt == get_opt[0]


def test_collection_portal(test_server):

    db = {"collection": "torsiondrive", "name": "Torsion123", "something": "else", "array": ["54321"]}

    client = portal.FractalClient(test_server)

    # Test add
    _ = client.add_collection(db)

    # Test get
    get_db = client.get_collection(db["collection"], db["name"], full_return=True)
    db_id = get_db.data[0].pop("id")

    assert db == get_db.data[0]

    # Test add w/o overwrite
    ret = client.add_collection(db, full_return=True)
    assert ret.meta.success is False

    # Test that client is smart enough to trap non-id'ed overwrites
    with pytest.raises(KeyError):
        _ = client.add_collection(db, overwrite=True)

    # Test that we cannot use a local key
    db['id'] = 'local'
    db['array'] = ["12345"]
    with pytest.raises(KeyError):
        _ = client.add_collection(db, overwrite=True)

    # Finally test that we can overwrite
    db['id'] = db_id
    _ = client.add_collection(db, overwrite=True)
    get_db = client.get_collection(db["collection"], db["name"], full_return=True)
    assert get_db.data[0]['array'] == ["12345"]
