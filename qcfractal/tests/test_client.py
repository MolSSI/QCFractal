"""
Tests the interface portal adapter to the REST API
"""

import pytest
import numpy as np

import qcfractal.interface as ptl
from qcfractal.testing import test_server

# All tests should import test_server, but not use it
# Make PyTest aware that this module needs the server

valid_encodings = ["json", "json-ext", "msgpack-ext"]

@pytest.mark.parametrize("encoding", valid_encodings)
def test_client_molecule(test_server, encoding):

    client = ptl.FractalClient(test_server)
    client._set_encoding(encoding)

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water.geometry[:] += np.random.random(water.geometry.shape)

    # Test add
    ret = client.add_molecules([water])

    # Test get
    get_mol = client.query_molecules(id=ret[0])
    assert water.compare(get_mol[0])

    # Test molecular_formula get
    get_mol = client.query_molecules(molecular_formula="H4O2")
    assert len(get_mol)


@pytest.mark.parametrize("encoding", valid_encodings)
def test_client_keywords(test_server, encoding):

    client = ptl.FractalClient(test_server)
    client._set_encoding(encoding)

    opt = ptl.models.KeywordSet(values={"one": "fish", "two": encoding})

    # Test add
    ret = client.add_keywords([opt])

    # Test get
    get_kw = client.query_keywords([ret[0]])
    assert opt == get_kw[0]

    get_kw = client.query_keywords(hash_index=[opt.hash_index])
    assert opt == get_kw[0]


@pytest.mark.parametrize("encoding", valid_encodings)
def test_client_duplicate_keywords(test_server, encoding):

    client = ptl.FractalClient(test_server)
    client._set_encoding(encoding)

    key_name = f"key-{encoding}"
    opt1 = ptl.models.KeywordSet(values={key_name: 1})
    opt2 = ptl.models.KeywordSet(values={key_name: 2})
    opt3 = ptl.models.KeywordSet(values={key_name: 3})

    # Test add
    ret = client.add_keywords([opt1, opt1])
    assert len(ret) == 2
    assert ret[0] == ret[1]

    ret2 = client.add_keywords([opt1])
    assert len(ret2) == 1
    assert ret2[0] == ret[0]

    ret3 = client.add_keywords([opt2, opt1, opt3])
    assert len(ret3) == 3
    assert ret3[1] == ret[0]

@pytest.mark.parametrize("encoding", valid_encodings)
def test_empty_query(test_server, encoding):

    client = ptl.FractalClient(test_server)
    client._set_encoding(encoding)

    with pytest.raises(IOError) as error:
        client.query_procedures(limit=1)

    assert "ID is required" in str(error.value)


@pytest.mark.parametrize("encoding", valid_encodings)
def test_collection_portal(test_server, encoding):

    db_name = f"Torsion123-{encoding}"
    db = {"collection": "torsiondrive", "name": db_name, "something": "else", "array": ["12345"]}

    client = ptl.FractalClient(test_server)
    client._set_encoding(encoding)

    # Test add
    _ = client.add_collection(db)

    # Test get
    get_db = client.get_collection(db["collection"], db["name"], full_return=True)
    db_id = get_db.data[0].pop("id")

    # got a default values when created
    get_db.data[0].pop("tags", None)
    get_db.data[0].pop("tagline", None)
    get_db.data[0].pop("provenance", None)

    assert db == get_db.data[0]

    # Test add w/o overwrite
    ret = client.add_collection(db, full_return=True)
    assert ret.meta.success is False

    # Test that client is smart enough to trap non-id'ed overwrites
    with pytest.raises(KeyError):
        _ = client.add_collection(db, overwrite=True)

    # Test that we cannot use a local key
    db['id'] = 'local'
    db['array'] = ["6789"]
    with pytest.raises(KeyError):
        _ = client.add_collection(db, overwrite=True)

    # Finally test that we can overwrite
    db['id'] = db_id
    r = client.add_collection(db, overwrite=True)
    get_db = client.get_collection(db["collection"], db["name"], full_return=True)
    assert get_db.data[0]['array'] == ["6789"]
