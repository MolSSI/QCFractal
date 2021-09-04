"""
Tests the interface portal adapter to the REST API
"""

import numpy as np
import pytest

import qcfractal.app.routes.collections
import qcfractal.interface as ptl

valid_encodings = ["json", "json-ext", "msgpack-ext"]


@pytest.mark.parametrize("encoding", valid_encodings)
def test_client_molecule(fractal_test_server, encoding):

    client = fractal_test_server.client()
    client._set_encoding(encoding)

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water.geometry[:] += np.random.random(water.geometry.shape)

    # Test add
    ret = client.add_molecules([water])

    # Test get as a list
    get_mol = client.query_molecules(id=[ret[0]])
    assert water.compare(get_mol[0])

    # Test get as a single id
    get_mol = client.query_molecules(id=ret[0])
    assert water.compare(get_mol[0])

    # Test molecular_formula get
    get_mol = client.query_molecules(molecular_formula=["H4O2"])
    assert len(get_mol)


@pytest.mark.parametrize("encoding", valid_encodings)
def test_client_keywords(fractal_test_server, encoding):

    client = fractal_test_server.client()
    client._set_encoding(encoding)

    opt = ptl.models.KeywordSet(values={"one": "fish", "two": encoding})

    # Test add
    ret = client.add_keywords([opt])

    # Test get
    get_kw = client.query_keywords([ret[0]])
    assert opt.dict(exclude={"id"}) == get_kw[0].dict(exclude={"id"})


@pytest.mark.parametrize("encoding", valid_encodings)
def test_client_duplicate_keywords(fractal_test_server, encoding):

    client = fractal_test_server.client()
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
def test_collection_portal(fractal_test_server, encoding):

    db_name = f"Torsion123-{encoding}"
    db = {
        "collection": "torsiondrive",
        "name": db_name,
        "something": "else",
        "array": ["12345"],
        "visibility": True,
        "view_available": False,
        "group": "default",
    }

    client = fractal_test_server.client()
    client._set_encoding(encoding)

    # Test add
    ret = client.add_collection(db, full_return=True)
    print(ret)

    # Test get
    get_db = qcfractal.app.new_routes.collections.get_collection(db["collection"], db["name"], full_return=True)
    db_id = get_db.data[0].pop("id")

    # got a default values when created
    get_db.data[0].pop("tags", None)
    get_db.data[0].pop("tagline", None)
    get_db.data[0].pop("provenance", None)
    get_db.data[0].pop("view_url_hdf5", None)
    get_db.data[0].pop("view_url_plaintext", None)
    get_db.data[0].pop("view_metadata", None)
    get_db.data[0].pop("description", None)

    assert db == get_db.data[0]

    # Test add w/o overwrite
    ret = client.add_collection(db, full_return=True)
    assert ret.meta.success is False

    # Test that client is smart enough to trap non-id'ed overwrites
    with pytest.raises(KeyError):
        _ = client.add_collection(db, overwrite=True)

    # Test that we cannot use a local key
    db["id"] = "local"
    db["array"] = ["6789"]
    with pytest.raises(KeyError):
        _ = client.add_collection(db, overwrite=True)

    # Finally test that we can overwrite
    db["id"] = db_id
    r = client.add_collection(db, overwrite=True)
    get_db = qcfractal.app.new_routes.collections.get_collection(db["collection"], db["name"], full_return=True)
    assert get_db.data[0]["array"] == ["6789"]
