"""
Tests the DQM Server class
"""

import qcfractal.interface as qp
from qcfractal.testing import test_server, test_server_address

import pytest
import requests

mol_api_addr = test_server_address + "molecule"


def test_molecule_socket(test_server):

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Add a molecule
    r = requests.post(mol_api_addr, json={"meta": {}, "data": {"molecules": [water.to_json()]}})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata.keys() == {"errors", "ids", "nInserted", "success"}

    # Retrieve said molecule
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"ids": pdata["ids"], "index": "id"}})
    assert r.status_code == 200

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])

    # Retrieve said molecule via hash
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"ids": water.get_hash(), "index": "hash"}})
    assert r.status_code == 200

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])
