"""
Tests the DQM Server class
"""

import qcfractal.interface as qp
from qcfractal.testing import test_server, test_server_address

import pytest
import requests

mol_api_addr = test_server_address + "molecule"
opt_api_addr = test_server_address + "option"

meta_set = {'errors', 'n_inserted', 'success', 'duplicates', 'error_description', 'validation_errors'}

def test_molecule_socket(test_server):

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Add a molecule
    r = requests.post(mol_api_addr, json={"meta": {}, "data": {"molecules": {"water": water.to_json()}}})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set

    # Retrieve said molecule
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"ids": pdata["data"]["water"], "index": "id"}})
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


def test_option_socket(test_server):

    opts = qp.data.get_options("psi_default")
    # Add a molecule
    r = requests.post(opt_api_addr, json={"meta": {}, "data": [opts]})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata.keys() == meta_set

    # ret = db_socket.add_options(opts)
    # assert ret["n_inserted"] == 1

    # ret = db_socket.add_options(opts)
    # assert ret["n_inserted"] == 0

    # del opts["_id"]
    # assert opts == db_socket.get_options({"name": opts["name"], "program": opts["program"]})[0]


