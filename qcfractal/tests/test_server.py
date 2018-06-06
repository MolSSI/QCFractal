"""
Tests the DQM Server class
"""

import qcfractal.interface as qp
from qcfractal.testing import test_server, test_server_address

import pytest
import requests

mol_api_addr = test_server_address + "molecule"
opt_api_addr = test_server_address + "option"
db_api_addr = test_server_address + "database"

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
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    r = requests.get(opt_api_addr, json={"meta": {}, "data": [(opts["program"], opts["name"])]})
    assert r.status_code == 200

    assert r.json()["data"][0] == opts


def test_database_socket(test_server):

    db = {"category": "OpenFF", "name": "Torsion123", "something": "else", "array": ["54321"]}

    r = requests.post(db_api_addr, json={"meta": {}, "data": db})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    r = requests.get(db_api_addr, json={"meta": {}, "data": [(db["category"], db["name"])]})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata["data"][0] == db


