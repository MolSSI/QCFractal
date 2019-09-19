"""
Tests the DQM Server class
"""

import os
import json
import threading

import pytest
import requests

import qcfractal.interface as ptl
from qcfractal import FractalServer, FractalSnowflake, FractalSnowflakeHandler
from qcfractal.testing import (await_true, find_open_port, pristine_loop,
                               test_server, using_geometric, using_rdkit, using_torsiondrive)

meta_set = {'errors', 'n_inserted', 'success', 'duplicates', 'error_description', 'validation_errors'}


def test_server_information(test_server):

    client = ptl.FractalClient(test_server)

    server_info = client.server_information()
    assert {"name", "heartbeat_frequency"} <= server_info.keys()


def test_molecule_socket(test_server):

    mol_api_addr = test_server.get_address("molecule")
    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    water_json = json.loads(water.json())
    # Add a molecule
    r = requests.post(mol_api_addr, json={"meta": {}, "data": [water_json]})
    assert r.status_code == 200, r.reason

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set

    # Retrieve said molecule
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"id": pdata["data"][0]}})
    assert r.status_code == 200, r.reason

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])

    # Retrieve said molecule via hash
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"molecule_hash": water.get_hash()}})
    assert r.status_code == 200, r.reason

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])


def test_keywords_socket(test_server):

    opt_api_addr = test_server.get_address("keyword")
    opts = {"values": {"opt": "a"}}
    # Add a molecule
    r = requests.post(opt_api_addr, json={"meta": {}, "data": [opts]})
    assert r.status_code == 200, r.reason

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    data_payload = {"id": pdata["data"][0]}

    r = requests.get(opt_api_addr, json={"meta": {}, "data": data_payload})
    assert r.status_code == 200, r.reason

    assert r.json()["data"][0]["values"] == opts["values"]

    # Try duplicates
    r = requests.post(opt_api_addr, json={"meta": {}, "data": [opts]})
    assert r.status_code == 200, r.reason
    assert len(r.json()["meta"]["duplicates"]) == 1


def test_storage_socket(test_server):

    storage_api_addr = test_server.get_address("collection")  # Targets and endpoint in the FractalServer
    storage = {"collection": "TorsionDriveRecord", "name": "Torsion123", "something": "else", "array": ["54321"]}
    # Cast collection type to lower since the server-side does it anyways
    storage['collection'] = storage['collection'].lower()

    r = requests.post(storage_api_addr, json={"meta": {}, "data": storage})
    assert r.status_code == 200, r.reason

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    r = requests.get(storage_api_addr,
                     json={
                         "meta": {},
                         "data": {
                             "collection": storage["collection"],
                             "name": storage["name"]
                         }
                     })
    assert r.status_code == 200, r.reason

    pdata = r.json()
    del pdata["data"][0]["id"]
    # got a default values when created
    pdata["data"][0].pop("tags", None)
    pdata["data"][0].pop("tagline", None)
    pdata["data"][0].pop("provenance", None)
    assert pdata["data"][0] == storage


@pytest.mark.slow
def test_snowflakehandler_restart():

    with FractalSnowflakeHandler() as server:
        server.client()
        proc1 = server._qcfractal_proc

        server.restart()

        server.client()
        proc2 = server._qcfractal_proc

    assert proc1 != proc2
    assert proc1.poll() is not None
    assert proc2.poll() is not None


def test_snowflakehandler_log():

    with FractalSnowflakeHandler() as server:
        proc = server._qcfractal_proc

        assert "No SSL files passed in" in server.show_log(show=False, nlines=100)
        assert "0 task" not in server.show_log(show=False, nlines=100)

    assert proc.poll() is not None


@pytest.mark.slow
@using_geometric
@using_torsiondrive
@using_rdkit
def test_snowflake_service():
    with FractalSnowflakeHandler() as server:

        client = server.client()

        hooh = ptl.data.get_molecule("hooh.json")

        # Geometric options
        tdinput = {
            "initial_molecule": [hooh],
            "keywords": {
                "dihedrals": [[0, 1, 2, 3]],
                "grid_spacing": [90]
            },
            "optimization_spec": {
                "program": "geometric",
                "keywords": {
                    "coordsys": "tric",
                }
            },
            "qc_spec": {
                "driver": "gradient",
                "method": "UFF",
                "basis": None,
                "keywords": None,
                "program": "rdkit",
            },
        }

        ret = client.add_service([tdinput])

        def geometric_await():
            td = client.query_procedures(id=ret.ids)[0]
            return td.status == 'COMPLETE'

        assert await_true(60, geometric_await, period=2), client.query_procedures(id=ret.ids)[0]
