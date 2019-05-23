"""
Tests the DQM Server class
"""

import os
import threading

import pytest
import requests

import qcfractal.interface as ptl
from qcfractal import FractalServer, FractalSnowflake, FractalSnowflakeHandler
from qcfractal.testing import check_active_mongo_server, find_open_port, pristine_loop, test_server

meta_set = {'errors', 'n_inserted', 'success', 'duplicates', 'error_description', 'validation_errors'}


@pytest.mark.skip(reason="Hangs on Travis for some reason")
def test_start_stop():
    check_active_mongo_server()

    with pristine_loop() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(port=find_open_port(), storage_project_name="something", loop=loop, ssl_options=False)

        thread = threading.Thread(target=server.start, name="test IOLoop")
        thread.daemon = True
        thread.start()

        loop_started = threading.Event()
        loop.add_callback(loop_started.set)
        loop_started.wait()

        try:
            loop.add_callback(server.stop)
            thread.join(timeout=5)
        except:
            pass


def test_server_information(test_server):

    client = ptl.FractalClient(test_server)

    server_info = client.server_information()
    assert {"name", "heartbeat_frequency"} <= server_info.keys()


def test_molecule_socket(test_server):

    mol_api_addr = test_server.get_address("molecule")
    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    # Add a molecule
    r = requests.post(mol_api_addr, json={"meta": {}, "data": [water.json_dict()]})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set

    # Retrieve said molecule
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"id": pdata["data"][0]}})
    assert r.status_code == 200

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])

    # Retrieve said molecule via hash
    r = requests.get(mol_api_addr, json={"meta": {}, "data": {"molecule_hash": water.get_hash()}})
    assert r.status_code == 200

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])


def test_keywords_socket(test_server):

    opt_api_addr = test_server.get_address("keyword")
    opts = {"values": {"opt": "a"}}
    # Add a molecule
    r = requests.post(opt_api_addr, json={"meta": {}, "data": [opts]})
    assert r.status_code == 200

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    data_payload = {"id": pdata["data"][0]}

    r = requests.get(opt_api_addr, json={"meta": {}, "data": data_payload})
    assert r.status_code == 200

    assert r.json()["data"][0]["values"] == opts["values"]

    # Try duplicates
    r = requests.post(opt_api_addr, json={"meta": {}, "data": [opts]})
    assert r.status_code == 200
    assert len(r.json()["meta"]["duplicates"]) == 1


def test_storage_socket(test_server):

    storage_api_addr = test_server.get_address("collection")  # Targets and endpoint in the FractalServer
    storage = {"collection": "TorsionDriveRecord", "name": "Torsion123", "something": "else", "array": ["54321"]}
    # Cast collection type to lower since the server-side does it anyways
    storage['collection'] = storage['collection'].lower()

    r = requests.post(storage_api_addr, json={"meta": {}, "data": storage})
    assert r.status_code == 200

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
    assert r.status_code == 200

    pdata = r.json()
    del pdata["data"][0]["id"]
    # got a default values when created
    pdata["data"][0].pop("tags", None)
    pdata["data"][0].pop("tagline", None)
    assert pdata["data"][0] == storage


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