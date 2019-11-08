"""
Tests the DQM Server class
"""

import json
import os
import threading

import pytest
import requests

import qcfractal.interface as ptl
from qcfractal import FractalServer, FractalSnowflake, FractalSnowflakeHandler
from qcfractal.testing import (
    await_true,
    find_open_port,
    pristine_loop,
    test_server,
    using_geometric,
    using_rdkit,
    using_torsiondrive,
)

meta_set = {"errors", "n_inserted", "success", "duplicates", "error_description", "validation_errors"}


def test_server_information(test_server):

    client = ptl.FractalClient(test_server)

    server_info = client.server_information()
    assert {"name", "heartbeat_frequency", "counts"} <= server_info.keys()
    assert server_info["counts"].keys() >= {"molecule", "kvstore", "result", "collection"}


def test_storage_socket(test_server):

    storage_api_addr = test_server.get_address() + "collection"  # Targets and endpoint in the FractalServer
    storage = {
        "collection": "TorsionDriveRecord",
        "name": "Torsion123",
        "something": "else",
        "array": ["54321"],
        "visibility": True,
        "view_available": False,
        "group": "default",
    }
    # Cast collection type to lower since the server-side does it anyways
    storage["collection"] = storage["collection"].lower()

    r = requests.post(storage_api_addr, json={"meta": {}, "data": storage})
    assert r.status_code == 200, r.reason

    pdata = r.json()
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    r = requests.get(
        storage_api_addr, json={"meta": {}, "data": {"collection": storage["collection"], "name": storage["name"]}}
    )
    print(r.content)
    assert r.status_code == 200, r.reason

    pdata = r.json()
    col_id = pdata["data"][0].pop("id")
    # got a default values when created
    pdata["data"][0].pop("tags", None)
    pdata["data"][0].pop("tagline", None)
    pdata["data"][0].pop("provenance", None)
    pdata["data"][0].pop("view_url_hdf5", None)
    pdata["data"][0].pop("view_url_plaintext", None)
    pdata["data"][0].pop("view_metadata", None)
    pdata["data"][0].pop("description", None)

    assert pdata["data"][0] == storage

    # Test collection id sub-resource
    r = requests.get(f"{storage_api_addr}/{col_id}", json={"meta": {}, "data": {}}).json()
    assert r["meta"]["success"] is True
    assert len(r["data"]) == 1
    assert r["data"][0]["id"] == col_id

    r = requests.get(f"{storage_api_addr}/{col_id}", json={"meta": {}, "data": {"name": "wrong name"}}).json()
    assert r["meta"]["success"] is True
    assert len(r["data"]) == 0


def test_bad_collection_get(test_server):
    for storage_api_addr in [
        test_server.get_address() + "collection/1234/entry",
        test_server.get_address() + "collection/1234/value",
        test_server.get_address() + "collection/1234/list",
        test_server.get_address() + "collection/1234/molecule",
    ]:
        r = requests.get(storage_api_addr, json={"meta": {}, "data": {}})
        assert r.status_code == 200, f"{r.reason} {storage_api_addr}"
        assert r.json()["meta"]["success"] is False, storage_api_addr


def test_bad_collection_post(test_server):
    storage = {
        "collection": "TorsionDriveRecord",
        "name": "Torsion123",
        "something": "else",
        "array": ["54321"],
        "visibility": True,
        "view_available": False,
    }
    # Cast collection type to lower since the server-side does it anyways
    storage["collection"] = storage["collection"].lower()

    for storage_api_addr in [
        test_server.get_address() + "collection/1234",
        test_server.get_address() + "collection/1234/value",
        test_server.get_address() + "collection/1234/entry",
        test_server.get_address() + "collection/1234/list",
        test_server.get_address() + "collection/1234/molecule",
    ]:
        r = requests.post(storage_api_addr, json={"meta": {}, "data": storage})
        assert r.status_code == 200, r.reason
        assert r.json()["meta"]["success"] is False


def test_bad_view_endpoints(test_server):
    """ Tests that certain misspellings of the view endpoints result in 404s """
    addr = test_server.get_address()

    assert requests.get(addr + "collection//value").status_code == 404
    assert requests.get(addr + "collection/234/values").status_code == 404
    assert requests.get(addr + "collections/234/value").status_code == 404
    assert requests.get(addr + "collection/234/view/value").status_code == 404
    assert requests.get(addr + "collection/value").status_code == 404
    assert requests.get(addr + "collection/S22").status_code == 404


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
            "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [90]},
            "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
            "qc_spec": {"driver": "gradient", "method": "UFF", "basis": None, "keywords": None, "program": "rdkit"},
        }

        ret = client.add_service([tdinput])

        def geometric_await():
            td = client.query_procedures(id=ret.ids)[0]
            return td.status == "COMPLETE"

        assert await_true(60, geometric_await, period=2), client.query_procedures(id=ret.ids)[0]
