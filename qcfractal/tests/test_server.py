"""
Tests the DQM Server class
"""

import pytest
import requests
from qcelemental.util import deserialize

meta_set = {"errors", "n_inserted", "success", "duplicates", "error_description", "validation_errors"}


def test_server_up(fractal_test_server):
    info_addr = fractal_test_server.get_uri() + "/information"  # Targets and endpoint in the FractalServer

    r = requests.get(info_addr, json={})
    assert r.status_code == 200, r.reason


def test_server_full_read(fractal_test_server):

    addr = fractal_test_server.get_uri() + "/manager"

    body = {"meta": "", "data": ""}

    r = requests.get(addr, json=body)  # , headers=fractal_test_server.app.config.headers)
    assert r.status_code == 200


def test_server_information(fractal_test_server):

    client = fractal_test_server.client()

    server_info = client.server_information()
    assert {"name", "manager_heartbeat_frequency"} <= set(server_info.keys())


def test_storage_api(fractal_test_server):

    storage_api_addr = fractal_test_server.get_uri() + "/collection"  # Targets and endpoint in the FractalServer
    print("ADDR: ", storage_api_addr)

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

    encoding = r.headers["Content-Type"].split("/")[1]
    pdata = deserialize(r.content, encoding=encoding)
    assert pdata["meta"].keys() == meta_set
    assert pdata["meta"]["n_inserted"] == 1

    r = requests.get(
        storage_api_addr, json={"meta": {}, "data": {"collection": storage["collection"], "name": storage["name"]}}
    )
    # print(r.content)
    assert r.status_code == 200, r.reason

    encoding = r.headers["Content-Type"].split("/")[1]
    pdata = deserialize(r.content, encoding=encoding)
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
    r = requests.get(f"{storage_api_addr}/{col_id}", json={"meta": {}, "data": {}})

    encoding = r.headers["Content-Type"].split("/")[1]
    r = deserialize(r.content, encoding=encoding)
    assert r["meta"]["success"] is True
    assert len(r["data"]) == 1
    assert r["data"][0]["id"] == col_id

    r = requests.get(f"{storage_api_addr}/{col_id}", json={"meta": {}, "data": {"name": "wrong name"}})

    encoding = r.headers["Content-Type"].split("/")[1]
    r = deserialize(r.content, encoding=encoding)
    assert r["meta"]["success"] is True
    assert len(r["data"]) == 0


def test_bad_collection_get(fractal_test_server):
    for storage_api_addr in [
        fractal_test_server.get_uri() + "/collection/1234/entry",
        fractal_test_server.get_uri() + "/collection/1234/value",
        fractal_test_server.get_uri() + "/collection/1234/list",
        fractal_test_server.get_uri() + "/collection/1234/molecule",
    ]:
        r = requests.get(storage_api_addr, json={"meta": {}, "data": {}})

        assert r.status_code == 200, f"{r.reason} {storage_api_addr}"

        encoding = r.headers["Content-Type"].split("/")[1]
        rcontent = deserialize(r.content, encoding=encoding)
        assert rcontent["meta"]["success"] is False, storage_api_addr


def test_bad_collection_post(fractal_test_server):
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
        fractal_test_server.get_uri() + "/collection/1234",
        fractal_test_server.get_uri() + "/collection/1234/value",
        fractal_test_server.get_uri() + "/collection/1234/entry",
        fractal_test_server.get_uri() + "/collection/1234/list",
        fractal_test_server.get_uri() + "/collection/1234/molecule",
    ]:
        r = requests.post(storage_api_addr, json={"meta": {}, "data": storage})
        assert r.status_code == 200, r.reason

        encoding = r.headers["Content-Type"].split("/")[1]
        r = deserialize(r.content, encoding=encoding)
        assert r["meta"]["success"] is False


def test_bad_view_endpoints(fractal_test_server):
    """Tests that certain misspellings of the view endpoints result in 404s"""
    addr = fractal_test_server.get_uri()

    assert requests.get(addr + "/collection//value").status_code == 404
    assert requests.get(addr + "/collection/234/values").status_code == 404
    assert requests.get(addr + "/collection/234/view/value").status_code == 404
    assert requests.get(addr + "/collection/value").status_code == 404
    assert requests.get(addr + "/collection/S22").status_code == 404
