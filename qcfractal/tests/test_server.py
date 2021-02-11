"""
Tests the DQM Server class
"""

import pytest
import requests

meta_set = {"errors", "n_inserted", "success", "duplicates", "error_description", "validation_errors"}


def test_server_up(test_server):
    info_addr = test_server.get_uri() + "/information"  # Targets and endpoint in the FractalServer

    r = requests.get(info_addr, json={})
    assert r.status_code == 200, r.reason


def test_server_full_read(test_server):

    addr = test_server.get_uri() + "/manager"

    body = {"meta": "", "data": ""}

    r = requests.get(addr, json=body)  # , headers=test_server.app.config.headers)
    assert r.status_code == 200


def test_server_information(test_server):

    client = test_server.client()

    server_info = client.server_information()
    assert {"name", "manager_heartbeat_frequency"} <= set(server_info.keys())


def test_storage_api(test_server):

    storage_api_addr = test_server.get_uri() + "/collection"  # Targets and endpoint in the FractalServer
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
    # print(r.content)
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
    r = requests.get(f"{storage_api_addr}/{col_id}", json={"meta": {}, "data": {}})
    r = r.json()
    assert r["meta"]["success"] is True
    assert len(r["data"]) == 1
    assert r["data"][0]["id"] == col_id

    r = requests.get(f"{storage_api_addr}/{col_id}", json={"meta": {}, "data": {"name": "wrong name"}}).json()
    assert r["meta"]["success"] is True
    assert len(r["data"]) == 0


def test_bad_collection_get(test_server):
    for storage_api_addr in [
        test_server.get_uri() + "/collection/1234/entry",
        test_server.get_uri() + "/collection/1234/value",
        test_server.get_uri() + "/collection/1234/list",
        test_server.get_uri() + "/collection/1234/molecule",
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
        test_server.get_uri() + "/collection/1234",
        test_server.get_uri() + "/collection/1234/value",
        test_server.get_uri() + "/collection/1234/entry",
        test_server.get_uri() + "/collection/1234/list",
        test_server.get_uri() + "/collection/1234/molecule",
    ]:
        r = requests.post(storage_api_addr, json={"meta": {}, "data": storage})
        assert r.status_code == 200, r.reason
        assert r.json()["meta"]["success"] is False


def test_bad_view_endpoints(test_server):
    """ Tests that certain misspellings of the view endpoints result in 404s """
    addr = test_server.get_uri()

    assert requests.get(addr + "/collection//value").status_code == 404
    # TODO: mocker can't handle this
    # assert requests.get(addr + "/collection/234/values").status_code == 404
    with pytest.raises(requests.exceptions.ConnectionError):
        assert requests.get(addr + "/collections/234/value").status_code == 404
    assert requests.get(addr + "/collection/234/view/value").status_code == 404
    assert requests.get(addr + "/collection/value").status_code == 404
    assert requests.get(addr + "/collection/S22").status_code == 404
