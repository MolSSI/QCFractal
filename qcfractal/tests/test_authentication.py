"""
Tests the on-node procedures compute capabilities.
"""

import pytest
import requests

import qcfractal
import qcfractal.interface as ptl
from qcfractal import testing

## For mock flask responses
from requests_mock_flask import add_flask_app_to_mock
import requests_mock
import responses


_users = {
    "read": {"pw": "hello", "rolename": "read"},
    "write": {"pw": "something", "rolename": "write"},
    "admin": {"pw": "something", "rolename": "admin"},
}

_roles = {
    "read": {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Deny", "Action": "*", "Resource": ["user", "manager"]},
        ]
    },
    "write": {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "POST", "Resource": ["molecule", "manager"]},
        ]
    },
    "admim": {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
        ]
    },
}


@pytest.fixture(scope="module")
def sec_server(request, postgres_server):
    """
    Builds a server instance with the event loop running in a thread.
    """

    storage_name = "test_auth_qcarchivedb"
    postgres_server.create_database(storage_name)

    # with testing.loop_in_thread() as loop:

    # Build server, manually handle IOLoop (no start/stop needed)
    server = qcfractal.FractalServer(
        port=testing.find_open_port(),
        storage_uri=postgres_server.database_uri(),
        storage_project_name=storage_name,
        security="local",
        allow_read=False,
        skip_storage_version_check=True,
        flask_config="testing",
    )

    # Clean and re-init the database
    server.storage._clear_db(storage_name)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp_m:
        # with requests_mock.Mocker() as resp_m:
        add_flask_app_to_mock(
            mock_obj=resp_m,
            flask_app=server.app,
            base_url=server.get_address(),
        )
        # add roles
        for k, v in _roles.items():
            assert server.storage.add_role(k, permissions=v)
        # Add local users
        for k, v in _users.items():
            assert server.storage.add_user(k, v["pw"], v["rolename"])

        yield server


@pytest.fixture(scope="module")
def sec_server_allow_read(sec_server, postgres_server):
    """
    New sec server with read allowed
    """
    server = qcfractal.FractalServer(
        name="qcf_server_allow_read",
        port=testing.find_open_port(),
        storage_project_name=sec_server.storage.get_project_name(),
        storage_uri=postgres_server.database_uri(),
        security="local",
        allow_read=True,
    )

    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp_m:
        # with requests_mock.Mocker() as resp_m:
        add_flask_app_to_mock(
            mock_obj=resp_m,
            flask_app=server.app,
            base_url=server.get_address(),
        )

        yield server


### Tests the compute queue stack
def test_security_auth_decline_none(sec_server):
    with pytest.raises(IOError) as excinfo:
        client = ptl.FractalClient(sec_server)
        client.query_molecules(id=[])
    assert "missing authorization header" in str(excinfo.value).lower()


# TODO: fixme: ssl error not raised
@pytest.mark.skip("SSL check not working!")
def test_security_auth_bad_ssl(sec_server):
    with pytest.raises(ConnectionError) as excinfo:
        client = ptl.FractalClient.from_file(
            {
                "address": sec_server.get_address(),
                "username": "write",
                "password": _users["write"]["pw"],
                "verify": True,
            }
        )
        client.query_molecules(id=[])

    assert "ssl handshake" in str(excinfo.value).lower()
    assert "verify=false" in str(excinfo.value).lower()


def test_security_auth_decline_bad_user(sec_server):
    with pytest.raises(IOError) as excinfo:
        client = ptl.FractalClient.from_file(
            {"address": sec_server.get_address(), "username": "hello", "password": "something", "verify": False}
        )
        r = client.query_molecules(id=[])
    assert "authentication failed" in str(excinfo.value).lower()


def test_security_auth_accept(sec_server):

    client = ptl.FractalClient(sec_server, username="write", password=_users["write"]["pw"])

    r = client.add_molecules([])
    r = client.query_molecules(id=[])


def test_security_auth_refresh(sec_server):

    client = ptl.FractalClient(sec_server, username="write", password=_users["write"]["pw"])
    client._set_encoding("json")

    client.add_molecules([])

    assert client.refresh_token

    import time

    time.sleep(3)

    r = requests.post(client.address + "molecule", json={"data": [], "meta": {}}, headers=client._headers)

    assert r.status_code == 401
    assert "Token has expired" in r.json()["msg"]

    # will automatically refresh JWT and get new access_token
    client.add_molecules([])


def test_security_auth_password_gen(sec_server):

    success, pw = sec_server.storage.add_user("autogenpw", None, "read")
    assert success
    assert sec_server.storage.verify_user("autogenpw", pw)[0]
    assert isinstance(pw, str)
    assert len(pw) > 20


## Allow read tests


def test_security_auth_allow_read(sec_server_allow_read):
    client = ptl.FractalClient(sec_server_allow_read)


def test_security_auth_allow_read_block_add(sec_server_allow_read):
    client = ptl.FractalClient(sec_server_allow_read)

    with pytest.raises(IOError) as excinfo:
        client.add_molecules([ptl.Molecule.from_data("He 0 0 0")])
    assert "forbidden" in str(excinfo.value).lower()
