"""
Tests the on-node procedures compute capabilities.
"""

import pytest
import qcfractal
import qcfractal.interface as ptl
import requests
from qcfractal import testing

_users = {
    "read": {
        "pw": "hello",
        "perm": ["read"]
    },
    "write": {
        "pw": "something",
        "perm": ["read", "write"]
    },
    "admin": {
        "pw": "something",
        "perm": ["read", "write", "compute", "admin"]
    }
}


@pytest.fixture(scope="module")
def sec_server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    # Check mongo
    testing.check_active_mongo_server()

    storage_name = "qcf_local_server_auth_test"

    with testing.loop_in_thread() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = qcfractal.FractalServer(
            port=testing.find_open_port(), storage_project_name=storage_name, loop=loop, security="local")

        # Clean and re-init the databse
        server.storage.client.drop_database(server.storage._project_name)

        # Add local users
        for k, v in _users.items():
            assert server.storage.add_user(k, _users[k]["pw"], _users[k]["perm"])

        yield server


@pytest.fixture(scope="module")
def sec_server_allow_read(sec_server):
    """
    New sec server with read allowed
    """

    yield qcfractal.FractalServer(
        name="qcf_server_allow_read",
        port=testing.find_open_port(),
        storage_project_name=sec_server.storage.get_project_name(),
        loop=sec_server.loop,
        security="local",
        allow_read=True)


### Tests the compute queue stack
def test_security_auth_decline_none(sec_server):
    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        client = ptl.FractalClient(sec_server)

    assert "user not found" in str(excinfo.value).lower()


def test_security_auth_bad_ssl(sec_server):
    with pytest.raises(requests.exceptions.SSLError) as excinfo:
        client = ptl.FractalClient.from_file({
            "address": sec_server.get_address(),
            "username": "read",
            "password": _users["write"]["pw"],
            "verify": True
        })

    assert "ssl handshake" in str(excinfo.value).lower()
    assert "verify=false" in str(excinfo.value).lower()


def test_security_auth_decline_bad_user(sec_server):
    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        client = ptl.FractalClient.from_file({
            "address": sec_server.get_address(),
            "username": "hello",
            "password": "something",
            "verify": False
        })

    assert "user not found" in str(excinfo.value).lower()


def test_security_auth_accept(sec_server):

    client = ptl.FractalClient(sec_server, username="write", password=_users["write"]["pw"])

    r = client.add_molecules([])
    r = client.get_molecules(id=[])


def test_security_auth_allow_read(sec_server_allow_read):
    client = ptl.FractalClient(sec_server_allow_read)


def test_security_auth_allow_read_block_add(sec_server_allow_read):
    client = ptl.FractalClient(sec_server_allow_read)

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        client.add_molecules([ptl.Molecule.from_data("He 0 0 0")])
    assert "user not found" in str(excinfo.value).lower()
