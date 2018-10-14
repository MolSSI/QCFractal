"""
Tests the on-node procedures compute capabilities.
"""

import cryptography
import pytest
import requests

import qcfractal
import qcfractal.interface as portal
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

    with testing.pristine_loop() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = qcfractal.FractalServer(
            port=testing.find_open_port(),
            storage_project_name=storage_name,
            io_loop=loop,
            security="local")

        # Clean and re-init the databse
        server.storage.client.drop_database(server.storage._project_name)
        server.storage.init_database()

        # Add local users
        for k, v in _users.items():
            assert server.storage.add_user(k, _users[k]["pw"], _users[k]["perm"])

        with testing.active_loop(loop) as act:
            yield server


### Tests the compute queue stack
def test_security_auth_decline_none(sec_server):
    client = portal.FractalClient(sec_server.get_address(), verify=False)
    assert "FractalClient" in str(client)

    with pytest.raises(requests.exceptions.HTTPError):
        r = client.get_molecules([])

    with pytest.raises(requests.exceptions.HTTPError):
        r = client.add_molecules({})

def test_security_auth_bad_ssl(sec_server):
    client = portal.FractalClient.from_file({
        "address": sec_server.get_address(),
        "username": "read",
        "password": _users["write"]["pw"],
        "verify": True
    })

    with pytest.raises(requests.exceptions.SSLError):
        r = client.get_molecules([])

def test_security_auth_decline_bad_user(sec_server):
    client = portal.FractalClient.from_file({
        "address": sec_server.get_address(),
        "username": "hello",
        "password": "something",
        "verify": False
    })

    with pytest.raises(requests.exceptions.HTTPError):
        r = client.get_molecules([])

    with pytest.raises(requests.exceptions.HTTPError):
        r = client.add_molecules({})


def test_security_auth_accept(sec_server):

    client = portal.FractalClient(
        sec_server.get_address(), username="write", password=_users["write"]["pw"], verify=False)

    r = client.add_molecules({})
    r = client.get_molecules([])