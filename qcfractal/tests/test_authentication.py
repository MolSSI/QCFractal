"""
Tests the on-node procedures compute capabilities.
"""

import pytest
import requests

import qcfractal
import qcfractal.interface as ptl
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
    # testing.check_active_mongo_server()
    # storage_name = "qcf_local_server_auth_test"
    # storage_uri = "mongodb://localhost"

    testing.check_active_mongo_server()
    storage_name = "test_qcarchivedb"

    with testing.loop_in_thread() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = qcfractal.FractalServer(port=testing.find_open_port(),
                                         storage_uri=testing.POSTGRES_TESTING_URI,
                                         storage_project_name=storage_name,
                                         loop=loop,
                                         security="local")

        # Clean and re-init the databse
        server.storage._clear_db(storage_name)

        # Add local users
        for k, v in _users.items():
            assert server.storage.add_user(k, _users[k]["pw"], _users[k]["perm"])

        yield server


@pytest.fixture(scope="module")
def sec_server_allow_read(sec_server):
    """
    New sec server with read allowed
    """
    yield qcfractal.FractalServer(name="qcf_server_allow_read",
                                  port=testing.find_open_port(),
                                  storage_project_name=sec_server.storage.get_project_name(),
                                  storage_uri=testing.POSTGRES_TESTING_URI,
                                  loop=sec_server.loop,
                                  security="local",
                                  allow_read=True)


### Tests the compute queue stack
def test_security_auth_decline_none(sec_server):
    with pytest.raises(IOError) as excinfo:
        client = ptl.FractalClient(sec_server)

    assert "user not found" in str(excinfo.value).lower()


def test_security_auth_bad_ssl(sec_server):
    with pytest.raises(ConnectionError) as excinfo:
        client = ptl.FractalClient.from_file({
            "address": sec_server.get_address(),
            "username": "read",
            "password": _users["write"]["pw"],
            "verify": True
        })

    assert "ssl handshake" in str(excinfo.value).lower()
    assert "verify=false" in str(excinfo.value).lower()


def test_security_auth_decline_bad_user(sec_server):
    with pytest.raises(IOError) as excinfo:
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
    r = client.query_molecules(id=[])


def test_security_auth_password_gen(sec_server):

    pw = sec_server.storage.add_user("autogenpw", None, ["read"])
    assert sec_server.storage.verify_user("autogenpw", pw, "read")[0]
    assert isinstance(pw, str)
    assert len(pw) > 20


def test_security_auth_overwrite(sec_server):

    user = "auth_overwrite"
    pw1 = sec_server.storage.add_user(user, None, ["read"])
    assert sec_server.storage.verify_user(user, pw1, "read")[0]

    pw2 = sec_server.storage.add_user(user, None, ["read"], overwrite=True)
    assert isinstance(pw2, str)
    assert pw1 != pw2

    assert sec_server.storage.verify_user(user, pw1, "read")[0] is False
    assert sec_server.storage.verify_user(user, pw2, "read")[0]


## Allow read tests


def test_security_auth_allow_read(sec_server_allow_read):
    client = ptl.FractalClient(sec_server_allow_read)


def test_security_auth_allow_read_block_add(sec_server_allow_read):
    client = ptl.FractalClient(sec_server_allow_read)

    with pytest.raises(IOError) as excinfo:
        client.add_molecules([ptl.Molecule.from_data("He 0 0 0")])
    assert "user not found" in str(excinfo.value).lower()
