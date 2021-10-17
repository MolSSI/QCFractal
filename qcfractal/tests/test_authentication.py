"""
Tests the on-node procedures compute capabilities.
"""

import time
import pytest
import requests

from qcfractal.portal.client import PortalClient, PortalRequestError
from qcfractal.portal.components.permissions import RoleInfo, UserInfo
from qcfractal.testing import TestingSnowflake


_users = {
    "read": {"pw": "hello1234", "role": "read"},
    "write": {"pw": "something123", "role": "submit"},
    "admin": {"pw": "something123", "role": "admin"},
}

_roles = {
    #    "read": {
    #        "Statement": [
    #            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
    #            {"Effect": "Deny", "Action": "*", "Resource": ["user", "manager"]},
    #        ]
    #    },
    "write": {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "POST", "Resource": ["molecule", "manager"]},
        ]
    },
    #    "admin": {
    #        "Statement": [
    #            {"Effect": "Allow", "Action": "*", "Resource": "*"},
    #        ]
    #    },
}


@pytest.fixture(scope="function")
def fractal_test_secure_server(temporary_database):
    """
    A QCFractal snowflake server with authorization/authentication enabled
    """

    db_config = temporary_database.config
    flask_config = {"jwt_access_token_expires": 1}  # expire tokens in 1 second
    extra_config = {"enable_security": True, "allow_unauthenticated_read": False, "flask": flask_config}

    with TestingSnowflake(db_config, start_flask=True, extra_config=extra_config) as server:
        # Get a storage socket and add the roles/users/passwords
        storage = server.get_storage_socket()
        for k, v in _roles.items():
            storage.roles.add(RoleInfo(rolename=k, permissions=v))
        for k, v in _users.items():
            uinfo = UserInfo(username=k, fullname="Ms. Test User", enabled=True, role=v["role"])
            storage.users.add(uinfo, password=v["pw"])
        yield server


@pytest.fixture(scope="function")
def fractal_test_secure_server_read(temporary_database):
    """
    A QCFractal snowflake server with authorization/authentication enabled, but allowing
    unauthenticated read
    """

    db_config = temporary_database.config
    flask_config = {"jwt_access_token_expires": 1}  # expire tokens in 1 second
    extra_config = {"enable_security": True, "allow_unauthenticated_read": True, "flask": flask_config}

    with TestingSnowflake(db_config, start_flask=True, extra_config=extra_config) as server:
        # Get a storage socket and add the roles/users/passwords
        storage = server.get_storage_socket()
        for k, v in _roles.items():
            storage.roles.add(RoleInfo(rolename=k, permissions=v))
        for k, v in _users.items():
            uinfo = UserInfo(username=k, fullname="Ms. Test User", enabled=True, role=v["role"])
            storage.users.add(uinfo, password=v["pw"])
        yield server


def test_security_auth_decline_none(fractal_test_secure_server):
    with pytest.raises(PortalRequestError, match=r"Missing Authorization Header"):
        client = fractal_test_secure_server.client()
        client.query_molecules(molecule_hash=[])


def test_security_auth_bad_ssl(fractal_test_secure_server):
    with pytest.raises(ConnectionRefusedError) as excinfo:
        address = fractal_test_secure_server.get_uri()
        address = address.replace("http", "https")

        client = PortalClient(
            address=address,
            username="write",
            password=_users["write"]["pw"],
            verify=True,
        )
        client.query_molecules(molecule_hash=[])

    assert "ssl handshake" in str(excinfo.value).lower()
    assert "verify=false" in str(excinfo.value).lower()


def test_security_auth_decline_bad_user(fractal_test_secure_server):
    with pytest.raises(ConnectionRefusedError, match=r"Authentication failed"):
        client = PortalClient(
            address=fractal_test_secure_server.get_uri(),
            username="hello",
            password="something",
            verify=False,
        )
        client.query_molecules(molecule_hash=[])


def test_security_auth_accept(fractal_test_secure_server):

    address = fractal_test_secure_server.get_uri()
    client = PortalClient(address=address, username="write", password=_users["write"]["pw"])

    client.add_molecules([])
    client.query_molecules(molecule_hash=[])


def test_security_auth_refresh(fractal_test_secure_server):

    address = fractal_test_secure_server.get_uri()
    client = PortalClient(address=address, username="write", password=_users["write"]["pw"])
    client.add_molecules([])

    assert client.refresh_token

    time.sleep(3)

    # Manually try to do this (to work around FractalClient doing automatic refreshing)
    r = requests.post(client.address + "v1/molecule", json=[], headers=client._headers)

    assert r.status_code == 401
    assert "Token has expired" in r.json()["msg"]

    # will automatically refresh JWT and get new access_token
    client.add_molecules([])


## Allow read tests


def test_security_auth_allow_read(fractal_test_secure_server_read):
    # Will attempt to read /information
    client = fractal_test_secure_server_read.client()


def test_security_auth_allow_read_block_add(fractal_test_secure_server_read):
    client = fractal_test_secure_server_read.client()

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.add_molecules([])
