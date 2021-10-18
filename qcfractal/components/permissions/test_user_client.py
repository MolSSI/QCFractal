"""
Tests the client functions related to user management
"""

import pytest

from .test_user_socket import invalid_usernames
from qcfractal.portal.client import PortalRequestError
from qcfractal.portal.components.permissions import UserInfo, PortalUser
from qcfractal.testing import TestingSnowflake, _test_users
from qcfractal.exceptions import InvalidUsernameError, InvalidPasswordError, InvalidRolenameError


def test_user_client_list(fractal_test_secure_server: TestingSnowflake):
    client = fractal_test_secure_server.client("admin_user", _test_users["admin_user"]["pw"])
    users = client.list_users()

    assert len(users) == len(_test_users)

    for u in users:
        assert u.username in _test_users

        tu = _test_users[u.username]["info"]
        assert u.role == tu["role"]
        assert u.fullname == tu["fullname"]
        assert u.organization == tu["organization"]
        assert u.email == tu["email"]


def test_user_client_get(fractal_test_secure_server: TestingSnowflake):
    client = fractal_test_secure_server.client("admin_user", _test_users["admin_user"]["pw"])

    for username, uinfo in _test_users.items():
        u = client.get_user(username)

        assert u.client is client
        assert u.username == username
        assert u.role == uinfo["info"]["role"]


@pytest.mark.parametrize("username", invalid_usernames)
def test_user_client_use_invalid_username(fractal_test_secure_server: TestingSnowflake, username: str):
    client = fractal_test_secure_server.client("admin_user", _test_users["admin_user"]["pw"])

    with pytest.raises(InvalidUsernameError):
        client.get_user(username)

    with pytest.raises(InvalidUsernameError):
        client.delete_user(username)


def test_user_client_use_nonexist(fractal_test_secure_server: TestingSnowflake):
    client = fractal_test_secure_server.client("admin_user", _test_users["admin_user"]["pw"])

    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.get_user("no_user")
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.delete_user("no_user")


def test_user_client_get_me(fractal_test_secure_server: TestingSnowflake):
    for username, uinfo in _test_users.items():
        client = fractal_test_secure_server.client(username, uinfo["pw"])
        me = client.get_user()

        assert me.client is client
        assert me.username == username
        assert me.role == uinfo["info"]["role"]


def test_user_client_delete(fractal_test_secure_server: TestingSnowflake):
    client = fractal_test_secure_server.client("admin_user", _test_users["admin_user"]["pw"])

    client.delete_user("read_user")

    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.get_user("read_user")


def test_user_client_delete_self(fractal_test_secure_server: TestingSnowflake):
    client = fractal_test_secure_server.client("admin_user", _test_users["admin_user"]["pw"])

    with pytest.raises(RuntimeError, match=r"Cannot delete your own user"):
        client.delete_user("admin_user")
