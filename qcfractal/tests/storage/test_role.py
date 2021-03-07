"""
Tests for the user and role subsockets
"""

import pytest
from qcfractal.interface.models import RoleInfo, UserInfo
from qcfractal.storage_sockets.sqlalchemy_socket import AuthorizationFailure
from qcfractal.storage_sockets.subsockets.role import default_roles


def test_role_defaults(storage_socket):
    # Test that default roles are created
    for rolename, permissions in default_roles.items():
        r = storage_socket.role.get(rolename)
        assert r.permissions == permissions


def test_role_nonexist(storage_socket):
    with pytest.raises(AuthorizationFailure, match=r"Role.*does not exist"):
        storage_socket.role.get("doesntexist")

    with pytest.raises(AuthorizationFailure, match=r"Role.*does not exist"):
        storage_socket.role.modify("doesntexist", "{}")


def test_role_add(storage_socket):
    # Try adding a new role that can read anything but only modify molecules
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "*", "Resource": ["molecule"]},
        ]
    }

    storage_socket.role.add("molecule_admin", new_perms)

    rinfo = storage_socket.role.get("molecule_admin")
    assert rinfo.permissions == new_perms

    # Raises exception on error
    storage_socket.role.delete("molecule_admin")


def test_role_modify_admin(storage_socket):
    # The admin role should not be modifiable
    with pytest.raises(AuthorizationFailure, match=r"Cannot modify the admin role"):
        storage_socket.role.modify("admin", "{}")


def test_role_modify(storage_socket):
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "*", "Resource": ["molecule"]},
        ]
    }

    storage_socket.role.modify("read", new_perms)

    rinfo = storage_socket.role.get("read")
    assert rinfo.permissions == new_perms


def test_role_delete_inuse(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.user.add(uinfo, password="oldpw")

    with pytest.raises(AuthorizationFailure, match=r"Role could not be deleted"):
        storage_socket.role.delete("read")

    # If we delete the user, we should be able to delete the role
    storage_socket.user.delete("george")
    storage_socket.role.delete("read")


def test_role_list(storage_socket):
    # Try adding a new role that can read anything but only modify molecules
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "*", "Resource": ["molecule"]},
        ]
    }

    storage_socket.role.add("molecule_admin", new_perms)

    # Now get all the roles
    role_lst = storage_socket.role.list()

    role_lst = sorted(role_lst, key=lambda x: x.rolename)

    # Build a list of the expected roles (default roles + the one we added)
    expected = [RoleInfo(rolename=k, permissions=v) for k, v in default_roles.items()]
    expected.append(RoleInfo(rolename="molecule_admin", permissions=new_perms))
    expected = sorted(expected, key=lambda x: x.rolename)

    assert role_lst == expected
