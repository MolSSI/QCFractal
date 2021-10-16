"""
Tests for the user and role subsockets
"""

import pytest
from qcfractal.exceptions import UserManagementError
from qcfractal.portal.components.permissions import RoleInfo, UserInfo
from qcfractal.components.permissions.role_socket import default_roles


def test_role_defaults(storage_socket):
    # Test that default roles are created
    for rolename, permissions in default_roles.items():
        r = storage_socket.roles.get(rolename)
        assert r.permissions == permissions


def test_role_nonexist(storage_socket):
    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.get("doesntexist")

    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.modify("doesntexist", "{}")


def test_role_add(storage_socket):
    # Try adding a new role that can read anything but only modify molecules
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "*", "Resource": ["molecule"]},
        ]
    }

    storage_socket.roles.add("molecule_admin", new_perms)

    rinfo = storage_socket.roles.get("molecule_admin")
    assert rinfo.permissions == new_perms

    # Raises exception on error
    storage_socket.roles.delete("molecule_admin")


def test_role_modify_admin(storage_socket):
    # The admin role should not be modifiable
    with pytest.raises(UserManagementError, match=r"Cannot modify the admin role"):
        storage_socket.roles.modify("admin", "{}")


def test_role_modify(storage_socket):
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "*", "Resource": ["molecule"]},
        ]
    }

    storage_socket.roles.modify("read", new_perms)

    rinfo = storage_socket.roles.get("read")
    assert rinfo.permissions == new_perms


def test_role_delete(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo, password="oldpw")

    with pytest.raises(UserManagementError, match=r"Role could not be deleted"):
        storage_socket.roles.delete("read")

    # If we delete the user, we should be able to delete the role
    storage_socket.users.delete("george")
    storage_socket.roles.delete("read")


def test_role_list(storage_socket):
    # Try adding a new role that can read anything but only modify molecules
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Allow", "Action": "*", "Resource": ["molecule"]},
        ]
    }

    storage_socket.roles.add("molecule_admin", new_perms)

    # Now get all the roles
    role_lst = storage_socket.roles.list()

    role_lst = sorted(role_lst, key=lambda x: x.rolename)

    # Build a list of the expected roles (default roles + the one we added)
    expected = [RoleInfo(rolename=k, permissions=v) for k, v in default_roles.items()]
    expected.append(RoleInfo(rolename="molecule_admin", permissions=new_perms))
    expected = sorted(expected, key=lambda x: x.rolename)

    assert role_lst == expected


def test_role_reset(storage_socket):
    # Test resetting the roles to their defaults

    # Modify something
    new_perms = {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
            {"Effect": "Allow", "Action": "Deny", "Resource": ["molecule"]},
        ]
    }

    storage_socket.roles.modify("read", new_perms)
    rinfo = storage_socket.roles.get("read")
    assert rinfo.permissions == new_perms

    # Also delete
    storage_socket.roles.delete("monitor")
    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.get("monitor")

    # Now reset
    storage_socket.roles.reset_defaults()

    # Was the deleted role recreated, and the permissions fixed?
    for rolename, permissions in default_roles.items():
        rinfo = storage_socket.roles.get(rolename)
        assert rinfo.permissions == default_roles[rolename]
