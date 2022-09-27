from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.auth.role_socket import default_roles
from qcportal.auth import RoleInfo, UserInfo
from qcportal.exceptions import UserManagementError, InvalidRolenameError

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

invalid_rolenames = ["\x00", "ab\x00cd", "1234", "a user", ""]


def test_role_socket_defaults(storage_socket: SQLAlchemySocket):
    # Test that default roles are created
    for rolename, permissions in default_roles.items():
        r = storage_socket.roles.get(rolename)
        assert r["permissions"] == permissions


def test_role_socket_add_get(storage_socket: SQLAlchemySocket):
    new_role = RoleInfo(
        rolename="new_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    storage_socket.roles.add(new_role)

    rinfo = storage_socket.roles.get("new_role")
    assert RoleInfo(**rinfo) == new_role


def test_role_socket_add_duplicate(storage_socket: SQLAlchemySocket):
    new_role = RoleInfo(
        rolename="new_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    storage_socket.roles.add(new_role)

    with pytest.raises(UserManagementError, match=r"Role.*already exists"):
        storage_socket.roles.add(new_role)


def test_role_socket_list(storage_socket: SQLAlchemySocket):
    new_role = RoleInfo(
        rolename="new_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    storage_socket.roles.add(new_role)

    # Now get all the roles
    role_lst = storage_socket.roles.list()
    role_lst_models = [RoleInfo(**x) for x in role_lst]

    role_lst_models = sorted(role_lst_models, key=lambda x: x.rolename)

    # Build a list of the expected roles (default roles + the one we added)
    expected = [RoleInfo(rolename=k, permissions=v) for k, v in default_roles.items()]
    expected.append(new_role)
    expected = sorted(expected, key=lambda x: x.rolename)

    assert role_lst_models == expected


def test_role_socket_delete(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo, password="oldpw123")

    with pytest.raises(UserManagementError, match=r"Role could not be deleted"):
        storage_socket.roles.delete("read")

    # If we delete the user, we should be able to delete the role
    storage_socket.users.delete("george")
    storage_socket.roles.delete("read")

    with pytest.raises(UserManagementError):
        storage_socket.roles.get("read")


def test_role_socket_nonexist(storage_socket: SQLAlchemySocket):
    mod_role = RoleInfo(
        rolename="doesntexist",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.get("doesntexist")

    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.modify(mod_role)

    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.delete("doesntexist")


def test_role_socket_no_modify_admin(storage_socket: SQLAlchemySocket):
    # The admin role should not be modifiable
    mod_role = RoleInfo(
        rolename="admin",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    with pytest.raises(UserManagementError, match=r"Cannot modify the admin role"):
        storage_socket.roles.modify(mod_role)


def test_role_socket_no_delete_admin(storage_socket: SQLAlchemySocket):
    # The admin role should not be deleteable

    with pytest.raises(UserManagementError, match=r"Cannot delete the admin role"):
        storage_socket.roles.delete("admin")


def test_role_socket_modify(storage_socket: SQLAlchemySocket):
    mod_role = RoleInfo(
        rolename="read",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    storage_socket.roles.modify(mod_role)

    rinfo = storage_socket.roles.get("read")
    assert rinfo["permissions"] == mod_role.permissions.dict()


def test_role_socket_reset(storage_socket: SQLAlchemySocket):
    # Test resetting the roles to their defaults

    # Modify something
    mod_role = RoleInfo(
        rolename="read",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                {"Effect": "Allow", "Action": "*", "Resource": ["something_else"]},
            ]
        },
    )

    storage_socket.roles.modify(mod_role)
    rinfo = storage_socket.roles.get("read")
    assert rinfo["permissions"] == mod_role.permissions.dict()

    # Also delete something
    storage_socket.roles.delete("monitor")
    with pytest.raises(UserManagementError, match=r"Role.*does not exist"):
        storage_socket.roles.get("monitor")

    # Now reset
    storage_socket.roles.reset_defaults()

    # Was the deleted role recreated, and the permissions fixed?
    for rolename, permissions in default_roles.items():
        rinfo = storage_socket.roles.get(rolename)
        assert rinfo["permissions"] == default_roles[rolename]


def test_role_socket_use_invalid_rolename(storage_socket: SQLAlchemySocket):
    # Normally, RoleInfo prevents bad rolenames. But the socket also checks, as a last resort
    # So we have to bypass the RoleInfo check with construct()

    for rolename in invalid_rolenames:
        new_role = RoleInfo.construct(
            rolename=rolename,
            permissions={
                "Statement": [
                    {"Effect": "Allow", "Action": "GET", "Resource": "something"},
                ]
            },
        )

        with pytest.raises(InvalidRolenameError):
            storage_socket.roles.add(new_role)

        with pytest.raises(InvalidRolenameError):
            storage_socket.roles.get(rolename)

        with pytest.raises(InvalidRolenameError):
            storage_socket.roles.modify(new_role)
