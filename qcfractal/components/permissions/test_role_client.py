"""
Tests the client functions related to user management
"""

import pytest

from qcfractal.portal.exceptions import InvalidRolenameError
from qcfractal.portal.client import PortalRequestError
from qcfractal.portal.permissions import RoleInfo, PermissionsPolicy
from qcfractal.testing import TestingSnowflake, _test_users
from .role_socket import default_roles
from .test_role_socket import invalid_rolenames


def test_role_client_list(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])
    roles = client.list_roles()

    assert len(roles) == len(default_roles)

    for r in roles:
        assert r.rolename in default_roles

        assert r.permissions == PermissionsPolicy(**default_roles[r.rolename])


def test_role_client_get(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])

    for r, permissions in default_roles.items():
        u = client.get_role(r)

        assert u.rolename == r
        assert u.permissions == PermissionsPolicy(**permissions)


def test_role_client_add(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])

    rinfo = RoleInfo(
        rolename="test_role",
        permissions={"Statement": [{"Effect": "Allow", "Action": "GET", "Resource": ["user", "role"]}]},
    )

    client.add_role(rinfo)

    r = client.get_role("test_role")
    assert r.permissions == rinfo.permissions


def test_role_client_add_existing(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])

    rinfo = RoleInfo(
        rolename="read", permissions={"Statement": [{"Effect": "Allow", "Action": "GET", "Resource": ["user", "role"]}]}
    )

    with pytest.raises(PortalRequestError, match=r"already exists"):
        client.add_role(rinfo)


def test_role_client_use_nonexist(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])

    rinfo = client.get_role("read")
    rinfo.__dict__["rolename"] = "no_role"  # bypass pydantic validation

    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.get_role("no_role")
    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.modify_role(rinfo)
    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.delete_role("no_role")


@pytest.mark.parametrize("rolename", invalid_rolenames)
def test_role_client_use_invalid(secure_snowflake: TestingSnowflake, rolename: str):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])

    rinfo = client.get_role("read")
    rinfo.__dict__["rolename"] = rolename  # bypass pydantic validation

    with pytest.raises(InvalidRolenameError):
        client.get_role(rolename)
    with pytest.raises(InvalidRolenameError):
        client.add_role(rinfo)
    with pytest.raises(InvalidRolenameError):
        client.modify_role(rinfo)
    with pytest.raises(InvalidRolenameError):
        client.delete_role(rolename)


def test_role_client_delete(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])

    rinfo = RoleInfo(
        rolename="test_role",
        permissions={"Statement": [{"Effect": "Allow", "Action": "GET", "Resource": ["user", "role"]}]},
    )

    client.add_role(rinfo)

    # Should succeed
    client.get_role("test_role")

    client.delete_role("test_role")

    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.get_role("test_role")


def test_role_client_modify(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", _test_users["admin_user"]["pw"])
    rinfo = client.get_role("read")

    rinfo.permissions.Statement.append({"Effect": "Allow", "Action": "PUT", "Resource": ["user", "role"]})

    rinfo2 = client.modify_role(rinfo)

    # update_on_server should have updated the model itself
    rinfo3 = client.get_role("read")
    assert rinfo2 == rinfo
    assert rinfo3 == rinfo
