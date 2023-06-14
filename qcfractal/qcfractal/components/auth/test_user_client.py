from typing import Optional

import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcportal import PortalRequestError
from qcportal.auth import UserInfo
from qcportal.exceptions import (
    InvalidUsernameError,
    InvalidPasswordError,
    AuthenticationFailure,
)
from .test_group_socket import invalid_groupnames
from .test_role_socket import invalid_rolenames
from .test_user_socket import invalid_usernames, invalid_passwords


def test_user_client_list(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    users = client.list_users()

    assert len(users) == len(test_users)

    for u in users:
        assert u.username in test_users

        tu = test_users[u.username]["info"]
        assert u.role == tu["role"]
        assert u.fullname == tu["fullname"]
        assert u.organization == tu["organization"]
        assert u.email == tu["email"]


def test_user_client_get(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for username, uinfo in test_users.items():
        u = client.get_user(username)

        assert u.username == username
        assert u.role == uinfo["info"]["role"]

        u2 = client.get_user(u.id)
        assert u2 == u


def test_user_client_get_me(secure_snowflake: QCATestingSnowflake):
    for username, uinfo in test_users.items():
        client = secure_snowflake.client(username, uinfo["pw"])
        me = client.get_user()

        assert me.username == username
        assert me.role == uinfo["info"]["role"]


def test_user_client_add(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = UserInfo(
        username="george",
        role="compute",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )

    pw = client.add_user(uinfo)

    secure_snowflake.client("george", pw)


def test_user_client_add_existing(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = UserInfo(
        username="read_user",
        role="compute",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )

    with pytest.raises(PortalRequestError, match=r"already exists"):
        client.add_user(uinfo)


def test_user_client_add_badrole(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = UserInfo(
        username="read_user",
        role="does_not_exist",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )

    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.add_user(uinfo)


def test_user_client_add_badgroup(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = UserInfo(
        username="read_user_2",
        role="read",
        groups=["group_does_not_exist"],
        enabled=True,
    )

    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.add_user(uinfo)


def test_user_client_use_nonexist(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = client.get_user()
    uinfo.__dict__["id"] = 1234  # bypass pydantic validation

    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.get_user("no_user")
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.modify_user(uinfo)
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.change_user_password("no_user", "abcde1234")
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.delete_user("no_user")


def test_user_client_use_invalid_username(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for username in invalid_usernames:
        uinfo = client.get_user()
        uinfo.__dict__["username"] = username  # bypass pydantic validation
        uinfo.__dict__["id"] = None

        with pytest.raises(InvalidUsernameError):
            client.get_user(username)
        with pytest.raises(PortalRequestError, match=r"Username"):
            client.add_user(uinfo)
        with pytest.raises(PortalRequestError, match=r"Username"):
            client.modify_user(uinfo)
        with pytest.raises(InvalidUsernameError):
            client.change_user_password(username, "abcde1234")
        with pytest.raises(InvalidUsernameError):
            client.delete_user(username)


def test_user_client_use_invalid_rolename(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for rolename in invalid_rolenames:
        uinfo = client.get_user()
        uinfo.__dict__["role"] = rolename  # bypass pydantic validation
        uinfo.__dict__["id"] = None

        with pytest.raises(PortalRequestError, match=r"Rolename"):
            client.add_user(uinfo)
        with pytest.raises(PortalRequestError, match=r"Rolename"):
            client.modify_user(uinfo)


def test_user_client_use_invalid_groupname(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for groupname in invalid_groupnames:
        uinfo = client.get_user()
        uinfo.__dict__["groups"] = [groupname]  # bypass pydantic validation
        uinfo.__dict__["id"] = None

        with pytest.raises(PortalRequestError, match=r"Groupname"):
            client.add_user(uinfo)
        with pytest.raises(PortalRequestError, match=r"Groupname"):
            client.modify_user(uinfo)


def test_user_client_use_invalid_password(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for password in invalid_passwords:
        uinfo = client.get_user()
        uinfo.__dict__["username"] = "new_user"  # bypass pydantic validation

        with pytest.raises(InvalidPasswordError):
            client.add_user(uinfo, password)
        with pytest.raises(InvalidPasswordError):
            client.change_user_password(None, password)


def test_user_client_delete(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    client.delete_user("read_user")

    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.get_user("read_user")


def test_user_client_delete_self(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    with pytest.raises(PortalRequestError, match=r"Cannot delete your own user"):
        client.delete_user("admin_user")


def test_user_client_modify(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    uinfo = client.get_user("read_user")

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.role = "submit"
    uinfo.groups = ["group3"]
    uinfo.enabled = False

    uinfo2 = client.modify_user(uinfo)

    uinfo3 = client.get_user("read_user")
    assert uinfo2 == uinfo
    assert uinfo3 == uinfo

    assert uinfo3.fullname == "New Full Name"
    assert uinfo3.organization == "New Organization"
    assert uinfo3.email == "New Email"
    assert uinfo3.role == "submit"
    assert uinfo3.enabled is False
    assert uinfo3.groups == ["group3"]


def test_user_client_modify_self(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("read_user", test_users["read_user"]["pw"])
    uinfo = client.get_user("read_user")

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.role = "submit"
    uinfo.groups = ["group3"]
    uinfo.enabled = False

    uinfo2 = client.modify_user(uinfo)

    uinfo3 = client.get_user("read_user")
    assert uinfo2 == uinfo3

    # Only some fields changed
    assert uinfo3.fullname == "New Full Name"
    assert uinfo3.organization == "New Organization"
    assert uinfo3.email == "New Email"

    # Did not change
    assert uinfo3.role == "read"
    assert uinfo3.enabled is True
    assert uinfo3.groups == ["group2"]


def test_user_client_modify_badrole(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    uinfo = client.get_user("read_user")

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.role = "bad_role"
    uinfo.enabled = False

    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.modify_user(uinfo)


def test_user_client_modify_badgroup(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    uinfo = client.get_user("read_user")

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.enabled = False
    uinfo.groups = ["does_not_exist"]

    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.modify_user(uinfo)


def test_user_client_change_password(secure_snowflake: QCATestingSnowflake):
    # First, make sure read user is denied
    with pytest.raises(AuthenticationFailure):
        secure_snowflake.client("read_user", "a_new_password1234")

    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    new_pw = client.change_user_password("read_user", "a_new_password1234")

    # Change password returns the same password
    assert new_pw == "a_new_password1234"

    # Now we can login
    secure_snowflake.client("read_user", "a_new_password1234")


def test_user_client_change_password_self(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("read_user", test_users["read_user"]["pw"])

    new_pw = client.change_user_password(None, "a_new_password1234")
    assert new_pw == "a_new_password1234"
    secure_snowflake.client("read_user", "a_new_password1234")

    # Reset password
    new_pw = client.change_user_password()
    secure_snowflake.client("read_user", new_pw)


#########################################################################
# Test some security issues
#
# Most of these tests are somewhat duplicated elsewhere, however since
# users/roles are sensitive, it makes sense to make double sure users
# can't do certain things (like modify other users)
#########################################################################


def test_user_client_secure_endpoints_disabled(snowflake_client):
    """
    Some secure endpoints are disabled when security is disabled
    """

    uinfo = UserInfo(
        username="george",
        role="compute",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )

    with pytest.raises(PortalRequestError, match=r"not available if security is not enabled"):
        snowflake_client.add_user(uinfo)

    with pytest.raises(PortalRequestError, match=r"not available if security is not enabled"):
        snowflake_client.delete_user("george")

    with pytest.raises(PortalRequestError, match=r"not available if security is not enabled"):
        snowflake_client.get_user("george")

    with pytest.raises(PortalRequestError, match=r"not available if security is not enabled"):
        snowflake_client.change_user_password("george")

    with pytest.raises(PortalRequestError, match=r"not available if security is not enabled"):
        snowflake_client.list_users()

    with pytest.raises(PortalRequestError, match=r"not available if security is not enabled"):
        snowflake_client.modify_user(uinfo)


def test_user_client_no_other(secure_snowflake: QCATestingSnowflake):
    #
    # a non-admin user cannot access/modify other users
    #

    # For trying some sneaky stuff
    admin_client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    read_info = admin_client.get_user("read_user")

    client = secure_snowflake.client("submit_user", test_users["submit_user"]["pw"])
    my_uinfo = client.get_user()

    with pytest.raises(PortalRequestError, match=r"Admin access is required"):
        client.list_users()

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.get_user("read_user")

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.make_request("get", f"api/v1/users/read_user", UserInfo)

    uinfo = UserInfo(id=read_info.id, username="read_user", role="read", fullname="New Full Name", enabled=True)

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.modify_user(uinfo)

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.make_request("patch", f"api/v1/users", UserInfo, body=uinfo)

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.change_user_password("read_user", "new_password")

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.make_request("put", f"api/v1/users/read_user/password", str, body="new_password")

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.change_user_password("read_user", None)

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.make_request("put", f"api/v1/users/read_user/password", str, body_model=Optional[str], body=None)


def test_user_client_no_anonymous(secure_snowflake_allow_read: QCATestingSnowflake):
    client = secure_snowflake_allow_read.client()

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.list_users()

    with pytest.raises(RuntimeError, match=r"not logged in"):
        client.get_user()

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.get_user("read_user")

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.make_request("get", f"api/v1/users/read_user", UserInfo)

    uinfo = UserInfo(username="read_user", role="read", fullname="New Full Name", enabled=True)

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.modify_user(uinfo)

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.make_request("patch", f"api/v1/users", UserInfo, body=uinfo)

    with pytest.raises(RuntimeError, match=r"not logged in"):
        client.change_user_password(None, "new password")

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.change_user_password("read_user", "new_password")

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.make_request("put", f"api/v1/users/read_user/password", str, body="new_password")

    with pytest.raises(RuntimeError, match=r"not logged in"):
        client.change_user_password(None, None)

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.change_user_password("read_user", None)

    with pytest.raises(PortalRequestError, match=r"Login is required"):
        client.make_request("put", f"api/v1/users/read_user/password", str, body_model=Optional[str], body=None)
