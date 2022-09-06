import pytest

from qcarchivetesting import test_users
from qcportal import PortalRequestError
from qcportal.exceptions import (
    InvalidUsernameError,
    InvalidPasswordError,
    InvalidRolenameError,
    AuthenticationFailure,
)
from qcportal.permissions import UserInfo
from .test_role_socket import invalid_rolenames
from .test_user_socket import invalid_usernames, invalid_passwords
from ...testing_helpers import TestingSnowflake


def test_user_client_list(secure_snowflake: TestingSnowflake):
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


def test_user_client_get(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for username, uinfo in test_users.items():
        u = client.get_user(username)

        assert u.username == username
        assert u.role == uinfo["info"]["role"]


def test_user_client_add(secure_snowflake: TestingSnowflake):
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


def test_user_client_add_existing(secure_snowflake: TestingSnowflake):
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


def test_user_client_add_badrole(secure_snowflake: TestingSnowflake):
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


def test_user_client_use_nonexist(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = client.get_user()
    uinfo.__dict__["username"] = "no_user"  # bypass pydantic validation

    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.get_user("no_user")
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.modify_user(uinfo)
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.change_user_password("no_user", "abcde1234")
    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.delete_user("no_user")


@pytest.mark.parametrize("username", invalid_usernames)
def test_user_client_use_invalid_username(secure_snowflake: TestingSnowflake, username: str):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = client.get_user()
    uinfo.__dict__["username"] = username  # bypass pydantic validation

    with pytest.raises(InvalidUsernameError):
        client.get_user(username)
    with pytest.raises(InvalidUsernameError):
        client.add_user(uinfo)
    with pytest.raises(InvalidUsernameError):
        client.modify_user(uinfo)
    with pytest.raises(InvalidUsernameError):
        client.change_user_password(username, "abcde1234")
    with pytest.raises(InvalidUsernameError):
        client.delete_user(username)


@pytest.mark.parametrize("rolename", invalid_rolenames)
def test_user_client_use_invalid_rolename(secure_snowflake: TestingSnowflake, rolename: str):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = client.get_user()
    uinfo.__dict__["role"] = rolename  # bypass pydantic validation

    with pytest.raises(InvalidRolenameError):
        client.add_user(uinfo)
    with pytest.raises(InvalidRolenameError):
        client.modify_user(uinfo)


@pytest.mark.parametrize("password", invalid_passwords)
def test_user_client_use_invalid_password(secure_snowflake: TestingSnowflake, password: str):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    uinfo = client.get_user()
    uinfo.__dict__["username"] = "new_user"  # bypass pydantic validation

    with pytest.raises(InvalidPasswordError):
        client.add_user(uinfo, password)
    with pytest.raises(InvalidPasswordError):
        client.change_user_password(None, password)


def test_user_client_delete(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    client.delete_user("read_user")

    with pytest.raises(PortalRequestError, match=r"User.*not found"):
        client.get_user("read_user")


def test_user_client_delete_self(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    with pytest.raises(RuntimeError, match=r"Cannot delete your own user"):
        client.delete_user("admin_user")


def test_user_client_modify(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    uinfo = client.get_user("read_user")

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.role = "submit"
    uinfo.enabled = False

    uinfo2 = client.modify_user(uinfo)

    # update_on_server should have updated the model itself
    uinfo3 = client.get_user("read_user")
    assert uinfo2 == uinfo
    assert uinfo3 == uinfo

    assert uinfo3.fullname == "New Full Name"
    assert uinfo3.organization == "New Organization"
    assert uinfo3.email == "New Email"
    assert uinfo3.role == "submit"
    assert uinfo3.enabled is False


def test_user_client_modify_badrole(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    uinfo = client.get_user("read_user")

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.role = "bad_role"
    uinfo.enabled = False

    with pytest.raises(PortalRequestError, match=r"Role.*does not exist"):
        client.modify_user(uinfo)


def test_user_client_change_password(secure_snowflake: TestingSnowflake):

    # First, make sure read user is denied
    with pytest.raises(AuthenticationFailure):
        secure_snowflake.client("read_user", "a_new_password1234")

    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    new_pw = client.change_user_password("read_user", "a_new_password1234")

    # Change password returns the same password
    assert new_pw == "a_new_password1234"

    # Now we can login
    secure_snowflake.client("read_user", "a_new_password1234")


def test_user_client_reset_password(secure_snowflake: TestingSnowflake):

    # First, make sure read user is denied
    with pytest.raises(AuthenticationFailure):
        secure_snowflake.client("read_user", "a_new_password1234")

    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    new_pw = client.change_user_password("read_user")

    # Now we can login
    secure_snowflake.client("read_user", new_pw)


def test_user_client_get_me(secure_snowflake: TestingSnowflake):
    for username, uinfo in test_users.items():
        client = secure_snowflake.client(username, uinfo["pw"])
        me = client.get_user()

        assert me.username == username
        assert me.role == uinfo["info"]["role"]


def test_user_client_modify_me(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("read_user", test_users["read_user"]["pw"])
    uinfo = client.get_user()

    uinfo.fullname = "New Full Name"
    uinfo.organization = "New Organization"
    uinfo.email = "New Email"
    uinfo.role = "admin"
    uinfo.enabled = False

    uinfo2 = client.modify_user(uinfo)

    uinfo3 = client.get_user()
    assert uinfo2 == uinfo3

    # Should have only updated certain fields
    assert uinfo2.fullname == "New Full Name"
    assert uinfo2.organization == "New Organization"
    assert uinfo2.email == "New Email"
    assert uinfo2.role == "read"  # unchanged
    assert uinfo2.enabled is True  # unchanged


def test_user_client_change_my_password(secure_snowflake: TestingSnowflake):

    # First, make sure read user is denied
    with pytest.raises(AuthenticationFailure):
        secure_snowflake.client("read_user", "a_new_password1234")

    client = secure_snowflake.client("read_user", test_users["read_user"]["pw"])
    new_pw = client.change_user_password(None, "a_new_password1234")

    # Change password returns the same password
    assert new_pw == "a_new_password1234"

    # Now we can login
    secure_snowflake.client("read_user", "a_new_password1234")


def test_user_client_reset_my_password(secure_snowflake: TestingSnowflake):

    # First, make sure read user is denied
    with pytest.raises(AuthenticationFailure):
        secure_snowflake.client("read_user", "a_new_password1234")

    client = secure_snowflake.client("read_user", test_users["read_user"]["pw"])
    new_pw = client.change_user_password()

    # Now we can login
    secure_snowflake.client("read_user", new_pw)


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


#########################################################################
# Test some security issues
#
# Most of these tests are somewhat duplicated elsewhere, however since
# users/roles are sensitive, it makes sense to make double sure users
# can't do certain things (like modify other users)
#########################################################################


def test_user_no_update_via_me(secure_snowflake: TestingSnowflake):
    client = secure_snowflake.client("read_user", test_users["read_user"]["pw"])
    uinfo = client.get_user()

    # Try to be sneaky
    uinfo.__dict__["username"] = "admin_user"

    with pytest.raises(PortalRequestError, match=r"Trying to update own user"):
        client._auto_request("put", "v1/me", UserInfo, None, UserInfo, uinfo, None)


def test_user_client_no_modify(secure_snowflake_allow_read: TestingSnowflake):
    """
    Cannot modify user if not logged in
    """
    client = secure_snowflake_allow_read.client()

    with pytest.raises(RuntimeError, match=r"not logged in"):
        client.get_user()

    uinfo = UserInfo(username="read_user", role="read", fullname="New Full Name", enabled=True)

    with pytest.raises(PortalRequestError, match=r"Forbidden"):
        client.modify_user(uinfo)

    with pytest.raises(PortalRequestError, match=r"not logged in"):
        client.change_user_password()


def test_user_client_no_login_disabled(secure_snowflake: TestingSnowflake):
    """
    Cannot login if disabled
    """

    # Should be ok
    secure_snowflake.client("read_user", test_users["read_user"]["pw"])

    # Disable the user
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    uinfo = client.get_user("read_user")
    uinfo.enabled = False
    client.modify_user(uinfo)

    # Fails now
    with pytest.raises(AuthenticationFailure, match=r"read_user is disabled"):
        secure_snowflake.client("read_user", test_users["read_user"]["pw"])
