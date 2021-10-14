"""
Tests for the user and role subsockets
"""

import pytest
from qcfractal.exceptions import UserManagementError, AuthenticationFailure
from qcfractal.portal.models.permissions import UserInfo


def test_user_basic(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    pw = storage_socket.users.add(uinfo, password="oldpw")
    assert storage_socket.users.exists("george")
    assert pw == "oldpw"

    # Raises exception on verification error
    storage_socket.users.verify("george", "oldpw")

    with pytest.raises(AuthenticationFailure, match=r"Incorrect password"):
        storage_socket.users.verify("george", "badpw")

    # Do we get the same data back?
    uinfo2 = storage_socket.users.get("george")
    assert uinfo2 == uinfo

    storage_socket.users.delete("george")
    assert storage_socket.users.exists("george") is False


def test_user_delete(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo)

    # Raises exception on error
    storage_socket.users.delete("george")
    assert storage_socket.users.exists("george") == False

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.delete("george")


def test_user_duplicates(storage_socket):

    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo, password="oldpw")

    # Duplicate should result in an exception
    # Note the spaces on the end. These should be stripped
    # And the username should be converted to lowercase
    uinfo2 = UserInfo(username="George  ", role="read", enabled=True)
    with pytest.raises(UserManagementError, match=r"User.*already exists"):
        storage_socket.users.add(uinfo2, "shortpw")


def test_unknown_user(storage_socket):
    assert storage_socket.users.exists("geoff") is False

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("geoff")

    with pytest.raises(AuthenticationFailure, match=r"User.*not found"):
        storage_socket.users.verify("geoff", "a password")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.reset_password("geoff")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password("geoff", "a password")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.delete("geoff")

    uinfo = UserInfo(username="geoff", role="read", fullname="Test user", email="george@example.com", enabled=True)
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.modify(uinfo, False)


def test_user_change_password(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    old_pw = storage_socket.users.add(uinfo, "oldpw")
    assert old_pw == "oldpw"

    storage_socket.users.verify("george", "oldpw")

    # update password...
    storage_socket.users.change_password("george", password="newpw")

    # Raises exception on failure
    storage_socket.users.verify("george", "newpw")

    with pytest.raises(AuthenticationFailure, match=r"Incorrect password"):
        storage_socket.users.verify("george", "oldpw")


def test_user_password_generation(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    gen_pw = storage_socket.users.add(uinfo)
    storage_socket.users.verify("george", gen_pw)

    ## update password...
    gen_pw_2 = storage_socket.users.reset_password("george")
    storage_socket.users.verify("george", gen_pw_2)

    with pytest.raises(AuthenticationFailure, match=r"Incorrect password"):
        storage_socket.users.verify("george", gen_pw)


def test_user_modify_admin(storage_socket):
    # If as_admin == True for user.modify(), then all fields can be modified
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    gen_pw = storage_socket.users.add(uinfo)

    uinfo2 = UserInfo(username="george", role="admin", fullname="Test user 2", email="test@example.com", enabled=False)
    storage_socket.users.modify(uinfo2, True)

    assert storage_socket.users.get("george") == uinfo2


def test_user_modify_noadmin(storage_socket):
    # If as_admin == False for user.modify(), then some fields won't be modified
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo)

    uinfo2 = UserInfo(
        username="george",
        role="admin",
        enabled=False,
        fullname="Test user 2",
        email="george2@example.com",
        organization="My Other Org",
    )
    storage_socket.users.modify(uinfo2, False)

    db_user = storage_socket.users.get("george")
    assert db_user.enabled == uinfo.enabled
    assert db_user.role == uinfo.role
    assert db_user.fullname == uinfo2.fullname
    assert db_user.email == uinfo2.email
    assert db_user.organization == uinfo2.organization


@pytest.mark.parametrize("role", ["admin", "read", "monitor", "compute"])
def test_user_permissions(storage_socket, role):

    uinfo = UserInfo(
        username="george",
        role=role,
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    gen_pw = storage_socket.users.add(uinfo)

    user_perms = storage_socket.users.get_permissions("george")

    # Also can get permissions from user.verify
    assert user_perms == storage_socket.users.verify("george", gen_pw)

    # Now get the permissions from the role socket
    role_model = storage_socket.roles.get(role)
    assert role_model.permissions == user_perms


def test_user_list(storage_socket):
    all_users = []
    for i in range(20):
        uinfo = UserInfo(
            username=f"george_{i}",
            role="read",
            enabled=bool(i % 2),
            fullname=f"Test user_{i}",
            email=f"george{i}@example.com",
            organization=f"My Org {i}",
        )
        storage_socket.users.add(uinfo)
        all_users.append(uinfo)

    user_lst = storage_socket.users.list()

    # Sort both lists by username
    all_users = sorted(all_users, key=lambda x: x.username)
    user_lst = sorted(user_lst, key=lambda x: x.username)
    assert all_users == user_lst


def test_user_delete_inuse(storage_socket):
    # TODO
    pass
