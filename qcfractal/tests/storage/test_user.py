"""
Tests for the user and role subsockets
"""

import pytest
from qcfractal.interface.models import UserInfo
from qcfractal.storage_sockets.sqlalchemy_socket import AuthorizationFailure


def test_user_basic(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    pw = storage_socket.user.add(uinfo, password="oldpw")
    assert storage_socket.user.exists("george")
    assert pw == "oldpw"

    # Raises exception on verification error
    storage_socket.user.verify("george", "oldpw")

    with pytest.raises(AuthorizationFailure, match=r"Incorrect password"):
        storage_socket.user.verify("george", "badpw")

    # Do we get the same data back?
    uinfo2 = storage_socket.user.get("george")
    assert uinfo2 == uinfo

    storage_socket.user.delete("george")
    assert storage_socket.user.exists("george") is False


def test_user_delete(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.user.add(uinfo)

    # Raises exception on error
    storage_socket.user.delete("george")
    assert storage_socket.user.exists("george") == False

    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.delete("george")


def test_user_duplicates(storage_socket):

    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.user.add(uinfo, password="oldpw")

    # Duplicate should result in an exception
    # Note the spaces on the end. These should be stripped
    # And the username should be converted to lowercase
    uinfo2 = UserInfo(username="George  ", role="read", enabled=True)
    with pytest.raises(AuthorizationFailure, match=r"User.*already exists"):
        storage_socket.user.add(uinfo2, "shortpw")


def test_unknown_user(storage_socket):
    assert storage_socket.user.exists("geoff") is False

    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.get("geoff")

    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.verify("geoff", "a password")

    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.reset_password("geoff")

    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.change_password("geoff", "a password")

    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.delete("geoff")

    uinfo = UserInfo(username="geoff", role="read", fullname="Test user", email="george@example.com", enabled=True)
    with pytest.raises(AuthorizationFailure, match=r"User.*not found"):
        storage_socket.user.modify(uinfo, False)


def test_user_change_password(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    old_pw = storage_socket.user.add(uinfo, "oldpw")
    assert old_pw == "oldpw"

    storage_socket.user.verify("george", "oldpw")

    # update password...
    storage_socket.user.change_password("george", password="newpw")

    # Raises exception on failure
    storage_socket.user.verify("george", "newpw")

    with pytest.raises(AuthorizationFailure, match=r"Incorrect password"):
        storage_socket.user.verify("george", "oldpw")


def test_user_password_generation(storage_socket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    gen_pw = storage_socket.user.add(uinfo)
    storage_socket.user.verify("george", gen_pw)

    ## update password...
    gen_pw_2 = storage_socket.user.reset_password("george")
    storage_socket.user.verify("george", gen_pw_2)

    with pytest.raises(AuthorizationFailure, match=r"Incorrect password"):
        storage_socket.user.verify("george", gen_pw)


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
    gen_pw = storage_socket.user.add(uinfo)

    uinfo2 = UserInfo(username="george", role="admin", fullname="Test user 2", email="test@example.com", enabled=False)
    storage_socket.user.modify(uinfo2, True)

    assert storage_socket.user.get("george") == uinfo2


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
    storage_socket.user.add(uinfo)

    uinfo2 = UserInfo(
        username="george",
        role="admin",
        enabled=False,
        fullname="Test user 2",
        email="george2@example.com",
        organization="My Other Org",
    )
    storage_socket.user.modify(uinfo2, False)

    db_user = storage_socket.user.get("george")
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
    gen_pw = storage_socket.user.add(uinfo)

    user_perms = storage_socket.user.get_permissions("george")

    # Also can get permissions from user.verify
    assert user_perms == storage_socket.user.verify("george", gen_pw)

    # Now get the permissions from the role socket
    role_model = storage_socket.role.get(role)
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
        storage_socket.user.add(uinfo)
        all_users.append(uinfo)

    user_lst = storage_socket.user.list()

    # Sort both lists by username
    all_users = sorted(all_users, key=lambda x: x.username)
    user_lst = sorted(user_lst, key=lambda x: x.username)
    assert all_users == user_lst


def test_user_delete_inuse(storage_socket):
    # TODO
    pass
