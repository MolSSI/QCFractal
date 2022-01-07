"""
Tests for the user and role subsockets
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.exceptions import (
    UserManagementError,
    AuthenticationFailure,
    InvalidPasswordError,
    InvalidUsernameError,
)
from qcportal.permissions.models import UserInfo, is_valid_password

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

invalid_usernames = ["\x00", "ab\x00cd", "1234", "a user", ""]
invalid_passwords = ["\x00", "abcd\x00efgh", "abcd", "1", ""]


def test_user_socket_add_get(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    pw = storage_socket.users.add(uinfo, password="oldpw123")
    assert pw == "oldpw123"

    # Do we get the same data back?
    # The initial userinfo doesn't contain the id
    uinfo2 = storage_socket.users.get("george")
    assert "password" not in uinfo2
    uinfo2.pop("id")
    assert UserInfo(**uinfo2) == uinfo


def test_user_socket_add_duplicate(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo, password="oldpw123")

    # Duplicate should result in an exception
    uinfo2 = UserInfo(username="george", role="read", enabled=True)
    with pytest.raises(UserManagementError, match=r"User.*already exists"):
        storage_socket.users.add(uinfo2, "newpw123")


def test_user_socket_add_with_id(storage_socket: SQLAlchemySocket):
    # Should not be able to add a user with the id set
    uinfo = UserInfo(
        id=123,
        username="george",
        role="read",
        enabled=True,
    )

    with pytest.raises(UserManagementError, match=r"id was given as part"):
        storage_socket.users.add(uinfo, password="oldpw123")


def test_user_socket_list(storage_socket: SQLAlchemySocket):
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
    user_lst_model = [UserInfo(**x) for x in user_lst]

    # Sort both lists by username
    all_users = sorted(all_users, key=lambda x: x.username)
    user_lst_model = sorted(user_lst_model, key=lambda x: x.username)

    d1 = [x.dict(exclude={"id"}) for x in all_users]
    d2 = [x.dict(exclude={"id"}) for x in user_lst_model]
    assert d1 == d2


def test_user_socket_delete(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo)

    # Raises exception on error
    storage_socket.users.delete("george")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("george")


@pytest.mark.parametrize("username", ["geoff", "George", "george_"])
def test_user_socket_use_unknown_user(storage_socket: SQLAlchemySocket, username):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get(username)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get_permissions(username)

    with pytest.raises(AuthenticationFailure, match=r"Incorrect username or password"):
        storage_socket.users.verify(username, "a password")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        uinfo = UserInfo(username=username, role="read", enabled=True)
        storage_socket.users.modify(uinfo, False)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password(username, "a password")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password(username, None)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.delete(username)


@pytest.mark.parametrize("password", ["simple", "ABC 1234", "ÃØ©þꝎꟇ"])
@pytest.mark.parametrize("guess", ["Simple", "ABC%1234", "ÃØ©þꝎB"])
def test_user_socket_verify_password(storage_socket: SQLAlchemySocket, password: str, guess: str):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    add_pw = storage_socket.users.add(uinfo, password=password)
    assert add_pw == password
    storage_socket.users.verify("george", add_pw)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.verify("george", guess)


@pytest.mark.parametrize("username", ["simple", "ABC1234", "ÃØ©þꝎꟇ"])
@pytest.mark.parametrize("guess", ["Simple", "simple!", "ABC%1234", "ÃØ©þꝎB"])
def test_user_socket_verify_user(storage_socket: SQLAlchemySocket, username: str, guess: str):
    uinfo = UserInfo(
        username=username,
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)
    storage_socket.users.verify(username, gen_pw)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.verify(guess, gen_pw)


@pytest.mark.parametrize("username", ["simple", "ABC1234", "ÃØ©þꝎꟇ"])
@pytest.mark.parametrize("guess", ["Simple", "simple!", "ABC%1234", "ÃØ©þꝎB"])
def test_user_socket_verify_user_disabled(storage_socket: SQLAlchemySocket, username: str, guess: str):
    uinfo = UserInfo(
        username=username,
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)
    storage_socket.users.verify(username, gen_pw)

    uinfo.enabled = False
    storage_socket.users.modify(uinfo, as_admin=True)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.verify(guess, gen_pw)


def test_user_socket_change_password(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    old_pw = storage_socket.users.add(uinfo, "oldpw123")
    assert old_pw == "oldpw123"

    storage_socket.users.verify("george", "oldpw123")

    # update password...
    storage_socket.users.change_password("george", password="newpw123")

    # Raises exception on failure
    storage_socket.users.verify("george", "newpw123")

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.verify("george", "oldpw123")


def test_user_socket_password_generation(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)
    storage_socket.users.verify("george", gen_pw)
    is_valid_password(gen_pw)
    storage_socket.users.verify("george", gen_pw)

    gen_pw_2 = storage_socket.users.change_password("george", None)
    storage_socket.users.verify("george", gen_pw_2)
    is_valid_password(gen_pw)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.verify("george", gen_pw)


@pytest.mark.parametrize("as_admin", [True, False])
def test_user_socket_modify(storage_socket: SQLAlchemySocket, as_admin: bool):
    # If as_admin == True for user.modify(), then all fields can be modified
    # Otherwise, some fields will always stay the same (enabled, role)

    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=False,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo)
    uid = storage_socket.users.get("george")["id"]

    uinfo2 = UserInfo(
        id=uid, username="george", role="admin", fullname="Test user 2", email="test@example.com", enabled=True
    )

    # Modify should return the same this as get
    uinfo3 = storage_socket.users.modify(uinfo2, as_admin=as_admin)
    uinfo4 = storage_socket.users.get("george")

    assert uinfo3 == UserInfo(**uinfo4)

    if as_admin is True:
        assert uinfo2 == UserInfo(**uinfo3)
    else:
        # Stayed the same
        assert uinfo3["enabled"] == uinfo.enabled
        assert uinfo3["role"] == uinfo.role

        # Can be modified
        assert uinfo3["fullname"] == uinfo2.fullname
        assert uinfo3["email"] == uinfo2.email
        assert uinfo3["organization"] == uinfo2.organization


@pytest.mark.parametrize("role", ["admin", "read", "monitor", "compute"])
def test_user_socket_permissions(storage_socket, role):

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
    role_dict = storage_socket.roles.get(role)
    assert role_dict["permissions"] == user_perms


@pytest.mark.parametrize("username", invalid_usernames)
def test_user_socket_use_invalid_username(storage_socket: SQLAlchemySocket, username: str):
    # TESTING INVALID USERNAMES #

    # Normally, UserInfo prevents bad usernames. But the socket also checks, as a last resort
    # So we have to bypass the UserInfo check with construct()
    uinfo = UserInfo.construct(
        username=username,
        role="read",
        enabled=True,
    )

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.add(uinfo, "password123")

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.get(username)

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.get_permissions(username)

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.verify(username, "a_password")

    with pytest.raises(InvalidUsernameError):
        uinfo = UserInfo.construct(username=username, role="a_role", enabled=True)
        storage_socket.users.modify(uinfo, False)

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.change_password(username, "a_password")

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.change_password(username, None)

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.delete(username)


@pytest.mark.parametrize("password", invalid_passwords)
def test_user_socket_use_invalid_password(storage_socket: SQLAlchemySocket, password: str):
    # TESTING INVALID PASSWORDS #

    uinfo = UserInfo.construct(
        username="george",
        role="read",
        enabled=True,
    )

    with pytest.raises(InvalidPasswordError):
        storage_socket.users.add(uinfo, password)

    #  Add for real now
    storage_socket.users.add(uinfo, "good_password")

    with pytest.raises(InvalidPasswordError):
        storage_socket.users.change_password("george", password)

    with pytest.raises(InvalidPasswordError):
        storage_socket.users.verify("george", password)
