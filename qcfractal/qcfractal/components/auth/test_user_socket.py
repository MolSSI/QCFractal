from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.auth.models import UserInfo, GroupInfo, is_valid_password
from qcportal.exceptions import (
    UserManagementError,
    AuthenticationFailure,
    InvalidPasswordError,
    InvalidUsernameError,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

invalid_usernames = ["\x00", "ab\x00cd", "a user", ""]
invalid_passwords = ["\x00", "abcd\x00efgh", "abcd", "1", ""]


def test_user_socket_add_get(storage_socket: SQLAlchemySocket):
    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.groups.add(GroupInfo(groupname="group2"))

    uinfo = UserInfo(
        username="george",
        role="read",
        groups=["group1", "group2"],
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

    # Get by id
    uinfo2 = storage_socket.users.get("george")
    uinfo3 = storage_socket.users.get(uinfo2["id"])
    assert uinfo2 == uinfo3


def test_user_socket_assert_group_membership(storage_socket: SQLAlchemySocket):
    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.groups.add(GroupInfo(groupname="group2"))
    storage_socket.groups.add(GroupInfo(groupname="group3"))

    uinfo = UserInfo(
        username="george",
        role="read",
        groups=["group1", "group2"],
        enabled=True,
    )

    storage_socket.users.add(uinfo)

    # The initial userinfo doesn't contain the id
    uinfo2 = storage_socket.users.get("george")
    uid = uinfo2["id"]

    # Test assert_group_member
    group1_id = storage_socket.groups.get("group1")["id"]
    group2_id = storage_socket.groups.get("group2")["id"]
    group3_id = storage_socket.groups.get("group3")["id"]

    storage_socket.users.assert_group_member(uid, group1_id)
    storage_socket.users.assert_group_member(uid, group2_id)

    with pytest.raises(AuthenticationFailure, match="does not belong to group"):
        storage_socket.users.assert_group_member(uid, group3_id)

    storage_socket.users.assert_group_member(None, None)
    storage_socket.users.assert_group_member(uid, None)

    with pytest.raises(AuthenticationFailure, match="cannot belong to group"):
        storage_socket.users.assert_group_member(None, group3_id)


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
    uinfo1 = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    uinfo2 = UserInfo(
        username="bill",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo1)
    storage_socket.users.add(uinfo2)

    uid1 = storage_socket.users.get("george")["id"]
    uid2 = storage_socket.users.get("bill")["id"]

    # Raises exception on error
    storage_socket.users.delete("george")
    storage_socket.users.delete(uid2)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("george")
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get(uid1)
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("bill")
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get(uid2)


def test_user_socket_use_unknown_user(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("geoff")

    with pytest.raises(AuthenticationFailure, match=r"Incorrect username or password"):
        storage_socket.users.authenticate("geoff", "a password")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        uinfo = UserInfo(id=1234, username="geoff", role="read", enabled=True)
        storage_socket.users.modify(uinfo, False)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password("geoff", "a password")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password("geoff", None)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.delete("geoff")


def test_user_socket_verify_password(storage_socket: SQLAlchemySocket):
    for idx, password in enumerate(["simple", "ABC 1234", "ÃØ©þꝎꟇ"]):
        username = f"george_{idx}"
        uinfo = UserInfo(
            username=username,
            role="read",
            enabled=True,
        )

        add_pw = storage_socket.users.add(uinfo, password=password)
        assert add_pw == password
        storage_socket.users.authenticate(username, add_pw)

        for guess in ["Simple", "ABC%1234", "ÃØ©þꝎB"]:
            with pytest.raises(AuthenticationFailure):
                storage_socket.users.authenticate(username, guess)


def test_user_socket_verify_user_disabled(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)

    uinfo2 = storage_socket.users.authenticate("george", gen_pw)

    uinfo2.enabled = False
    storage_socket.users.modify(uinfo2, as_admin=True)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("george", gen_pw)


def test_user_socket_change_password(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    old_pw = storage_socket.users.add(uinfo, "oldpw123")
    assert old_pw == "oldpw123"

    storage_socket.users.authenticate("george", "oldpw123")

    # update password...
    storage_socket.users.change_password("george", password="newpw123")

    # Raises exception on failure
    storage_socket.users.authenticate("george", "newpw123")

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("george", "oldpw123")


def test_user_socket_password_generation(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)
    storage_socket.users.authenticate("george", gen_pw)
    is_valid_password(gen_pw)
    storage_socket.users.authenticate("george", gen_pw)

    gen_pw_2 = storage_socket.users.change_password("george", None)
    storage_socket.users.authenticate("george", gen_pw_2)
    is_valid_password(gen_pw)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("george", gen_pw)


@pytest.mark.parametrize("as_admin", [True, False])
def test_user_socket_no_modify_username(storage_socket: SQLAlchemySocket, as_admin: bool):
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
        id=uid, username="george2", role="admin", fullname="Test user 2", email="test@example.com", enabled=True
    )

    with pytest.raises(UserManagementError, match=r"Cannot change"):
        uinfo3 = storage_socket.users.modify(uinfo2, as_admin=as_admin)


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


def test_user_socket_use_invalid_username(storage_socket: SQLAlchemySocket):
    for username in invalid_usernames:
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
            storage_socket.users.authenticate(username, "a_password")

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.change_password(username, "a_password")

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.change_password(username, None)

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.delete(username)


def test_user_socket_use_invalid_password(storage_socket: SQLAlchemySocket):
    for idx, password in enumerate(invalid_passwords):
        username = f"george_{idx}"

        uinfo = UserInfo.construct(
            username=username,
            role="read",
            enabled=True,
        )
        with pytest.raises(InvalidPasswordError):
            storage_socket.users.add(uinfo, password)

        #  Add for real now
        storage_socket.users.add(uinfo, "good_password")
        uid = storage_socket.users.get(username)["id"]

        with pytest.raises(InvalidPasswordError):
            storage_socket.users.change_password(username, password)

        with pytest.raises(InvalidPasswordError):
            storage_socket.users.change_password(uid, password)

        with pytest.raises(InvalidPasswordError):
            storage_socket.users.authenticate(username, password)
