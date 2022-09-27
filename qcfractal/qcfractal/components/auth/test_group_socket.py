from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.auth.models import GroupInfo
from qcportal.exceptions import (
    UserManagementError,
    InvalidGroupnameError,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

invalid_groupnames = ["\x00", "ab\x00cd", "a user", ""]


def test_group_socket_add_get(storage_socket: SQLAlchemySocket):
    ginfo = GroupInfo(
        groupname="group1",
        description="Test group 1",
    )
    storage_socket.groups.add(ginfo)

    # Do we get the same data back?
    # The initial userinfo doesn't contain the id
    ginfo2 = storage_socket.groups.get("group1")
    ginfo2.pop("id")
    assert GroupInfo(**ginfo2) == ginfo

    # Get by id
    ginfo2 = storage_socket.groups.get("group1")
    ginfo3 = storage_socket.groups.get(ginfo2["id"])
    assert ginfo2 == ginfo3


def test_group_socket_add_duplicate(storage_socket: SQLAlchemySocket):
    ginfo = GroupInfo(
        groupname="group1",
        description="Test group 1",
    )
    storage_socket.groups.add(ginfo)

    # Duplicate should result in an exception
    ginfo2 = GroupInfo(
        groupname="group1",
        description="Another Test group 1",
    )
    with pytest.raises(UserManagementError, match=r"Group.*already exists"):
        storage_socket.groups.add(ginfo2)


def test_group_socket_add_with_id(storage_socket: SQLAlchemySocket):
    # Should not be able to add a group with the id set
    ginfo = GroupInfo(
        id=1234,
        groupname="group1",
        description="Test group 1",
    )

    with pytest.raises(UserManagementError, match=r"id was given as part"):
        storage_socket.groups.add(ginfo)


def test_group_socket_list(storage_socket: SQLAlchemySocket):
    all_groups = []
    for i in range(20):
        ginfo = GroupInfo(groupname=f"group_{i}", description=f"Test group {i}")
        storage_socket.groups.add(ginfo)
        all_groups.append(ginfo)

    group_lst = storage_socket.groups.list()
    group_lst_model = [GroupInfo(**x) for x in group_lst]

    # Sort both lists by username
    all_groups = sorted(all_groups, key=lambda x: x.groupname)
    group_lst_model = sorted(group_lst_model, key=lambda x: x.groupname)

    d1 = [x.dict(exclude={"id"}) for x in all_groups]
    d2 = [x.dict(exclude={"id"}) for x in group_lst_model]
    assert d1 == d2


def test_group_socket_delete(storage_socket: SQLAlchemySocket):
    ginfo1 = GroupInfo(
        groupname="group1",
        description="Test group 1",
    )
    ginfo2 = GroupInfo(
        groupname="group2",
        description="Test group 1",
    )
    storage_socket.groups.add(ginfo1)
    storage_socket.groups.add(ginfo2)

    gid1 = storage_socket.groups.get("group1")["id"]
    gid2 = storage_socket.groups.get("group2")["id"]

    # Raises exception on error
    storage_socket.groups.delete("group1")
    storage_socket.groups.delete(gid2)

    with pytest.raises(UserManagementError, match=r"Group.*does not exist"):
        storage_socket.groups.get("group1")
    with pytest.raises(UserManagementError, match=r"Group.*does not exist"):
        storage_socket.groups.get(gid1)
    with pytest.raises(UserManagementError, match=r"Group.*does not exist"):
        storage_socket.groups.get("group2")
    with pytest.raises(UserManagementError, match=r"Group.*does not exist"):
        storage_socket.groups.get(gid2)


def test_group_socket_use_invalid_groupname(storage_socket: SQLAlchemySocket):
    # TESTING INVALID GROUP NAMES #

    for groupname in invalid_groupnames:
        # Normally, GroupInfo prevents bad usernames. But the socket also checks, as a last resort
        # So we have to bypass the GroupInfo check with construct()
        ginfo = GroupInfo.construct(
            groupname=groupname,
            description="Test group 1",
        )

        with pytest.raises(InvalidGroupnameError):
            storage_socket.groups.add(ginfo)

        with pytest.raises(InvalidGroupnameError):
            storage_socket.groups.get(groupname)

        with pytest.raises(InvalidGroupnameError):
            storage_socket.groups.delete(groupname)
