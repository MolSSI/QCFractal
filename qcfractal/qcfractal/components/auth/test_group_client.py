import pytest

from qcarchivetesting import test_users, test_groups
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcportal import PortalRequestError
from qcportal.auth import GroupInfo
from qcportal.exceptions import (
    InvalidGroupnameError,
)
from .test_group_socket import invalid_groupnames


def test_group_client_list(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    groups = client.list_groups()

    assert len(groups) == 3

    for g in groups:
        assert g.groupname in test_groups


def test_group_client_get(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for groupname in test_groups:
        g = client.get_group(groupname)
        assert g.groupname == groupname

        g2 = client.get_group(g.id)
        assert g == g2


def test_group_client_add(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    client.add_group(GroupInfo(groupname="a_group", description="A description"))
    g = client.get_group("a_group")
    assert g.groupname == "a_group"
    assert g.id is not None
    assert g.description == "A description"


def test_group_client_add_with_id(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    with pytest.raises(RuntimeError, match=r"Cannot add group.*contains an id"):
        ginfo = GroupInfo(id=1234, groupname="a_group", description="A description")
        client.add_group(ginfo)


def test_group_client_add_existing(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    with pytest.raises(PortalRequestError, match=r"already exists"):
        ginfo = GroupInfo(groupname="group1", description="A description")
        client.add_group(ginfo)


def test_group_client_use_nonexist(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.get_group("no_group")
    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.get_group(1234)
    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.delete_group("no_group")
    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.delete_group(1234)


def test_group_client_use_invalid_groupname(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    for groupname in invalid_groupnames:
        with pytest.raises(InvalidGroupnameError):
            client.get_group(groupname)
        with pytest.raises(InvalidGroupnameError):
            client.delete_group(groupname)


def test_group_client_delete(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])

    user = client.get_user("admin_user")
    assert "group1" in user.groups
    assert "group2" in user.groups

    gid2 = client.get_group("group2").id

    client.delete_group("group1")
    client.delete_group(gid2)

    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.get_group("group1")
    with pytest.raises(PortalRequestError, match=r"Group.*does not exist"):
        client.get_group(gid2)

    user = client.get_user("admin_user")
    assert "group1" not in user.groups
    assert "group2" not in user.groups
