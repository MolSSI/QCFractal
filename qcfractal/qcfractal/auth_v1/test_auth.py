import pytest

from qcarchivetesting import valid_encodings
from qcfractal.testing_helpers import TestingSnowflake
from qcportal import PortalRequestError
from qcportal.molecules import Molecule
from qcportal.permissions import RoleInfo, UserInfo


@pytest.fixture(scope="function", params=valid_encodings)
def authtest_snowflake(temporary_database, request):
    """
    A QCFractal snowflake server for testing authorization
    """

    db_config = temporary_database.config
    with TestingSnowflake(
        db_config,
        encoding=request.param,
        start_flask=True,
        create_users=False,
        enable_security=True,
        allow_unauthenticated_read=False,
    ) as server:
        yield server


def test_auth_allow_deny_read(authtest_snowflake):

    # add a test role. Can only access molecule
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/molecules"]},
            ]
        },
    )

    socket = authtest_snowflake.get_storage_socket()
    socket.roles.add(rinfo)

    uinfo = UserInfo(username="test_user", enabled=True, role="test_role")
    pw = socket.users.add(uinfo)

    client = authtest_snowflake.client(username="test_user", password=pw)
    client.query_molecules()

    ####################################################
    # Explicit deny
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
                {"Effect": "Deny", "Action": "READ", "Resource": ["/api/v1/molecules"]},
            ]
        },
    )

    socket.roles.modify(rinfo)
    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.query_molecules()

    ####################################################
    # Implicit deny
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
            ]
        },
    )

    socket.roles.modify(rinfo)
    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.query_molecules()


def test_auth_allow_deny_write(authtest_snowflake):
    test_mol = Molecule(symbols=["h"], geometry=[0, 0, 0])

    # add a test role. Can only access molecule
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
                {"Effect": "Allow", "Action": "WRITE", "Resource": ["/api/v1/molecules"]},
            ]
        },
    )

    socket = authtest_snowflake.get_storage_socket()
    socket.roles.add(rinfo)

    uinfo = UserInfo(username="test_user", enabled=True, role="test_role")
    pw = socket.users.add(uinfo)

    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.query_molecules()

    client.add_molecules([test_mol])

    ####################################################
    # Explicit deny
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
                {"Effect": "Deny", "Action": "WRITE", "Resource": ["/api/v1/molecules"]},
            ]
        },
    )

    socket.roles.modify(rinfo)
    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.add_molecules([test_mol])

    ####################################################
    # Implicit deny
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
            ]
        },
    )

    socket.roles.modify(rinfo)
    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.add_molecules([test_mol])


def test_auth_allow_deny_delete(authtest_snowflake):

    # add a test role. Can only access molecule
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
                {"Effect": "Allow", "Action": "DELETE", "Resource": ["/api/v1/molecules"]},
            ]
        },
    )

    socket = authtest_snowflake.get_storage_socket()
    socket.roles.add(rinfo)

    uinfo = UserInfo(username="test_user", enabled=True, role="test_role")
    pw = socket.users.add(uinfo)

    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.query_molecules()

    client.delete_molecules([1])

    ####################################################
    # Explicit deny
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
                {"Effect": "Deny", "Action": "DELETE", "Resource": ["/api/v1/molecules"]},
            ]
        },
    )

    socket.roles.modify(rinfo)
    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.delete_molecules([1])

    ####################################################
    # Implicit deny
    rinfo = RoleInfo(
        rolename="test_role",
        permissions={
            "Statement": [
                {"Effect": "Allow", "Action": "READ", "Resource": ["/api/v1/information"]},
            ]
        },
    )

    socket.roles.modify(rinfo)
    client = authtest_snowflake.client(username="test_user", password=pw)

    with pytest.raises(PortalRequestError, match=r"not authorized to access"):
        client.delete_molecules([1])
