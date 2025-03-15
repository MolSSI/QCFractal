import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.testing_helpers import mname1
from qcportal import PortalRequestError
from qcportal.auth import RoleInfo, UserInfo
from qcportal.molecules import Molecule


@pytest.fixture(scope="module")
def module_authtest_snowflake(postgres_server, pytestconfig):
    """
    A QCFractal snowflake server for testing authorization
    """

    pg_harness = postgres_server.get_new_harness("module_authtest_snowflake")
    encoding = pytestconfig.getoption("--client-encoding")
    with QCATestingSnowflake(
        pg_harness,
        encoding,
        create_users=False,
        enable_security=True,
        allow_unauthenticated_read=False,
    ) as snowflake:
        pg_harness.create_template()
        yield snowflake


@pytest.fixture(scope="function")
def authtest_snowflake(module_authtest_snowflake):
    """
    A QCFractal snowflake used for testing (function-scoped)

    The underlying database is deleted and recreated after each test function
    """

    yield module_authtest_snowflake
    module_authtest_snowflake.reset()


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


def test_auth_default_role_read(secure_snowflake):
    client = secure_snowflake.client("read_user", password=test_users["read_user"]["pw"])

    test_mol = Molecule(symbols=["h"], geometry=[0, 0, 0])

    # Read from molecules and records
    client.query_molecules()
    client.query_records()

    # Can't add molecules or records
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.add_molecules([test_mol])

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.add_singlepoints(test_mol, "prog", "energy", "b3lyp", "6-31g")

    # Or modify things
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.modify_molecule(0, name="new name")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.modify_records([0], new_compute_tag="new tag")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.reset_records([0])

    # Or delete things
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.delete_molecules([0])

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.delete_records([0])

    # Can't access users and roles
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_users()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_user("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_roles()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_role("read")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user", "new_pw")

    # Can't access error log, server log, etc
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_access_log()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_error_log()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_managers()

    # Can't activate a manager
    mclient = secure_snowflake.manager_client(mname1, "read_user", test_users["read_user"]["pw"])
    with pytest.raises(PortalRequestError, match="Forbidden"):
        mclient.activate("1.23", {"prog": ["1.23"]}, ["*"])

    # Can access own user, and modify it
    uinfo = client.get_user()
    uinfo.fullname = "A new full name"
    client.modify_user(uinfo)
    client.change_user_password()


def test_auth_default_role_submit(secure_snowflake):
    client = secure_snowflake.client("submit_user", password=test_users["submit_user"]["pw"])

    test_mol = Molecule(symbols=["h"], geometry=[0, 0, 0])

    # Read from molecules and records
    client.query_molecules()
    client.query_records()

    # Can add molecules or records
    _, mol_ids = client.add_molecules([test_mol])
    _, rec_ids = client.add_singlepoints(test_mol, "prog", "energy", "b3lyp", "6-31g")

    # and modify things
    client.modify_molecule(mol_ids[0], name="new name")
    client.modify_records(rec_ids[0], new_compute_tag="new tag")
    client.reset_records(rec_ids)

    # and delete things
    client.delete_molecules(mol_ids)
    client.delete_records(rec_ids)

    # Can't access users and roles
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_users()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_user("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_roles()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_role("read")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user", "new_pw")

    # Can't access error log, server log, etc
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_access_log()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_error_log()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_managers()

    # Can't activate a manager
    mclient = secure_snowflake.manager_client(mname1, "submit_user", test_users["submit_user"]["pw"])
    with pytest.raises(PortalRequestError, match="Forbidden"):
        mclient.activate("1.23", {"prog": ["1.23"]}, ["*"])

    # Can access own user, and modify it
    uinfo = client.get_user()
    uinfo.fullname = "A new full name"
    client.modify_user(uinfo)
    client.change_user_password()


def test_auth_default_role_monitor(secure_snowflake):
    client = secure_snowflake.client("monitor_user", password=test_users["monitor_user"]["pw"])

    test_mol = Molecule(symbols=["h"], geometry=[0, 0, 0])

    # Read from molecules and records
    client.query_molecules()
    client.query_records()

    # Can't add molecules or records
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.add_molecules([test_mol])

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.add_singlepoints(test_mol, "prog", "energy", "b3lyp", "6-31g")

    # Or modify things
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.modify_molecule(0, name="new name")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.modify_records([0], new_compute_tag="new tag")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.reset_records([0])

    # Or delete things
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.delete_molecules([0])

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.delete_records([0])

    # Can't access users and roles
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_users()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_user("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_roles()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_role("read")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user", "new_pw")

    # Can access error log, server log, etc
    client.query_access_log()
    client.query_error_log()
    client.query_managers()

    # Can't activate a manager
    mclient = secure_snowflake.manager_client(mname1, "monitor_user", test_users["monitor_user"]["pw"])
    with pytest.raises(PortalRequestError, match="Forbidden"):
        mclient.activate("1.23", {"prog": ["1.23"]}, ["*"])

    # Can access own user, and modify it
    uinfo = client.get_user()
    uinfo.fullname = "A new full name"
    client.modify_user(uinfo)
    client.change_user_password()


def test_auth_default_role_compute(secure_snowflake):
    client = secure_snowflake.client("compute_user", password=test_users["compute_user"]["pw"])

    test_mol = Molecule(symbols=["h"], geometry=[0, 0, 0])

    # Can't read from molecules and records
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_molecules()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_records()

    # Can't add molecules or records
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.add_molecules([test_mol])

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.add_singlepoints(test_mol, "prog", "energy", "b3lyp", "6-31g")

    # Or modify things
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.modify_molecule(0, name="new name")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.modify_records([0], new_compute_tag="new tag")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.reset_records([0])

    # Or delete things
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.delete_molecules([0])

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.delete_records([0])

    # Can't access users and roles
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_users()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_user("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.list_roles()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.get_role("read")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user")

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.change_user_password("admin_user", "new_pw")

    # Can't access error log, server log, etc
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_access_log()

    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_error_log()

    # Can't query managers
    with pytest.raises(PortalRequestError, match="Forbidden"):
        client.query_managers()

    # Can activate a manager
    mclient = secure_snowflake.manager_client(mname1, "compute_user", test_users["compute_user"]["pw"])
    mclient.activate("1.23", {"prog": ["1.23"]}, ["*"])

    # Can access own user, and modify it
    uinfo = client.get_user()
    uinfo.fullname = "A new full name"
    client.modify_user(uinfo)
    client.change_user_password()
