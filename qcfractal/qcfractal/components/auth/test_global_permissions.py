import pytest

from qcarchivetesting import test_users
from qcfractal.testing_helpers import mname1
from qcportal import PortalRequestError
from qcportal.molecules import Molecule


def test_auth_global_role_read(secure_snowflake):
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


def test_auth_global_role_submit(secure_snowflake):
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

    # And get/query managers
    client.query_managers()

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

    # Can't activate a manager
    mclient = secure_snowflake.manager_client(mname1, "submit_user", test_users["submit_user"]["pw"])
    with pytest.raises(PortalRequestError, match="Forbidden"):
        mclient.activate("1.23", {"prog": ["1.23"]}, ["*"])

    # Can access own user, and modify it
    uinfo = client.get_user()
    uinfo.fullname = "A new full name"
    client.modify_user(uinfo)
    client.change_user_password()


def test_auth_global_role_monitor(secure_snowflake):
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


def test_auth_global_role_compute(secure_snowflake):
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
