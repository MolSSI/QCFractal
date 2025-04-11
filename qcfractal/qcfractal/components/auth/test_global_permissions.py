from functools import partial
from typing import Any, Optional

import pytest

from qcarchivetesting import test_users
from qcfractal.testing_helpers import mname1
from qcportal import PortalClient
from qcportal import PortalRequestError
from qcportal.auth import UserInfo, GroupInfo
from qcportal.managers import ManagerActivationBody, ManagerUpdateBody
from qcportal.molecules import Molecule
from qcportal.singlepoint.test_dataset_models import test_entries, test_specs
from qcportal.tasks import TaskClaimBody, TaskReturnBody
from qcportal.utils import now_at_utc

test_mol = Molecule(symbols=["h"], geometry=[0, 0, 0])
test_uinfo = UserInfo(username="test_new_user", role="read", enabled=True)


@pytest.fixture(scope="function")
def roletest_snowflake(secure_snowflake_allow_read):
    # Add a test molecule, record, and dataset
    client = secure_snowflake_allow_read.client("admin_user", password=test_users["admin_user"]["pw"])

    client.add_molecules([test_mol])
    client.add_singlepoints(molecules=[test_mol], program="psi4", driver="energy", method="b3lyp", basis="sto-3g")
    ds = client.add_dataset("singlepoint", "testds")
    ds.add_entries(test_entries)
    ds.add_specification("test_spec", test_specs[0])
    return secure_snowflake_allow_read


# Make functions that only take a portal client as the only argument, then
# Assemble those into a testing map
def submit_dataset(client: PortalClient):
    client.get_dataset("singlepoint", "testds").submit()


def submit_dataset_background(client: PortalClient):
    client.get_dataset("singlepoint", "testds").background_submit()


def delete_dataset_entry(client: PortalClient):
    client.get_dataset("singlepoint", "testds").delete_entries(["testent"])


def delete_dataset_specification(client: PortalClient):
    client.get_dataset("singlepoint", "testds").delete_specification("testspec")


def remove_dataset_records(client: PortalClient):
    client.get_dataset("singlepoint", "testds").remove_records(["testent"], ["testspec"])


def read_dataset(client: PortalClient):
    ds = client.get_dataset("singlepoint", "testds")
    for e, s, r in ds.iterate_records():
        ds.get_entry(e)


def get_dataset_internal_jobs(client: PortalClient):
    client.get_dataset("singlepoint", "testds").list_internal_jobs()


def get_dataset_attachments(client: PortalClient):
    _ = client.get_dataset("singlepoint", "testds").attachments


def modify_dataset_metadata(client: PortalClient):
    client.get_dataset("singlepoint", "testds").set_default_compute_tag("abc")


def create_dataset_view(client: PortalClient):
    client.get_dataset("singlepoint", "testds").create_view("a test view", {})


def activate_manager(client: PortalClient):
    # Go through the route directly
    manager_info = ManagerActivationBody(
        name_data=mname1,
        manager_version="1.23",
        username=client.username,
        programs={"prog": ["1.23"]},
        compute_tags=["*"],
    )

    return client.make_request(
        "post",
        "compute/v1/managers",
        None,
        body=manager_info,
    )


def claim_tasks(client: PortalClient):
    # Go through the route directly
    # Doesn't matter if the manager has been activated - we are looking for "Forbidden"
    body = TaskClaimBody(name_data=mname1, programs={"prog": ["1.23"]}, compute_tags=["*"], limit=1)

    client.make_request(
        "post",
        "compute/v1/tasks/claim",
        Any,
        body=body,
    )


def return_tasks(client: PortalClient):
    # Go through the route directly
    # Doesn't matter if the manager has been activated - we are looking for "Forbidden"

    body = TaskReturnBody(name_data=mname1, results_compressed={})
    client.make_request(
        "post",
        "compute/v1/tasks/return",
        Any,
        body=body,
    )


def manager_modify_heartbeat(client: PortalClient):
    # Go through the route directly
    # Doesn't matter if the manager has been activated - we are looking for "Forbidden"
    body = ManagerUpdateBody(status="active", active_tasks=10, active_cores=20, active_memory=100, total_cpu_hours=1000)

    client.make_request(
        "patch",
        f"compute/v1/managers/{mname1.fullname}",
        Any,
        body=body,
    )


def manager_modify_deactivate(client: PortalClient):
    # Go through the route directly
    # Doesn't matter if the manager has been activated - we are looking for "Forbidden"
    body = ManagerUpdateBody(
        status="inactive", active_tasks=10, active_cores=20, active_memory=100, total_cpu_hours=1000
    )

    client.make_request(
        "patch",
        f"compute/v1/managers/{mname1.fullname}",
        Any,
        body=body,
    )


def access_my_user_me(client: PortalClient):
    # Access own user, via /me
    return client.make_request("get", f"api/v1/me", UserInfo)


def access_my_user(client: PortalClient):
    # Access own user, via /users
    return client.make_request("get", f"api/v1/users/{client.username}", UserInfo)


def get_my_preferences_me(client: PortalClient):
    return client.make_request("get", f"api/v1/me/preferences", Any)


def get_my_preferences(client: PortalClient):
    return client.make_request("get", f"api/v1/users/{client.username}/preferences", Any)


def get_other_preferences(client: PortalClient):
    return client.make_request("get", f"api/v1/users/submit_user_2/preferences", Any)


def get_my_sessions_me(client: PortalClient):
    return client.make_request("get", f"api/v1/me/sessions", Any)


def get_my_sessions(client: PortalClient):
    return client.make_request("get", f"api/v1/users/{client.username}/sessions", Any)


def list_all_sessions(client: PortalClient):
    return client.make_request("get", f"api/v1/sessions", Any)


def get_other_sessions(client: PortalClient):
    return client.make_request("get", f"api/v1/users/submit_user_2", Any)


# fmt: off
test_function_map = {
    "get_information": PortalClient.get_server_information,
    "set_motd": partial(PortalClient.set_motd, new_motd="hi"),

    "get_access_log": [PortalClient.query_access_log,
                       PortalClient.query_access_summary],
    "delete_access_log": partial(PortalClient.delete_access_log, before=now_at_utc()),

    "get_error_log": PortalClient.query_error_log,
    "delete_error_log": partial(PortalClient.delete_error_log, before=now_at_utc()),

    "get_internal_job": [partial(PortalClient.get_internal_job, job_id=1),
                         PortalClient.query_internal_jobs],
    "modify_internal_job": partial(PortalClient.cancel_internal_job, job_id=1),
    "delete_internal_job": partial(PortalClient.delete_internal_job, job_id=1),

    "list_users": PortalClient.list_users,
    "add_user": partial(PortalClient.add_user, user_info=test_uinfo),
    "delete_user": partial(PortalClient.delete_user, username_or_id=1),

    "get_me": [access_my_user,
               access_my_user_me],
    "get_other_user": partial(PortalClient.get_user, username_or_id="submit_user_2"),

    "get_my_preferences": [get_my_preferences_me,
                           get_my_preferences],
    "get_other_preferences": get_other_preferences,

    "get_my_sessions": [get_my_sessions_me,
                        get_my_sessions],
    "list_all_sessions": list_all_sessions,
    "get_other_sessions": get_other_sessions,

    "list_groups": PortalClient.list_groups,
    "add_group": partial(PortalClient.add_group, group_info=GroupInfo(groupname="testgroup")),
    "get_group": partial(PortalClient.get_group, groupname_or_id="group1"),
    "delete_group": partial(PortalClient.delete_group, groupname_or_id="group1"),

    "add_molecule": partial(PortalClient.add_molecules, molecules=[test_mol]),
    "get_molecule": partial(PortalClient.get_molecules, molecule_ids=[1]),
    "query_molecule": PortalClient.query_molecules,
    "modify_molecule": partial(PortalClient.modify_molecule, molecule_id=1, name="new_name"),
    "delete_molecule": partial(PortalClient.delete_molecules, molecule_ids=[1]),

    "add_record": partial(PortalClient.add_singlepoints, molecules=[test_mol],  program='psi4', driver='energy', method='b3lyp', basis='sto-3g'),
    "get_record": partial(PortalClient.get_records, record_ids=[1], missing_ok=True),
    "query_record": PortalClient.query_records,
    "modify_record": [partial(PortalClient.modify_records, record_ids=[1], new_compute_tag="new_tag"),
                      partial(PortalClient.cancel_records, record_ids=[1])],
    "delete_record": partial(PortalClient.delete_records, record_ids=[1]),

    "list_datasets": PortalClient.list_datasets,
    "add_dataset": partial(PortalClient.add_dataset, dataset_type="singlepoint", name="newdataset"),
    "get_dataset": [partial(PortalClient.get_dataset_by_id, dataset_id=1),
                    partial(PortalClient.get_dataset, dataset_type="singlepoint", dataset_name="testds")],
    "read_dataset": [read_dataset,
                     get_dataset_internal_jobs,
                     get_dataset_attachments],
    "submit_dataset": [submit_dataset,
                       submit_dataset_background],
    "modify_dataset": [delete_dataset_entry,
                       delete_dataset_specification,
                       remove_dataset_records,
                       modify_dataset_metadata],
    "create_dataset_view": create_dataset_view,
    "delete_dataset": partial(PortalClient.delete_dataset, dataset_id=1, delete_records=False),

    "get_manager": partial(PortalClient.get_managers, names=['testmanager'], missing_ok=True),
    "query_manager": PortalClient.query_managers,

    "activate_manager": activate_manager,
    "modify_manager": [manager_modify_heartbeat, manager_modify_deactivate],
    "claim_tasks": claim_tasks,
    "return_tasks": return_tasks,

}

test_role_permissions_map = {
    "admin": set(test_function_map.keys()),
    "maintain": {"get_information", "set_motd",
                 "get_access_log", "delete_access_log",
                 "get_error_log", "delete_error_log",
                 "get_internal_job", "modify_internal_job", "delete_internal_job",
                 "list_groups", "add_group", "get_group", "delete_group",
                 "add_molecule", "get_molecule", "query_molecule", "modify_molecule", "delete_molecule",
                 "add_record", "get_record", "query_record", "modify_record", "delete_record",
                 "list_datasets", "add_dataset", "get_dataset", "read_dataset", "submit_dataset",
                 "modify_dataset", "delete_dataset", "create_dataset_view",
                 "get_manager", "query_manager",
                 "get_me", "get_my_preferences", "get_my_sessions"},
    "monitor": {"get_information",
                "get_access_log", "get_error_log", "get_internal_job",
                "get_molecule", "query_molecule", "get_record", "query_record", "list_datasets",
                "get_dataset", "read_dataset", "get_manager", "query_manager",
                "get_me", "get_my_preferences", "get_my_sessions"},
    "submit": {"get_information",
               "add_molecule", "get_molecule", "query_molecule", "modify_molecule", "delete_molecule",
               "add_record", "get_record", "query_record", "modify_record", "delete_record",
               "list_datasets", "add_dataset", "get_dataset", "read_dataset", "submit_dataset",
               "modify_dataset", "delete_dataset", "create_dataset_view",
               "get_manager", "query_manager",
               "get_me", "get_my_preferences", "get_my_sessions"},
    "read": {"get_information", "get_molecule", "query_molecule", "get_record", "query_record", "list_datasets",
             "get_dataset", "read_dataset", "get_manager", "query_manager",
             "get_me", "get_my_preferences", "get_my_sessions"},
    "anonymous": {"get_information", "get_molecule", "query_molecule", "get_record", "query_record", "list_datasets",
                  "get_dataset", "read_dataset", "get_manager", "query_manager"},
    "compute": {"get_information", "activate_manager", "modify_manager", "claim_tasks", "return_tasks",
                "get_me", "get_my_preferences", "get_my_sessions"},
}
# fmt: off

@pytest.mark.parametrize(
    "username,role",
    [
        ('admin_user', 'admin'),
        ('maintain_user', 'maintain'),
        ('monitor_user', 'monitor'),
        ('submit_user', 'submit'),
        ('read_user', 'read'),
        (None, 'anonymous'),
        ('compute_user', 'compute')
    ]
)
def test_auth_global_role_permissions(roletest_snowflake, username, role):
    if username is None:
        client = roletest_snowflake.client()
    else:
        client = roletest_snowflake.client(username, password=test_users[username]["pw"])

    role_perms = test_role_permissions_map[role]

    # Make sure our tests are sane - everything in the permissions map is something we test
    assert set(role_perms) <= set(test_function_map.keys())

    for action, f in test_function_map.items():
        allowed = action in role_perms
        print(role, action, allowed)

        if allowed:
            # Wrap in try/except - exceptions are ok, as long as they aren't "Forbidden" errors
            if isinstance(f, list):
                for x in f:
                    try:
                        x(client)
                    except Exception as err:
                        assert "Forbidden" not in str(err)
            else:
                try:
                    f(client)
                except Exception as err:
                    assert "Forbidden" not in str(err)
        else:
            if isinstance(f, list):
                for x in f:
                    with pytest.raises(PortalRequestError, match="Forbidden"):
                        x(client)
            else:
                with pytest.raises(PortalRequestError, match="Forbidden"):
                    f(client)


def test_auth_global_no_unauth_read(secure_snowflake):
    with pytest.raises(PortalRequestError, match="Server requires login"):
        secure_snowflake.client()

    # Fake a client - constructing normally results in trying to get information
    client = secure_snowflake.client("admin_user", password=test_users["admin_user"]["pw"])
    client.username = None
    client._password = None
    client._jwt_access_exp = None
    client._jwt_refresh_exp = None
    client._jwt_refresh_token = None
    client._jwt_access_token = None
    client._req_session.headers.pop("Authorization")

    for action, f in test_function_map.items():
        if isinstance(f, list):
            for x in f:
                with pytest.raises(PortalRequestError, match="Server requires login"):
                    x(client)
        else:
            with pytest.raises(PortalRequestError, match="Server requires login"):
                f(client)

@pytest.mark.parametrize(
    "username,role",
    [
        ('admin_user', 'admin'),
        ('maintain_user', 'maintain'),
        ('monitor_user', 'monitor'),
        ('submit_user', 'submit'),
        ('read_user', 'read'),
        (None, 'anonymous'),
        ('compute_user', 'compute')
    ]
)
def test_auth_global_user_management(secure_snowflake_allow_read, username, role):
    # We need userinfo for a completely different user
    client0 = secure_snowflake_allow_read.client("submit_user_2", password=test_users["submit_user_2"]["pw"])
    other_uinfo = client0.get_user()

    if username is None:
        client = secure_snowflake_allow_read.client()
    else:
        client = secure_snowflake_allow_read.client(username, password=test_users[username]["pw"])

    if username is None:
        # cannot access /me
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("get", f"api/v1/me", UserInfo)
    else:
        client.make_request("get", f"api/v1/me", UserInfo)

    # All can change own password, except if no user
    if username is None:
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("put", f"api/v1/me/password", Any, body="new_password")
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("put", f"api/v1/me/password", Any, body_model=Optional[str], body=None)
    else:
        client.make_request("put", f"api/v1/me/password", Any, body="new_password")
        client.make_request("put", f"api/v1/me/password", Any, body_model=Optional[str], body=None)
        client.make_request("put", f"api/v1/users/{username}/password", Any, body="new_password")
        client.make_request("put", f"api/v1/users/{username}/password", Any, body_model=Optional[str], body=None)

    if role != 'admin':
        # cannot access or modify other users
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("get", f"api/v1/users/submit_user_2", UserInfo)
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("patch", f"api/v1/users", UserInfo, body=other_uinfo)
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("put", f"api/v1/users/submit_user_2/password", Any, body="new_password")
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.make_request("put", f"api/v1/users/submit_user_2/password", str, body_model=Optional[str], body=None)
    else:
        client.make_request("get", f"api/v1/users/submit_user_2", UserInfo)
        client.make_request("patch", f"api/v1/users", UserInfo, body=other_uinfo)
        client.make_request("put", f"api/v1/users/submit_user_2/password", Any, body="new_password")
        client.make_request("put", f"api/v1/users/submit_user_2/password", str, body_model=Optional[str], body=None)

@pytest.mark.parametrize("as_admin", [True, False])
def test_auth_protected_endpoints(snowflake, as_admin):
    # Cannot add or modify users/groups when security is disabled
    uinfo = UserInfo(username='test', role="read", enabled=True)
    ginfo = GroupInfo(groupname="testg")

    snowflake.create_users()

    if as_admin:
        client: PortalClient = snowflake.client(username="admin_user", password=test_users["admin_user"]["pw"])
    else:
        client: PortalClient = snowflake.client()

    # Can't add user
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.add_user(uinfo)

    # Can't get own user or another user
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.make_request("get", f"api/v1/me", UserInfo)

    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.make_request("get", f"api/v1/users/submit_user", UserInfo)

    # Can't modify a user
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.make_request("patch", f"api/v1/users", UserInfo, body=uinfo)

    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.make_request("patch", f"api/v1/me", UserInfo, body=uinfo)

    # Can't change passwords
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.make_request("put", f"api/v1/me/password", Any, body="new_password")
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.make_request("put", f"api/v1/users/submit_user/password", Any, body="new_password")

    # Can't delete users
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.delete_user("submit_user")

    # Can't add or delete groups
    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.add_group(ginfo)

    with pytest.raises(PortalRequestError, match="Cannot access.*security disabled"):
        client.delete_group("group1")
