import re
from typing import Any

import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.auth import GLOBAL_ROLE_PERMISSIONS, AuthorizedEnum
from qcportal import PortalRequestError
from qcportal.auth import UserInfo, GroupInfo
from qcportal.exceptions import AuthorizationFailure


def substitute_route_url(route) -> str:
    # Substitute values for various placeholders in flask routes
    route = re.sub(r"<string:[^>]+>", "some_string", route)
    route = re.sub(r"<int:[^>]+>", "1234", route)
    route = route.replace("<username_or_id>", "some_username")
    return route


def test_global_permissions(secure_snowflake_allow_read: QCATestingSnowflake):
    client = secure_snowflake_allow_read.user_client("admin_user")
    perms = client.make_request("get", "/api/v1/all_routes", dict[str, dict[str, str]])

    all_roles = set(GLOBAL_ROLE_PERMISSIONS.keys())

    role_clients = {}
    for role in all_roles:
        if role == "anonymous":
            role_clients[role] = secure_snowflake_allow_read.client()
        else:
            role_user = secure_snowflake_allow_read.get_test_user_by_role(role)
            role_clients[role] = secure_snowflake_allow_read.user_client(role_user.username)

    for endpoint, info in perms.items():
        action, method, resource = info["action"], info["method"], info["resource"]

        # Replace various values in the route with placeholders (ie, 1234 for <int:dataset_id>)
        endpoint = substitute_route_url(endpoint)

        for role in all_roles:
            # Make a request using the client
            # For the most part, we don't care about input/output. If the user is denied access,
            # that should be the error. Otherwise, other errors will pop up, but that's fine
            role_client = role_clients[role]

            # Somewhat a duplicate of the other logic, but helpful for testing
            if resource == "none" and action == "none":
                # No permissions required
                expected_forbidden = False
            else:
                role_perms = GLOBAL_ROLE_PERMISSIONS[role]
                if resource in role_perms:
                    resource_perms = role_perms[resource]
                elif "*" in role_perms:
                    resource_perms = role_perms["*"]
                else:
                    resource_perms = {}

                if action in resource_perms:
                    action_perms = resource_perms[action]
                elif "*" in resource_perms:
                    action_perms = resource_perms["*"]
                else:
                    action_perms = AuthorizedEnum.Deny

                expected_forbidden = action_perms != AuthorizedEnum.Allow

            try:
                role_client.make_request(method, endpoint, None, body=None)
                forbidden = False
            except AuthorizationFailure:
                forbidden = True
            except Exception as e:
                if "Forbidden" in str(e):
                    forbidden = True
                else:
                    forbidden = False

            # print(role, endpoint, resource, method, action, expected_forbidden, forbidden)
            if forbidden != expected_forbidden:
                if expected_forbidden:
                    raise RuntimeError(
                        f"Role='{role}' method='{method}' endpoint='{endpoint}' resource='{resource}' "
                        f"action='{action}' was allowed, but should have been denied."
                    )
                else:
                    raise RuntimeError(
                        f"Role='{role}' method='{method}' endpoint='{endpoint}' resource='{resource}' "
                        f"action='{action}' was denied, but should have been allowed."
                    )


def test_admin_only(secure_snowflake_allow_read: QCATestingSnowflake):
    # This just double/triple checks that very sensitive routes are only accessible to admins

    sensitive_endpoints = [
        ("patch", "/api/v1/users"),
        ("delete", "/api/v1/users/1234"),
        ("put", "/api/v1/users/1234/password"),
        ("put", "/api/v1/users/1234/preferences"),
        ("get", "/api/v1/users/1234/sessions"),
    ]

    for username, uinfo in test_users.items():
        role = uinfo["info"]["role"]
        if role in ("admin", "maintain"):
            continue
        for method, endpoint in sensitive_endpoints:
            client = secure_snowflake_allow_read.user_client(username)
            with pytest.raises(PortalRequestError, match=r"Forbidden"):
                client.make_request(method, endpoint, None, body=None)


def test_auth_global_no_unauth_read(secure_snowflake):
    with pytest.raises(PortalRequestError, match="Server requires login"):
        secure_snowflake.client()

    client = secure_snowflake.user_client("admin_user")
    perms = client.make_request("get", "/api/v1/all_routes", dict[str, dict[str, str]])

    # Fake a client - constructing normally results in trying to get information
    client = secure_snowflake.user_client("admin_user")
    client.username = None
    client._password = None
    client._jwt_access_exp = None
    client._jwt_refresh_exp = None
    client._jwt_refresh_token = None
    client._jwt_access_token = None
    client._req_session.headers.pop("Authorization")

    for endpoint, info in perms.items():
        method, resource = info["method"], info["resource"]
        if resource == "none":
            continue # no login required

        # Replace various values in the route with placeholders (ie, 1234 for <int:dataset_id>)
        endpoint = substitute_route_url(endpoint)

        with pytest.raises(PortalRequestError, match="Server requires login"):
            client.make_request(method, endpoint, None, body=None)


@pytest.mark.parametrize("as_admin", [True, False])
def test_auth_protected_endpoints(snowflake, as_admin):
    # Cannot add or modify users/groups when security is disabled
    uinfo = UserInfo(username='test', role="read", enabled=True)
    ginfo = GroupInfo(groupname="testg")

    snowflake.create_users()

    if as_admin:
        client = snowflake.user_client("admin_user")
    else:
        client = snowflake.client()

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
