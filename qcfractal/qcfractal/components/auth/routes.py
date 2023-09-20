from typing import Optional, Tuple, Union

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
from qcportal.auth import UserInfo, RoleInfo, GroupInfo
from qcportal.exceptions import (
    InconsistentUpdateError,
    SecurityNotEnabledError,
    UserManagementError,
    AuthorizationFailure,
)


###################################################################
# We have two user endpoints
# 1. /user/<username> is for admins. This allows for changing info
#    about other users, and for enabling/disabling users or changing
#    their roles
# 2. /me allows for changing passwords, fullname, etc of only the
#    logged-in user. Enabling/disabling or changing roles is not
#    allowed through this endpoint, but it is designed to be
#    accessible from all (logged-in users)
#
# Also note that all endpoints are disabled if security is not
# enabled. This is because we don't want to allow modifying users
# while permissions are not checked.
###################################################################


def is_same_user(username_or_id: Union[int, str]) -> bool:
    assert isinstance(username_or_id, (int, str))

    if "user_id" not in g:
        return False
    if "username" not in g:
        return False

    if isinstance(username_or_id, int) and username_or_id == g.user_id:
        return True
    if isinstance(username_or_id, str) and username_or_id == g.username:
        return True

    return False


def assert_security_enabled():
    if not current_app.config["QCFRACTAL_CONFIG"].enable_security:
        raise SecurityNotEnabledError("This functionality is not available if security is not enabled on the server")


# TODO - may not be needed after changing RBAC/ABAC framework
# can be removed after proper/more granular permissions are implemented
def assert_logged_in():
    if g.get("user_id", None) is None:
        raise AuthorizationFailure("Login is required")


def assert_admin():
    if g.get("role", None) != "admin":
        raise AuthorizationFailure("Forbidden: Admin access is required")


#################################
# Roles
#################################


@api_v1.route("/roles", methods=["GET"])
@wrap_route("READ")
def list_roles_v1():
    assert_security_enabled()
    return storage_socket.roles.list()


@api_v1.route("/roles", methods=["POST"])
@wrap_route("WRITE")
def add_role_v1(body_data: RoleInfo):
    assert_security_enabled()
    return storage_socket.roles.add(body_data)


@api_v1.route("/roles/<string:rolename>", methods=["GET"])
@wrap_route("READ")
def get_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.get(rolename)


@api_v1.route("/roles/<string:rolename>", methods=["PUT"])
@wrap_route("WRITE")
def modify_role_v1(rolename: str, body_data: RoleInfo):
    assert_security_enabled()
    body_data = body_data
    if rolename != body_data.rolename:
        raise InconsistentUpdateError(f"Cannot update role at {rolename} with role info for {body_data.rolename}")

    return storage_socket.roles.modify(body_data)


@api_v1.route("/roles/<string:rolename>", methods=["DELETE"])
@wrap_route("DELETE")
def delete_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.delete(rolename)


#################################
# Groups
#################################


@api_v1.route("/groups", methods=["GET"])
@wrap_route("READ")
def list_groups_v1():
    assert_security_enabled()
    assert_logged_in()
    assert_admin()

    return storage_socket.groups.list()


@api_v1.route("/groups", methods=["POST"])
@wrap_route("WRITE")
def add_group_v1(body_data: GroupInfo):
    assert_security_enabled()
    assert_logged_in()
    return storage_socket.groups.add(body_data)


@api_v1.route("/groups/<groupname_or_id>", methods=["GET"])
@wrap_route("READ")
def get_group_v1(groupname_or_id: Union[int, str]):
    assert_security_enabled()
    assert_logged_in()
    return storage_socket.groups.get(groupname_or_id)


@api_v1.route("/groups/<groupname_or_id>", methods=["DELETE"])
@wrap_route("DELETE")
def delete_group_v1(groupname_or_id: Union[int, str]):
    assert_security_enabled()
    assert_logged_in()
    return storage_socket.groups.delete(groupname_or_id)


#################################
# Users
#################################


@api_v1.route("/users", methods=["GET"])
@wrap_route("READ")
def list_users_v1():
    assert_security_enabled()
    assert_logged_in()
    assert_admin()
    return storage_socket.users.list()


@api_v1.route("/users", methods=["POST"])
@wrap_route("WRITE")
def add_user_v1(body_data: Tuple[UserInfo, Optional[str]]):
    assert_security_enabled()
    assert_logged_in()

    user_info, password = body_data
    return storage_socket.users.add(user_info, password)


@api_v1.route("/users/<username_or_id>", methods=["GET"])
@wrap_route("READ")
def get_user_v1(username_or_id: Union[int, str]):
    assert_security_enabled()
    assert_logged_in()

    # admin can do all
    if g.role == "admin":
        return storage_socket.users.get(username_or_id)

    if is_same_user(username_or_id):
        return storage_socket.users.get(username_or_id)

    raise AuthorizationFailure("Cannot get user information: Forbidden")


@api_v1.route("/users", methods=["PATCH"])
@wrap_route("WRITE")
def modify_user_v1(body_data: UserInfo):
    assert_security_enabled()
    assert_logged_in()

    if g.role == "admin":
        return storage_socket.users.modify(body_data, as_admin=True)

    if body_data.id is None or body_data.username is None:
        raise UserManagementError("Cannot modify user: id or username is missing")

    # Users can only modify themselves, and only certain fields
    # This checks id and name to make sure this user is only modifying themselves
    if is_same_user(body_data.id) and is_same_user(body_data.username):
        return storage_socket.users.modify(body_data, as_admin=False)

    raise AuthorizationFailure("Cannot modify user: Forbidden")


@api_v1.route("/users/<username_or_id>/password", methods=["PUT"])
@wrap_route("WRITE")
def change_password_v1(username_or_id: Union[int, str], body_data: Optional[str]):
    assert_security_enabled()
    assert_logged_in()

    if g.role == "admin":
        return storage_socket.users.change_password(username_or_id, password=body_data)

    # This checks id or name to make sure this user is only modifying themselves
    if is_same_user(username_or_id):
        return storage_socket.users.change_password(username_or_id, password=body_data)

    raise AuthorizationFailure("Cannot change password: Forbidden")


@api_v1.route("/users/<username_or_id>", methods=["DELETE"])
@wrap_route("DELETE")
def delete_user_v1(username_or_id: Union[int, str]):
    assert_security_enabled()
    assert_logged_in()

    if is_same_user(username_or_id):
        raise UserManagementError("Cannot delete your own user")

    return storage_socket.users.delete(username_or_id)
