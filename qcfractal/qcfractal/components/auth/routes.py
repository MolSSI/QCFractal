from typing import Optional, Tuple, Union, Dict, Any

from flask import g
from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcportal.auth import UserInfo, GroupInfo
from qcportal.exceptions import (
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

    if isinstance(username_or_id, str) and username_or_id.isdecimal():
        username_or_id = int(username_or_id)

    if "user_id" not in g:
        return False
    if "username" not in g:
        return False

    if isinstance(username_or_id, int) and username_or_id == g.user_id:
        return True
    if isinstance(username_or_id, str) and username_or_id == g.username:
        return True

    return False


#################################
# Groups
#################################


@api_v1.route("/groups", methods=["GET"])
@check_permissions("groups", "read", True)
@serialization()
def list_groups_v1():
    return storage_socket.groups.list()


@api_v1.route("/groups", methods=["POST"])
@check_permissions("groups", "add", True)
@serialization()
def add_group_v1(body_data: GroupInfo):
    return storage_socket.groups.add(body_data)


@api_v1.route("/groups/<groupname_or_id>", methods=["GET"])
@check_permissions("groups", "read", True)
@serialization()
def get_group_v1(groupname_or_id: Union[int, str]):
    return storage_socket.groups.get(groupname_or_id)


@api_v1.route("/groups/<groupname_or_id>", methods=["DELETE"])
@check_permissions("groups", "delete", True)
@serialization()
def delete_group_v1(groupname_or_id: Union[int, str]):
    return storage_socket.groups.delete(groupname_or_id)


#################################
# Users
#################################


@api_v1.route("/users", methods=["GET"])
@check_permissions("users", "read", True)
@serialization()
def list_users_v1():
    return storage_socket.users.list()


@api_v1.route("/users", methods=["POST"])
@check_permissions("users", "add", True)
@serialization()
def add_user_v1(body_data: Tuple[UserInfo, Optional[str]]):
    user_info, password = body_data
    return storage_socket.users.add(user_info, password)


@api_v1.route("/users/<username_or_id>", methods=["GET"])
@check_permissions("users", "read", True)
@serialization()
def get_user_v1(username_or_id: Union[int, str]):
    return storage_socket.users.get(username_or_id)


@api_v1.route("/me", methods=["GET"])
@check_permissions("me", "read", True)
@serialization()
def get_my_user_v1():
    return storage_socket.users.get(g.user_id)


@api_v1.route("/users", methods=["PATCH"])
@check_permissions("users", "modify", True)
@serialization()
def modify_user_v1(body_data: UserInfo):
    return storage_socket.users.modify(body_data, as_admin=True)


@api_v1.route("/me", methods=["PATCH"])
@check_permissions("me", "modify", True)
@serialization()
def modify_my_user_v1(body_data: UserInfo):
    if body_data.id is None or body_data.username is None:
        raise UserManagementError("Cannot modify user: id or username is missing")

    if is_same_user(body_data.id) and is_same_user(body_data.username):
        return storage_socket.users.modify(body_data, as_admin=False)

    raise AuthorizationFailure("Cannot modify user: Forbidden")


@api_v1.route("/users/<username_or_id>/password", methods=["PUT"])
@check_permissions("users", "modify", True)
@serialization()
def change_password_v1(username_or_id: Union[int, str], body_data: Optional[str]):
    return storage_socket.users.change_password(username_or_id, password=body_data)


@api_v1.route("/me/password", methods=["PUT"])
@check_permissions("me", "modify", True)
@serialization()
def change_my_password_v1(body_data: Optional[str]):
    return storage_socket.users.change_password(g.user_id, password=body_data)


@api_v1.route("/users/<username_or_id>", methods=["DELETE"])
@check_permissions("users", "delete", True)
@serialization()
def delete_user_v1(username_or_id: Union[int, str]):
    if is_same_user(username_or_id):
        raise UserManagementError("Cannot delete your own user")

    return storage_socket.users.delete(username_or_id)


###########################
# User preferences management
###########################
@api_v1.route("/users/<username_or_id>/preferences", methods=["GET"])
@check_permissions("users", "read", True)
@serialization()
def get_user_preferences_v1(username_or_id: Union[int, str]):
    return storage_socket.users.get_preferences(username_or_id)


@api_v1.route("/me/preferences", methods=["GET"])
@check_permissions("me", "read", True)
@serialization()
def get_my_preferences_v1():
    return storage_socket.users.get_preferences(g.user_id)


@api_v1.route("/users/<username_or_id>/preferences", methods=["PUT"])
@check_permissions("users", "modify", True)
@serialization()
def set_user_preferences_v1(username_or_id: Union[int, str], body_data: Dict[str, Any]):
    return storage_socket.users.set_preferences(username_or_id, body_data)


@api_v1.route("/me/preferences", methods=["PUT"])
@check_permissions("me", "modify", True)
@serialization()
def set_my_preferences_v1(body_data: Dict[str, Any]):
    return storage_socket.users.set_preferences(g.user_id, body_data)


###########################
# User session management
###########################
@api_v1.route("/sessions", methods=["GET"])
@check_permissions("users", "read", True)
@serialization()
def list_all_user_sessions_v1():
    return storage_socket.auth.list_all_user_sessions()


@api_v1.route("/users/<username_or_id>/sessions", methods=["GET"])
@check_permissions("users", "read", True)
@serialization()
def list_user_sessions_v1(username_or_id: Union[int, str]):
    return storage_socket.auth.list_user_sessions(username_or_id)


@api_v1.route("/me/sessions", methods=["GET"])
@check_permissions("me", "read", True)
@serialization()
def list_my_sessions_v1():
    return storage_socket.auth.list_user_sessions(g.user_id)
