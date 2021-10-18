from flask import jsonify, request, current_app, g

from werkzeug.exceptions import BadRequest
from qcfractal.app import main, storage_socket
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.components.permissions import UserInfo, RoleInfo
from qcfractal.exceptions import InconsistentUpdateError, SecurityNotEnabledError
from typing import Optional, Tuple

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


def assert_security_enabled():
    if not current_app.config["QCFRACTAL_CONFIG"].enable_security:
        raise SecurityNotEnabledError("This functionality is not available if security is not enabled on the server")


#################################
# Roles
#################################


@main.route("/v1/role", methods=["GET"])
@wrap_route(None, None)
@check_access
def list_roles_v1():
    assert_security_enabled()
    return storage_socket.roles.list()


@main.route("/v1/role", methods=["POST"])
@wrap_route(RoleInfo, None)
@check_access
def add_role_v1():
    assert_security_enabled()
    return storage_socket.roles.add(g.validated_data)


@main.route("/v1/role/<string:rolename>", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.get(rolename)


@main.route("/v1/role/<string:rolename>", methods=["PUT"])
@wrap_route(RoleInfo, None)
@check_access
def modify_role_v1(rolename: str):
    assert_security_enabled()
    role_info = g.validated_data
    if rolename != role_info.rolename:
        raise InconsistentUpdateError(f"Cannot update role at {rolename} with role info for {role_info.rolename}")

    return storage_socket.roles.modify(role_info)


@main.route("/v1/role/<string:rolename>", methods=["DELETE"])
@wrap_route(None, None)
@check_access
def delete_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.delete(rolename)


#################################
# Users
#################################
@main.route("/v1/user", methods=["GET"])
@wrap_route(None, None)
@check_access
def list_users_v1():
    assert_security_enabled()
    return storage_socket.users.list()


@main.route("/v1/user", methods=["POST"])
@wrap_route(Tuple[UserInfo, Optional[str]], None)
@check_access
def add_user_v1(username: str):

    user_info, password = g.validated_data

    assert_security_enabled()
    return storage_socket.users.add(user_info, password)


@main.route("/v1/user/<string:username>", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_user_v1(username: str):
    assert_security_enabled()
    return storage_socket.users.get(username)


@main.route("/v1/user/<string:username>", methods=["PUT"])
@wrap_route(UserInfo, None)
@check_access
def modify_user_v1(username: str):
    assert_security_enabled()

    current_app.logger.info(f"Modifying user {username}")

    user_info = g.validated_data
    if username != user_info.username:
        raise InconsistentUpdateError(f"Cannot update user at {username} with user info for {user_info.username}")

    # If you have access to this endpoint, then you should be an admin, at least as far
    # as user management is concerned
    return storage_socket.users.modify(user_info, as_admin=True)


@main.route("/v1/user/<string:username>/password", methods=["PUT"])
@wrap_route(Optional[str], None)
@check_access
def change_password_v1(username: str):
    assert_security_enabled()
    new_password = g.validated_data

    # Returns the password (new or generated)
    return storage_socket.users.change_password(username, new_password)


@main.route("/v1/user/<string:username>", methods=["DELETE"])
@wrap_route(None, None)
@check_access
def delete_user_v1(username: str):
    assert_security_enabled()

    if g.get("user", None) == username:
        raise BadRequest("Cannot delete own user")

    return storage_socket.users.delete(username)


#################################
# My User
#################################
@main.route("/v1/me", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_my_user_v1():
    assert_security_enabled()
    username = g.get("user", None)

    if username is None:
        raise BadRequest("No current user - not logged in")

    return storage_socket.users.get(g.user)


@main.route("/v1/me/password", methods=["PUT"])
@wrap_route(Optional[str], None)
@check_access
def change_my_password_v1():
    assert_security_enabled()
    username = g.get("user", None)

    if username is None:
        raise BadRequest("No current user - not logged in")

    new_password = g.validated_data

    # Returns the password (new or generated)
    return storage_socket.users.reset_password(username, new_password)
