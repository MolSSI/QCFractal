from typing import Optional, Tuple

from flask import current_app, g
from werkzeug.exceptions import BadRequest

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.exceptions import InconsistentUpdateError, SecurityNotEnabledError
from qcportal.permissions import UserInfo, RoleInfo


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


@main.route("/v1/roles", methods=["GET"])
@wrap_route(None, None, "READ")
def list_roles_v1():
    assert_security_enabled()
    return storage_socket.roles.list()


@main.route("/v1/roles", methods=["POST"])
@wrap_route(RoleInfo, None, "WRITE")
def add_role_v1(body_data: RoleInfo):
    assert_security_enabled()
    return storage_socket.roles.add(body_data)


@main.route("/v1/roles/<string:rolename>", methods=["GET"])
@wrap_route(None, None, "READ")
def get_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.get(rolename)


@main.route("/v1/roles/<string:rolename>", methods=["PUT"])
@wrap_route(RoleInfo, None, "WRITE")
def modify_role_v1(rolename: str, *, body_data: RoleInfo):
    assert_security_enabled()
    body_data = body_data
    if rolename != body_data.rolename:
        raise InconsistentUpdateError(f"Cannot update role at {rolename} with role info for {body_data.rolename}")

    return storage_socket.roles.modify(body_data)


@main.route("/v1/roles/<string:rolename>", methods=["DELETE"])
@wrap_route(None, None, "DELETE")
def delete_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.delete(rolename)


#################################
# Users
#################################
@main.route("/v1/users", methods=["GET"])
@wrap_route(None, None, "READ")
def list_users_v1():
    assert_security_enabled()
    return storage_socket.users.list()


@main.route("/v1/users", methods=["POST"])
@wrap_route(Tuple[UserInfo, Optional[str]], None, "WRITE")
def add_user_v1(body_data: Tuple[UserInfo, Optional[str]]):

    user_info, password = body_data

    assert_security_enabled()
    return storage_socket.users.add(user_info, password)


@main.route("/v1/users/<string:username>", methods=["GET"])
@wrap_route(None, None, "READ")
def get_user_v1(username: str):
    assert_security_enabled()
    return storage_socket.users.get(username)


@main.route("/v1/users/<string:username>", methods=["PUT"])
@wrap_route(UserInfo, None, "WRITE")
def modify_user_v1(username: str, *, body_data: UserInfo):
    assert_security_enabled()

    current_app.logger.info(f"Modifying user {username}")

    if username != body_data.username:
        raise InconsistentUpdateError(f"Cannot update user at {username} with user info for {body_data.username}")

    # If you have access to this endpoint, then you should be an admin, at least as far
    # as user management is concerned
    return storage_socket.users.modify(body_data, as_admin=True)


@main.route("/v1/users/<string:username>/password", methods=["PUT"])
@wrap_route(Optional[str], None, "WRITE")
def change_password_v1(username: str, *, body_data: Optional[str]):
    assert_security_enabled()

    # Returns the password (new or generated)
    return storage_socket.users.change_password(username, password=body_data)


@main.route("/v1/users/<string:username>", methods=["DELETE"])
@wrap_route(None, None, "DELETE")
def delete_user_v1(username: str):
    assert_security_enabled()

    if g.get("user", None) == username:
        raise BadRequest("Cannot delete own user")

    return storage_socket.users.delete(username)


#################################
# My User
#################################
@main.route("/v1/me", methods=["GET"])
@wrap_route(None, None, "READ")
def get_my_user_v1():
    assert_security_enabled()

    # Get the logged-in user
    username = g.get("user", None)

    if username is None:
        raise BadRequest("No current user - not logged in")

    return storage_socket.users.get(g.user)


@main.route("/v1/me", methods=["PUT"])
@wrap_route(UserInfo, None, "WRITE")
def modify_my_user_v1(body_data: UserInfo):
    assert_security_enabled()

    # Get the logged-in user
    username = g.get("user", None)

    if username is None:
        raise BadRequest("No current user - not logged in")

    current_app.logger.info(f"Modifying my user {username}")

    if username != body_data.username:
        raise InconsistentUpdateError(f"Trying to update own user {username} with user info for {body_data.username}")

    # This endpoint is not for admins
    return storage_socket.users.modify(body_data, as_admin=False)


@main.route("/v1/me/password", methods=["PUT"])
@wrap_route(Optional[str], None, "WRITE")
def change_my_password_v1(body_data: Optional[str]):
    assert_security_enabled()
    username = g.get("user", None)

    if username is None:
        raise BadRequest("No current user - not logged in")

    # Returns the password (new or generated)
    return storage_socket.users.change_password(username, password=body_data)
