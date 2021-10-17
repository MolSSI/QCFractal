from flask import jsonify, request, current_app, g

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.components.permissions import UserInfo, RoleInfo
from qcfractal.exceptions import InconsistentUpdateError, SecurityNotEnabledError


def assert_security_enabled():
    if not current_app.config["QCFRACTAL_CONFIG"].enable_security:
        raise SecurityNotEnabledError("This functionality is not available if security is not enabled on the server")


@main.route("/v1/role", methods=["GET"])
@wrap_route(None, None)
@check_access
def list_roles_v1():
    assert_security_enabled()
    return storage_socket.roles.list()


@main.route("/v1/role/<string:rolename>", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_role_v1(rolename: str):
    assert_security_enabled()
    return storage_socket.roles.get(rolename)


@main.route("/v1/role", methods=["POST"])
@wrap_route(RoleInfo, None)
@check_access
def add_role_v1():
    assert_security_enabled()
    return storage_socket.roles.add(g.validated_data)


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


@main.route("/v1/user", methods=["GET"])
@wrap_route(None, None)
@check_access
def list_users_v1():
    # assert_security_enabled()
    return storage_socket.users.list()


@main.route("/v1/user/<string:username>", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_user_v1(username: str):
    # assert_security_enabled()
    return storage_socket.users.get(username)


@main.route("/v1/user/<string:username>", methods=["PUT"])
@wrap_route(UserInfo, None)
@check_access
def modify_user_v1(username: str):
    # assert_security_enabled()

    user_info = g.validated_data
    if username != user_info.username:
        raise InconsistentUpdateError(f"Cannot update user at {username} with user info for {user_info.username}")

    # If you have access to this endpoint, then you should be an admin, at least as far
    # as user management is concerned
    return storage_socket.users.modify(user_info, as_admin=True)
