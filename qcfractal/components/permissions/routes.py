from flask import jsonify, request, current_app, g

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.components.permissions import RoleInfo


@main.route("/v1/role", methods=["GET"])
@wrap_route(None, None)
@check_access
def list_roles_v1():
    return storage_socket.roles.list()


@main.route("/v1/role/<string:rolename>", methods=["GET"])
@wrap_route(None, None)
@check_access
def get_role_v1(rolename: str):
    return storage_socket.roles.get(rolename)


@main.route("/v1/role", methods=["POST"])
@wrap_route(RoleInfo, None)
@check_access
def add_role_v1():
    rolename = g.validated_data.rolename
    permissions = g.validated_data.permissions
    storage_socket.roles.add(rolename, permissions)


@main.route("/role", methods=["PUT"])
@check_access
def update_role_v1():
    rolename = request.json["rolename"]
    permissions = request.json["permissions"]

    try:
        storage_socket.roles.update(rolename, permissions)
        return jsonify({"msg": "Role was updated!"}), 200
    except Exception as e:
        current_app.logger.warning(f"Error updating role {rolename}: {str(e)}")
        return jsonify({"msg": "Failed to update role"}), 400


@main.route("/role", methods=["DELETE"])
@check_access
def delete_role_v1():
    rolename = request.json["rolename"]

    try:
        storage_socket.roles.delete(rolename)
        return jsonify({"msg": "Role was deleted!"}), 200
    except Exception as e:
        current_app.logger.warning(f"Error deleting role {rolename}: {str(e)}")
        return jsonify({"msg": "Failed to delete role!"}), 400
