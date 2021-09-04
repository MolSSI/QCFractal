from flask import jsonify, request, current_app

from qcfractal.app import storage_socket
from qcfractal.app.new_routes.main import main
from qcfractal.app.new_routes.permissions import check_access


@main.route("/role", methods=["GET"])
@check_access
def list_roles_v1():
    roles = storage_socket.role.list()
    # TODO - SerializedResponse?
    r = [x.dict() for x in roles]
    return jsonify(roles), 200


@main.route("/role/<string:rolename>", methods=["GET"])
@check_access
def get_role_v1(rolename: str):

    role = storage_socket.role.get(rolename)
    # TODO - SerializedResponse?
    return jsonify(role.dict()), 200


@main.route("/role/<string:rolename>", methods=["POST"])
@check_access
def add_role_v1():
    rolename = request.json["rolename"]
    permissions = request.json["permissions"]

    try:
        storage_socket.role.add(rolename, permissions)
        return jsonify({"msg": "New role created!"}), 201
    except Exception as e:
        current_app.logger.warning(f"Error creating role {rolename}: {str(e)}")
        return jsonify({"msg": "Error creating role"}), 400


@main.route("/role", methods=["PUT"])
@check_access
def update_role_v1():
    rolename = request.json["rolename"]
    permissions = request.json["permissions"]

    try:
        storage_socket.role.update(rolename, permissions)
        return jsonify({"msg": "Role was updated!"}), 200
    except Exception as e:
        current_app.logger.warning(f"Error updating role {rolename}: {str(e)}")
        return jsonify({"msg": "Failed to update role"}), 400


@main.route("/role", methods=["DELETE"])
@check_access
def delete_role_v1():
    rolename = request.json["rolename"]

    try:
        storage_socket.role.delete(rolename)
        return jsonify({"msg": "Role was deleted!"}), 200
    except Exception as e:
        current_app.logger.warning(f"Error deleting role {rolename}: {str(e)}")
        return jsonify({"msg": "Failed to delete role!"}), 400
