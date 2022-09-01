from flask import request, current_app, jsonify
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity

from qcfractal.flask_app import auth, storage_socket
from qcportal.exceptions import AuthenticationFailure


@auth.route("/v1/login", methods=["POST"])
def login():
    try:
        if request.is_json:
            username = request.json["username"]
            password = request.json["password"]
        else:
            username = request.form["username"]
            password = request.form["password"]
    except Exception:
        current_app.logger.info("Invalid/malformed login request")
        raise AuthenticationFailure("Invalid/malformed login request")

    if username is None:
        current_app.logger.info("No username provided for login")
        raise AuthenticationFailure("No username provided for login")
    if password is None:
        current_app.logger.info(f"No username provided for login of user {username}")
        raise AuthenticationFailure("No password provided for login")

    # Raises exceptions on error
    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
    # This should be handled properly by the flask errorhandlers
    try:
        permissions = storage_socket.users.verify(username, password)
    except AuthenticationFailure as e:
        current_app.logger.info(f"Authentication failed for user {username}: {str(e)}")
        raise

    access_token = create_access_token(identity=username, additional_claims={"permissions": permissions})
    # expires_delta=datetime.timedelta(days=3))
    refresh_token = create_refresh_token(identity=username)

    current_app.logger.info(f"Successful login for user {username}")
    return jsonify(msg="Login succeeded!", access_token=access_token, refresh_token=refresh_token), 200


@auth.route("/v1/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    username = get_jwt_identity()
    role, permissions = storage_socket.users.get_permissions(username)
    ret = {
        "access_token": create_access_token(
            identity=username, additional_claims={"role": role, "permissions": permissions}
        )
    }
    return jsonify(ret), 200


# @auth.route("/v1/fresh-login", methods=["POST"])
# def fresh_login():
#    if request.is_json:
#        username = request.json["username"]
#        password = request.json["password"]
#    else:
#        username = request.form["username"]
#        password = request.form["password"]
#
#    # Raises exceptions on error
#    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
#    # This should be handled properly by the flask errorhandlers
#    permissions = storage_socket.users.verify(username, password)
#
#    access_token = create_access_token(
#        identity=username, additional_claims={"permissions": permissions.dict()}, fresh=True
#    )
#    return jsonify(msg="Fresh login succeeded!", access_token=access_token), 200


# def register():
#    if request.is_json:
#        username = request.json["username"]
#        password = request.json["password"]
#        fullname = request.json["fullname"]
#        email = request.json["email"]
#        organization = request.json["organization"]
#    else:
#        username = request.form["username"]
#        password = request.form["password"]
#        fullname = request.form["fullname"]
#        email = request.form["email"]
#        organization = request.form["organization"]
#
#    role = "read"
#    try:
#        user_info = UserInfo(
#            username=username,
#            enabled=True,
#            fullname=fullname,
#            email=email,
#            organization=organization,
#            role=role,
#        )
#    except Exception as e:
#        return jsonify(msg=f"Invalid user information: {str(e)}"), 500
#
#    # add returns the password. Raises exception on error
#    # Exceptions should be handled property by the flask errorhandlers
#    pw = storage_socket.users.add(user_info, password=password)
#    if password is None or len(password) == 0:
#        return jsonify(msg="New user created!"), 201
#    else:
#        return jsonify(msg="New user created! Password is '{pw}'"), 201
