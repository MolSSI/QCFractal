from flask import Flask, jsonify, request
import os
from flask_jwt_extended import JWTManager, jwt_required, create_access_token
from flask_mail import Mail, Message

import asyncio
import datetime
import logging
import ssl
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Union

from .extras import get_information
from .interface import FractalClient
from .qc_queue import QueueManager, QueueManagerHandler, ServiceQueueHandler, TaskQueueHandler, ComputeManagerHandler
from .services import construct_service
from .storage_sockets import ViewHandler, storage_socket_factory
from .storage_sockets.api_logger import API_AccessLogger
from .web_handlers import (
    CollectionHandler,
    InformationHandler,
    KeywordHandler,
    KVStoreHandler,
    MoleculeHandler,
    OptimizationHandler,
    ProcedureHandler,
    ResultHandler,
    WavefunctionStoreHandler,
)
import json

from pydantic import ValidationError
from qcelemental.util import deserialize, serialize

from .interface.models.rest_models import rest_model
from .storage_sockets.storage_utils import add_metadata_template
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['JWT_SECRET_KEY'] = 'super-secret'  # change this IRL
app.config['MAIL_SERVER'] = 'smtp.mailtrap.io'
app.config['MAIL_USERNAME'] = os.environ['MAIL_USERNAME']
app.config['MAIL_PASSWORD'] = os.environ['MAIL_PASSWORD']

jwt = JWTManager(app)
mail = Mail(app)

storage = storage_socket_factory(
    "postgresql://localhost:5432",
    project_name="qcfractal_default",
    bypass_security=False,
    allow_read=False,
    max_limit=1000,
    skip_version_check=True
)

_valid_encodings = {
    "application/json": "json",
    "application/json-ext": "json-ext",
    "application/msgpack-ext": "msgpack-ext",
}


def parse_bodymodel(args, model):
    try:
        return model(**args)
    except ValidationError:
        return jsonify(message=ValidationError), 500


@app.route('/information', methods=['GET'])
def get_information():
    public_information = {
            "name": "self.name",
            "heartbeat_frequency": "self.heartbeat_frequency",
            "version": "version",
            "query_limit": "self.storage.get_limit(1.0e9)",
            "client_lower_version_limit": "0.12.1",
            "client_upper_version_limit": "0.13.99",
            }
    return jsonify(public_information)


@app.route('/molecule', methods=['GET'])
@jwt_required
def get_molecule():
    """
    Request:
        "meta" - Overall options to the Molecule pull request
            - "index" - What kind of index used to find the data ("id", "molecule_hash", "molecular_formula")
        "data" - A dictionary of {key : index} requests

    Returns:
        "meta" - Metadata associated with the query
            - "errors" - A list of errors in (index, error_id) format.
            - "n_found" - The number of molecule found.
            - "success" - If the query was successful or not.
            - "error_description" - A string based description of the error or False
            - "missing" - A list of keys that were not found.
        "data" - A dictionary of {key : molecule JSON} results
    """
    content_type = request.headers.get("Content-Type", "application/json")
    encoding = _valid_encodings[content_type]

    body_model, response_model = rest_model("molecule", "get")
    body = parse_bodymodel(request.json, body_model)

    molecules = storage.get_molecules(**{**body.data.dict(), **body.meta.dict()})
    ret = response_model(**molecules)

    if not isinstance(ret, (str, bytes)):
        data = serialize(ret, encoding)

    return data


@app.route('/molecule', methods=['POST'])
@jwt_required
def post_molecule():
    """
    Request:
        "meta" - Overall options to the Molecule pull request
            - No current options
        "data" - A dictionary of {key : molecule JSON} requests

    Returns:
        "meta" - Metadata associated with the query
            - "errors" - A list of errors in (index, error_id) format.
            - "n_inserted" - The number of molecule inserted.
            - "success" - If the query was successful or not.
            - "error_description" - A string based description of the error or False
            - "duplicates" - A list of keys that were already inserted.
        "data" - A dictionary of {key : id} results
    """
    content_type = request.headers.get("Content-Type", "application/json")
    encoding = _valid_encodings[content_type]

    body_model, response_model = rest_model("molecule", "post")
    body = parse_bodymodel(request.json, body_model)

    ret = storage.add_molecules(body.data)
    response = response_model(**ret)

    if not isinstance(response, (str, bytes)):
        data = serialize(response, encoding)

    return data


@app.route('/register', methods=['POST'])
def register():
    if request.is_json:
        email = request.json['email']
        password = request.json['password']
    else:
        email = request.form['email']
        password = request.form['password']

    success, pw = storage.add_user(email, password=password, permissions=["read"])
    if success:
        print(f"\n>>> New user successfully added, password:\n{pw}")
        return jsonify({'message' : 'New user created!'}), 201
    else:
        print("\n>>> Failed to add user. Perhaps the username is already taken?")
        return jsonify({'message' : 'Failed to add user.'}), 500


@app.route('/login', methods=['POST'])
def login():
    if request.is_json:
        email = request.json['email']
        password = request.json['password']
    else:
        email = request.form['email']
        password = request.form['password']

    success = storage.verify_user(email, password, "read")[0]
    if success:
        access_token = create_access_token(identity=email)
        return jsonify(message="Login succeeded!", access_token=access_token)
    else:
        return jsonify(message="Bad email or password"), 401


@app.route('/kvstore', methods=['GET'])
@jwt_required
def get_kvstore():
    """
    Request:
        "data" - A list of key requests
    Returns:
        "meta" - Metadata associated with the query
            - "errors" - A list of errors in (index, error_id) format.
            - "n_found" - The number of molecule found.
            - "success" - If the query was successful or not.
            - "error_description" - A string based description of the error or False
            - "missing" - A list of keys that were not found.
        "data" - A dictionary of {key : value} dictionary of the results
    """
    content_type = request.headers.get("Content-Type", "application/json")
    encoding = _valid_encodings[content_type]

    body_model, response_model = rest_model("kvstore", "get")
    body = parse_bodymodel(body_model)

    ret = storage.get_kvstore(body.data.id)
    ret = response_model(**ret)

    if not isinstance(ret, (str, bytes)):
        data = serialize(ret, encoding)

    return data


@app.route('/collection/<int:collection_id>/<string:view_function>', methods=['GET','POST','DELETE'])
@jwt_required
def handle_collection(collection_id: int, view_function: str):

    if request.method == 'GET':
        # List collections
        if (collection_id is None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")
            body = parse_bodymodel(body_model)

            cols = storage.get_collections(
                **body.data.dict(), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

        # Get specific collection
        elif (collection_id is not None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")

            body = parse_bodymodel(body_model)
            cols = storage.get_collections(
                **body.data.dict(), col_id=int(collection_id), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

        # View-backed function on collection
        elif (collection_id is not None) and (view_function is not None):
            body_model, response_model = rest_model(f"collection/{collection_id}/{view_function}", "get")
            body = parse_bodymodel(body_model)
            if view_handler is None:
                meta = {
                    "success": False,
                    "error_description": "Server does not support collection views.",
                    "errors": [],
                    "msgpacked_cols": [],
                }
                response = response_model(meta=meta, data=None)
                if not isinstance(response, (str, bytes)):
                    data = serialize(response, encoding)

                return data

            result = view_handler.handle_request(collection_id, view_function, body.data.dict())
            response = response_model(**result)

        # Unreachable?
        else:
            body_model, response_model = rest_model("collection", "get")
            meta = add_metadata_template()
            meta["success"] = False
            meta["error_description"] = "GET request for view with no collection ID not understood."
            response = response_model(meta=meta, data=None)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    elif request.method == 'POST':
        body_model, response_model = rest_model("collection", "post")
        body = parse_bodymodel(body_model)

        # POST requests not supported for anything other than "/collection"
        if collection_id is not None or view_function is not None:
            meta = add_metadata_template()
            meta["success"] = False
            meta["error_description"] = "POST requests not supported for sub-resources of /collection"
            response = response_model(meta=meta, data=None)
            if not isinstance(response, (str, bytes)):
                data = serialize(response, encoding)

            return data

        ret = storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    elif request.method == 'DELETE':
        body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
        ret = storage.del_collection(col_id=collection_id)
        if ret == 0:
            return jsonify(message="Collection does not exist."), 404
        else:
            response = response_model(meta={"success": True, "errors": [], "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

# @app.route('/retrieve_password/<string:email>', methods=['GET'])
# def retrieve_password(email: str):
#     user = User.query.filter_by(email=email).first()
#     if user:
#         msg = Message("your planetary API password is " + user.password,
#                       sender="admin@planetary-api.com",
#                       recipients=[email])
#         mail.send(msg)
#         return jsonify(message="Password sent to " + email)
#     else:
#         return jsonify(message="That email doesn't exist"), 401


if __name__ == '__main__':
    app.run()
