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
def get_molecule():
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
    email = request.form['email']
    test = User.query.filter_by(email=email).first()
    if test:
        return jsonify(message='That email already exists.'), 409
    else:
        first_name = request.form['first_name']
        last_name = request.form['last_name']
        password = request.form['password']
        user = User(first_name=first_name, last_name=last_name, email=email, password=password)
        db.session.add(user)
        db.session.commit()
        return jsonify(message="User created successfully."), 201


@app.route('/login', methods=['POST'])
def login():
    if request.is_json:
        email = request.json['email']
        password = request.json['password']
    else:
        email = request.form['email']
        password = request.form['password']

    test = User.query.filter_by(email=email, password=password).first()
    if test:
        access_token = create_access_token(identity=email)
        return jsonify(message="Login succeeded!", access_token=access_token)
    else:
        return jsonify(message="Bad email or password"), 401


@app.route('/retrieve_password/<string:email>', methods=['GET'])
def retrieve_password(email: str):
    user = User.query.filter_by(email=email).first()
    if user:
        msg = Message("your planetary API password is " + user.password,
                      sender="admin@planetary-api.com",
                      recipients=[email])
        mail.send(msg)
        return jsonify(message="Password sent to " + email)
    else:
        return jsonify(message="That email doesn't exist"), 401


if __name__ == '__main__':
    app.run()
