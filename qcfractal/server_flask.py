import asyncio
import logging
import ssl
import time
import traceback
import json
import os
import tornado.ioloop

from datetime import datetime, timedelta
from urllib.parse import urlparse
from flask import Flask, jsonify, request
from flask_jwt_extended import (
    JWTManager,
    jwt_required,
    fresh_jwt_required,
    create_access_token,
    get_jwt_claims,
    get_current_user,
    jwt_refresh_token_required,
    create_refresh_token,
    get_jwt_identity
)
from flask_mail import Mail, Message
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Union
from .extras import get_information
from .interface import FractalClient
from .qc_queue import QueueManager, QueueManagerHandler, ServiceQueueHandler, TaskQueueHandler, ComputeManagerHandler
from .services import construct_service
from .storage_sockets import ViewHandler, storage_socket_factory
from .storage_sockets.api_logger import API_AccessLogger
from .storage_sockets.storage_utils import add_metadata_template
from pydantic import ValidationError
from qcelemental.util import deserialize, serialize
from .interface.models.rest_models import rest_model
from werkzeug.security import generate_password_hash, check_password_hash
from .procedures import check_procedure_available, get_procedure_parser
from .policyuniverse import Policy


def _build_ssl():
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    import sys
    import socket
    import ipaddress
    import random

    hostname = socket.gethostname()
    public_ip = ipaddress.ip_address(socket.gethostbyname(hostname))

    key = rsa.generate_private_key(public_exponent=65537, key_size=1024, backend=default_backend())

    alt_name_list = [x509.DNSName(hostname), x509.IPAddress(ipaddress.ip_address(public_ip))]
    alt_names = x509.SubjectAlternativeName(alt_name_list)

    # Basic data
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)])
    basic_contraints = x509.BasicConstraints(ca=True, path_length=0)
    now = datetime.utcnow()

    # Build cert
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(int(random.random() * sys.maxsize))
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=10 * 365))
        .add_extension(basic_contraints, False)
        .add_extension(alt_names, False)
        .sign(key, hashes.SHA256(), default_backend())
    )  # yapf: disable

    # Build and return keys
    cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )  # yapf: disable

    return cert_pem, key_pem


class FractalServer:
    def __init__(
        self,
        # Server info options
        name: str = "QCFractal Server",
        port: int = 5000,
        loop: "IOLoop" = None,
        compress_response: bool = True,
        # Security
        security: Optional[str] = None,
        allow_read: bool = False,
        ssl_options: Union[bool, Dict[str, str]] = True,
        # Database options
        storage_uri: str = "postgresql://localhost:5432",
        storage_project_name: str = "test_qcfractal_compute_snowflake1",
        query_limit: int = 1000,
        # View options
        view_enabled: bool = False,
        view_path: Optional[str] = None,
        # Log options
        logfile_prefix: str = None,
        loglevel: str = "info",
        log_apis: bool = False,
        geo_file_path: str = None,
        # Queue options
        queue_socket: "BaseAdapter" = None,
        heartbeat_frequency: float = 1800,
        # Service options
        max_active_services: int = 20,
        service_frequency: float = 60,
        # Testing functions
        skip_storage_version_check=True,
    ):
        """QCFractal initialization

        Parameters
        ----------
        name : str, optional
            The name of the server itself, provided when users query information
        port : int, optional
            The port the server will listen on.
        loop : IOLoop, optional
            Provide an IOLoop to use for the server
        compress_response : bool, optional
            Automatic compression of responses, turn on unless behind a proxy that
            provides this capability.
        security : Optional[str], optional
            The security options for the server {None, "local"}. The local security
            option uses the database to cache users.
        allow_read : bool, optional
            Allow unregistered to perform GET operations on Molecule/KeywordSets/KVStore/Results/Procedures
        ssl_options : Optional[Dict[str, str]], optional
            True, automatically creates self-signed SSL certificates. False, turns off SSL entirely. A user can also supply a dictionary of valid certificates.
        storage_uri : str, optional
            The database URI that the underlying storage socket will connect to.
        storage_project_name : str, optional
            The project name to use on the database.
        query_limit : int, optional
            The maximum number of entries a query will return.
        logfile_prefix : str, optional
            The logfile to use for logging.
        loglevel : str, optional
            The level of logging to output
        queue_socket : BaseAdapter, optional
            An optional Adapter to provide for server to have limited local compute.
            Should only be used for testing and interactive sessions.
        heartbeat_frequency : float, optional
            The time (in seconds) of the heartbeat manager frequency.
        max_active_services : int, optional
            The maximum number of active Services that can be running at any given time.
        service_frequency : float, optional
            The time (in seconds) before checking and updating services.
        """
        # Save local options
        self.name = name
        self.port = port
        if ssl_options is False:
            self._address = "http://localhost:" + str(self.port) + "/"
        else:
            self._address = "https://localhost:" + str(self.port) + "/"

        self.max_active_services = max_active_services
        self.service_frequency = service_frequency
        self.heartbeat_frequency = heartbeat_frequency

        self.logger = logging.getLogger("flask.application")
        self.logger.setLevel(loglevel.upper())

        # Create API Access logger class if enables
        if log_apis:
            self.api_logger = API_AccessLogger(geo_file_path=geo_file_path)
        else:
            self.api_logger = None

        # Build security layers
        if security is None:
            storage_bypass_security = True
        elif security == "local":
            storage_bypass_security = False
        else:
            raise KeyError("Security option '{}' not recognized.".format(security))

        # Handle SSL
        ssl_ctx = None
        self.client_verify = True
        if ssl_options is True:
            self.logger.warning("No SSL files passed in, generating self-signed SSL certificate.")
            self.logger.warning("Clients must use `verify=False` when connecting.\n")

            cert, key = _build_ssl()

            # Add quick names
            ssl_name = name.lower().replace(" ", "_")
            cert_name = ssl_name + "_ssl.crt"
            key_name = ssl_name + "_ssl.key"

            ssl_options = {"crt": cert_name, "key": key_name}

            with open(cert_name, "wb") as handle:
                handle.write(cert)

            with open(key_name, "wb") as handle:
                handle.write(key)

            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(ssl_options["crt"], ssl_options["key"])

            # Destroy keyfiles upon close
            import atexit
            import os

            atexit.register(os.remove, cert_name)
            atexit.register(os.remove, key_name)
            self.client_verify = False

        elif ssl_options is False:
            ssl_ctx = None

        elif isinstance(ssl_options, dict):
            if ("crt" not in ssl_options) or ("key" not in ssl_options):
                raise KeyError("'crt' (SSL Certificate) and 'key' (SSL Key) fields are required for `ssl_options`.")

            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_ctx.load_cert_chain(ssl_options["crt"], ssl_options["key"])
        else:
            raise KeyError("ssl_options not understood")

        # Setup the database connection
        self.storage_database = storage_project_name
        self.storage_uri = storage_uri
        self.storage = storage_socket_factory(
            storage_uri,
            project_name=storage_project_name,
            bypass_security=storage_bypass_security,
            allow_read=allow_read,
            max_limit=query_limit,
            skip_version_check=skip_storage_version_check,
        )

        if view_enabled:
            self.view_handler = ViewHandler(view_path)
        else:
            self.view_handler = None

        # Build up the application
        self.objects = {
            "storage_socket": self.storage,
            "logger": self.logger,
            "api_logger": self.api_logger,
            "view_handler": self.view_handler,
        }

        # Public information
        self.objects["public_information"] = {
            "name": self.name,
            "heartbeat_frequency": self.heartbeat_frequency,
            "version": get_information("version"),
            "query_limit": self.storage.get_limit(1.0e9),
            "client_lower_version_limit": "0.12.1",  # Must be XX.YY.ZZ
            "client_upper_version_limit": "0.13.99",  # Must be XX.YY.ZZ
        }
        self.update_public_information()

        # Build the app
        app_settings = {"compress_response": compress_response}

        # Add periodic callback holders
        self.periodic = {}

        # Exit callbacks
        self.exit_callbacks = []

        self.logger.info("FractalServer:")
        self.logger.info("    Name:          {}".format(self.name))
        self.logger.info("    Version:       {}".format(get_information("version")))
        self.logger.info("    Address:       {}".format(self._address))
        self.logger.info("    Database URI:  {}".format(storage_uri))
        self.logger.info("    Database Name: {}".format(storage_project_name))
        self.logger.info("    Query Limit:   {}\n".format(self.storage.get_limit(1.0e9)))
        self.loop_active = False

        # Create a executor for background processes
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.futures = {}

        # Queue manager if direct build
        self.queue_socket = queue_socket
        if self.queue_socket is not None:
            if security == "local":
                raise ValueError("Cannot yet use local security with a internal QueueManager")

            def _build_manager():
                client = FractalClient(self, username="qcfractal_server")
                self.objects["queue_manager"] = QueueManager(
                    client,
                    self.queue_socket,
                    logger=self.logger,
                    manager_name="FractalServer",
                    cores_per_task=1,
                    memory_per_task=1,
                    verbose=False,
                )

            # Build the queue manager, will not run until loop starts
            self.futures["queue_manager_future"] = self._run_in_thread(_build_manager)

        # create flask app
        self.app = Flask(__name__)
        # config
        self.app.config['JWT_SECRET_KEY'] = 'super-secret'
        self.app.config['JWT_REFRESH_TOKEN_EXPIRES'] = 86400
        self.app.config['MAIL_SERVER'] = 'smtp.mailtrap.io'
        self.app.config['MAIL_USERNAME'] = os.environ['MAIL_USERNAME']
        self.app.config['MAIL_PASSWORD'] = os.environ['MAIL_PASSWORD']
        jwt = JWTManager(self.app)
        mail = Mail(self.app)

        # Routes
        self.app.add_url_rule('/information', view_func=self.get_information, methods=['GET'])
        self.app.add_url_rule('/register', view_func=self.register, methods=['POST'])
        self.app.add_url_rule('/login', view_func=self.login, methods=['POST'])
        self.app.add_url_rule('/refresh', view_func=self.refresh, methods=['POST'])
        self.app.add_url_rule('/fresh-login', view_func=self.fresh_login, methods=['POST'])
        self.app.add_url_rule('/molecule', view_func=self.get_molecule, methods=['GET'])
        self.app.add_url_rule('/molecule', view_func=self.post_molecule, methods=['POST'])
        self.app.add_url_rule('/kvstore', view_func=self.get_kvstore, methods=['GET'])
        self.app.add_url_rule('/kvstore', view_func=self.get_kvstore, methods=['GET'])
        self.app.add_url_rule('/collection/<int:collection_id>/<string:view_function>',
                              view_func=self.get_collection, methods=['GET'])
        self.app.add_url_rule('/collection/<int:collection_id>/<string:view_function>',
                              view_func=self.post_collection, methods=['POST'])
        self.app.add_url_rule('/collection/<int:collection_id>/<string:view_function>',
                              view_func=self.delete_collection, methods=['DELETE'])
        self.app.add_url_rule('/result/<string:query_type>',
                              view_func=self.get_result, methods=['GET'])
        self.app.add_url_rule('/wavefunctionstore',
                              view_func=self.get_wave_function, methods=['GET'])
        self.app.add_url_rule('/procedure/<string:query_type>',
                              view_func=self.get_procedure, methods=['GET'])
        self.app.add_url_rule('/optimization/<string:query_type>',
                              view_func=self.get_optimization, methods=['GET'])
        self.app.add_url_rule('/task_queue',
                              view_func=self.get_task_queue, methods=['GET'])
        self.app.add_url_rule('/task_queue',
                              view_func=self.post_task_queue, methods=['POST'])
        self.app.add_url_rule('/task_queue',
                              view_func=self.put_task_queue, methods=['PUT'])
        self.app.add_url_rule('/service_queue',
                              view_func=self.get_service_queue, methods=['GET'])
        self.app.add_url_rule('/service_queue',
                              view_func=self.post_service_queue, methods=['POST'])
        self.app.add_url_rule('/service_queue',
                              view_func=self.put_service_queue, methods=['PUT'])
        self.app.add_url_rule('/queue_manager',
                              view_func=self.post_queue_manager, methods=['Post'])
        self.app.add_url_rule('/queue_manager',
                              view_func=self.put_queue_manager, methods=['PUT'])
        self.app.add_url_rule('/manager',
                              view_func=self.get_manager, methods=['GET'])
        self.app.add_url_rule('/role', view_func=self.get_roles, methods=['GET'])
        self.app.add_url_rule('/role/<string:rolename>',
                              view_func=self.get_role, methods=['GET'])
        self.app.add_url_rule('/role', view_func=self.create_role, methods=['POST'])
        self.app.add_url_rule('/role', view_func=self.update_role, methods=['PUT'])
        self.app.add_url_rule('/role', view_func=self.delete_role, methods=['DELETE'])

        # JWT
        @jwt.user_loader_callback_loader
        def user_loader_callback(identity):
            try:
                # host_url = request.host_url
                claims = get_jwt_claims()
                resource = urlparse(request.url).path.split("/")[1]
                method = request.method
                context = {
                    "Principal": identity,
                    "Action": request.method,
                    "Resource": urlparse(request.url).path.split("/")[1],
                    "IpAddress": request.remote_addr,
                    "AccessTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                }
                policy = Policy(claims.get('permissions'))
                if policy.evaluate(context):
                    return {"identity": identity, "permissions": claims.get('permissions')}
                else:
                    return None

            except Exception as e:
                print(e)
                return None

        @jwt.user_loader_error_loader
        def custom_user_loader_error(identity):
            resource = urlparse(request.url).path.split("/")[1]
            ret = {
                "msg": "User {} is not authorized to access '{}' resource.".format(identity, resource)
            }
            return jsonify(ret), 403

    _valid_encodings = {
        "application/json": "json",
        "application/json-ext": "json-ext",
        "application/msgpack-ext": "msgpack-ext",
    }

    def register(self):
        if request.is_json:
            email = request.json['email']
            password = request.json['password']
        else:
            email = request.form['email']
            password = request.form['password']

        success = self.storage.add_user(email, password=password, rolename="user")
        if success:
            return jsonify({'message': 'New user created!'}), 201
        else:
            print("\n>>> Failed to add user. Perhaps the username is already taken?")
            return jsonify({'message': 'Failed to add user.'}), 500

    def login(self):
        if request.is_json:
            email = request.json['email']
            password = request.json['password']
        else:
            email = request.form['email']
            password = request.form['password']

        success, error_message, permissions = self.storage.verify_user(email, password)
        if success:
            access_token = create_access_token(identity=email, user_claims={"permissions": permissions})
            refresh_token = create_refresh_token(identity=email)
            return jsonify(message="Login succeeded!", access_token=access_token,
                           refresh_token=refresh_token), 200
        else:
            return jsonify(message=error_message), 401

            @self.server.route('/')
            def home_func():
                return '<h1>Success</h1>'

            # Then, you make it an object member manually:
            self.home = home_func

    @jwt_required
    def get_information(self):
        current_user = get_current_user()
        public_information = {
            "name": "self.name",
            "heartbeat_frequency": "self.heartbeat_frequency",
            "version": "version",
            "query_limit": "self.storage.get_limit(1.0e9)",
            "client_lower_version_limit": "0.12.1",
            "client_upper_version_limit": "0.13.99",
        }
        return jsonify(public_information)

    @jwt_refresh_token_required
    def refresh(self):
        email = get_jwt_identity()
        ret = {
            'access_token': create_access_token(identity=email)
        }
        return jsonify(ret), 200

    def fresh_login(self):
        if request.is_json:
            email = request.json['email']
            password = request.json['password']
        else:
            email = request.form['email']
            password = request.form['password']

        success, error_message, permissions = self.storage.verify_user(email, password)
        if success:
            access_token = create_access_token(identity=email, user_claims={"permissions": permissions}, fresh=True)
            return jsonify(message="Fresh login succeeded!", access_token=access_token), 200
        else:
            return jsonify(message=error_message), 401

    @jwt_required
    def get_molecule(self):
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
        encoding = self._valid_encodings[content_type]

        body_model, response_model = rest_model("molecule", "get")
        body = parse_bodymodel(request.json, body_model)

        molecules = self.storage.get_molecules(**{**body.data.dict(), **body.meta.dict()})
        ret = response_model(**molecules)

        if not isinstance(ret, (str, bytes)):
            data = serialize(ret, encoding)

        return data

    @jwt_required
    def post_molecule(self):
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
        encoding = self._valid_encodings[content_type]

        body_model, response_model = rest_model("molecule", "post")
        body = parse_bodymodel(request.json, body_model)

        ret = self.storage.add_molecules(body.data)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_kvstore(self):
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
        encoding = self._valid_encodings[content_type]

        body_model, response_model = rest_model("kvstore", "get")
        body = parse_bodymodel(body_model)

        ret = self.storage.get_kvstore(body.data.id)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_collection(self, collection_id: int, view_function: str):
        # List collections
        if (collection_id is None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")
            body = parse_bodymodel(body_model)

            cols = self.storage.get_collections(
                **body.data.dict(), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

        # Get specific collection
        elif (collection_id is not None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")

            body = parse_bodymodel(body_model)
            cols = self.storage.get_collections(
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

    @jwt_required
    def post_collection(self, collection_id: int, view_function: str):
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

        ret = self.storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def delete_collection(self, collection_id: int, view_function: str):
        body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
        ret = self.storage.del_collection(col_id=collection_id)
        if ret == 0:
            return jsonify(message="Collection does not exist."), 404
        else:
            response = response_model(meta={"success": True, "errors": [], "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_result(self, query_type: str):
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = self._valid_encodings[content_type]

        body_model, response_model = rest_model("procedure", query_type)
        body = parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            return jsonify(message=KeyError), 500

        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_wave_function(self):
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = self._valid_encodings[content_type]

        body_model, response_model = rest_model("wavefunctionstore", "get")
        body = parse_bodymodel(body_model)

        ret = self.storage.get_wavefunction_store(body.data.id, include=body.meta.include)
        if len(ret["data"]):
            ret["data"] = ret["data"][0]
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_procedure(self, query_type: str):
        body_model, response_model = rest_model("procedure", query_type)
        body = parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            return jsonify(message=KeyError), 500

        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_optimization(self, query_type: str):
        body_model, response_model = rest_model(f"optimization/{query_type}", "get")
        body = parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("optimization", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            return jsonify(message=KeyError), 500

        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_task_queue(self):
        body_model, response_model = rest_model("task_queue", "get")
        body = parse_bodymodel(body_model)

        tasks = self.storage.get_queue(**{**body.data.dict(), **body.meta.dict()})
        response = response_model(**tasks)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def post_task_queue(self):
        body_model, response_model = rest_model("task_queue", "post")
        body = parse_bodymodel(body_model)

        # Format and submit tasks
        if not check_procedure_available(body.meta.procedure):
            return jsonify(message="Unknown procedure {}.".format(body.meta.procedure)), 500

        procedure_parser = get_procedure_parser(body.meta.procedure, storage, logger)

        # Verify the procedure
        verify = procedure_parser.verify_input(body)
        if verify is not True:
            return jsonify(message="Verify error"), 400

        payload = procedure_parser.submit_tasks(body)
        response = response_model(**payload)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def put_task_queue(self):
        body_model, response_model = rest_model("task_queue", "put")
        body = parse_bodymodel(body_model)

        if (body.data.id is None) and (body.data.base_result is None):
            return jsonify(message="Id or ResultId must be specified."), 400
        if body.meta.operation == "restart":
            tasks_updated = self.storage.queue_reset_status(**body.data.dict(), reset_error=True)
            data = {"n_updated": tasks_updated}
        else:
            return jsonify(message="Operation '{operation}' is not valid."), 400

        response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_service_queue(self):
        body_model, response_model = rest_model("service_queue", "get")
        body = parse_bodymodel(body_model)

        ret = self.storage.get_services(**{**body.data.dict(), **body.meta.dict()})
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def post_service_queue(self):
        """Posts new services to the service queue."""

        body_model, response_model = rest_model("service_queue", "post")
        body = parse_bodymodel(body_model)

        new_services = []
        for service_input in body.data:
            # Get molecules with ids
            if isinstance(service_input.initial_molecule, list):
                molecules = self.storage.get_add_molecules_mixed(service_input.initial_molecule)["data"]
                if len(molecules) != len(service_input.initial_molecule):
                    return jsonify(message=KeyError), 500
            else:
                molecules = self.storage.get_add_molecules_mixed([service_input.initial_molecule])["data"][0]

            # Update the input and build a service object
            service_input = service_input.copy(update={"initial_molecule": molecules})
            new_services.append(
                initialize_service(
                    storage, logger, service_input, tag=body.meta.tag, priority=body.meta.priority
                )
            )

        ret = self.storage.add_services(new_services)
        ret["data"] = {"ids": ret["data"], "existing": ret["meta"]["duplicates"]}
        ret["data"]["submitted"] = list(set(ret["data"]["ids"]) - set(ret["meta"]["duplicates"]))
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def put_service_queue(self):
        """Modifies services in the service queue"""

        body_model, response_model = rest_model("service_queue", "put")
        body = parse_bodymodel(body_model)

        if (body.data.id is None) and (body.data.procedure_id is None):
            return jsonify(message="Id or ProcedureId must be specified."), 400

        if body.meta.operation == "restart":
            updates = self.storage.update_service_status("running", **body.data.dict())
            data = {"n_updated": updates}
        else:
            return jsonify(message="Operation '{operation}' is not valid."), 400

        response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def _get_name_from_metadata(meta):
        """
        Form the canonical name string.
        """
        ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
        return ret

    def insert_complete_tasks(storage_socket, results, logger):
        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(list)

        queue = self.storage_socket.get_queue(id=list(results.keys()))["data"]
        queue = {v.id: v for v in queue}

        error_data = []

        task_success = 0
        task_failures = 0
        task_totals = len(results.items())
        for key, result in results.items():
            try:
                # Successful task
                if result["success"] is False:
                    if "error" not in result:
                        error = {"error_type": "not_supplied", "error_message": "No error message found on task."}
                    else:
                        error = result["error"]

                    logger.warning(
                        "Computation key {key} did not complete successfully:\n"
                        "error_type: {error_type}\nerror_message: {error_message}".format(key=str(key), **error)
                    )

                    error_data.append((key, error))
                    task_failures += 1

                # Failed task
                elif key not in queue:
                    logger.warning(f"Computation key {key} completed successfully, but not found in queue.")
                    error_data.append((key, "Internal Error: Queue key not found."))
                    task_failures += 1

                # Success!
                else:
                    parser = queue[key].parser
                    new_results[parser].append(
                        {"result": result, "task_id": key, "base_result": queue[key].base_result}
                    )
                    task_success += 1

            except Exception:
                msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                logger.warning("update: ERROR\n{}".format(msg))
                error_data.append((key, msg))
                task_failures += 1

        if task_totals:
            logger.info(
                "QueueManager: Found {} complete tasks ({} successful, {} failed).".format(
                    task_totals, task_success, task_failures
                )
            )

        # Run output parsers
        completed = []
        for k, v in new_results.items():
            procedure_parser = get_procedure_parser(k, storage_socket, logger)
            com, err, hks = procedure_parser.parse_output(v)
            completed.extend(com)
            error_data.extend(err)

        # Handle complete tasks
        storage_socket.queue_mark_complete(completed)
        storage_socket.queue_mark_error(error_data)
        return len(completed), len(error_data)

        def get_queue_manager(sefl):
            """Pulls new tasks from the task queue"""

            body_model, response_model = rest_model("queue_manager", "get")
            body = parse_bodymodel(body_model)

            # Figure out metadata and kwargs
            name = _get_name_from_metadata(body.meta)

            # Grab new tasks and write out
            new_tasks = self.storage.queue_get_next(
                name, body.meta.programs, body.meta.procedures, limit=body.data.limit, tag=body.meta.tag
            )
            response = response_model(
                **{
                    "meta": {
                        "n_found": len(new_tasks),
                        "success": True,
                        "errors": [],
                        "error_description": "",
                        "missing": [],
                    },
                    "data": new_tasks,
                }
            )
            # Update manager logs
            storage.manager_update(name, submitted=len(new_tasks), **body.meta.dict())
            if not isinstance(response, (str, bytes)):
                data = serialize(response, encoding)

            return data

    @jwt_required
    def post_queue_manager(self):
        """Posts complete tasks to the task queue"""

        body_model, response_model = rest_model("queue_manager", "post")
        body = parse_bodymodel(body_model)

        name = _get_name_from_metadata(body.meta)
        # logger.info("QueueManager: Received completed task packet from {}.".format(name))
        success, error = insert_complete_tasks(storage, body.data, logger)

        completed = success + error

        response = response_model(
            **{
                "meta": {
                    "n_inserted": completed,
                    "duplicates": [],
                    "validation_errors": [],
                    "success": True,
                    "errors": [],
                    "error_description": "",
                },
                "data": True,
            }
        )

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def put_queue_manager(self):
        """
        Various manager manipulation operations
        """

        ret = True

        body_model, response_model = rest_model("queue_manager", "put")
        body = parse_bodymodel(body_model)

        name = _get_name_from_metadata(body.meta)
        op = body.data.operation
        if op == "startup":
            storage.manager_update(
                name, status="ACTIVE", configuration=body.data.configuration, **body.meta.dict(), log=True
            )
            # logger.info("QueueManager: New active manager {} detected.".format(name))

        elif op == "shutdown":
            nshutdown = self.storage.queue_reset_status(manager=name, reset_running=True)
            storage.manager_update(name, returned=nshutdown, status="INACTIVE", **body.meta.dict(), log=True)

            # logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(name, nshutdown))

            ret = {"nshutdown": nshutdown}

        elif op == "heartbeat":
            storage.manager_update(name, status="ACTIVE", **body.meta.dict(), log=True)
            # logger.debug("QueueManager: Heartbeat of manager {} detected.".format(name))

        else:
            msg = "Operation '{}' not understood.".format(op)
            return jsonify(message=msg), 400

        response = response_model(**{"meta": {}, "data": ret})
        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_manager(self):
        """Gets manager information from the task queue"""

        body_model, response_model = rest_model("manager", "get")
        body = parse_bodymodel(body_model)

        # logger.info("GET: ComputeManagerHandler")
        managers = self.storage.get_managers(**{**body.data.dict(), **body.meta.dict()})

        # remove passwords?
        # TODO: Are passwords stored anywhere else? Other kinds of passwords?
        for m in managers["data"]:
            if "configuration" in m and isinstance(m["configuration"], dict) and "server" in m["configuration"]:
                m["configuration"]["server"].pop("password", None)

        response = response_model(**managers)
        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def get_roles(self):
        roles = self.storage.get_roles()
        return jsonify(roles), 200

    @jwt_required
    def get_role(self, rolename: str):

        success, role = self.storage.get_role(rolename)
        return jsonify(role), 200

    @jwt_required
    def create_role(self):
        rolename = request.json['rolename']
        permissions = request.json['permissions']

        success, error_message = self.storage.create_role(rolename, permissions)
        if success:
            return jsonify({'message': 'New role created!'}), 201
        else:
            return jsonify({'message': error_message}), 400

    @fresh_jwt_required
    def update_role(self):
        rolename = request.json['rolename']
        permissions = request.json['permissions']

        success = self.storage.update_role(rolename, permissions)
        if success:
            return jsonify({'message': 'Role was updated!'}), 200
        else:
            return jsonify({'message': 'Failed to update role'}), 400

    @fresh_jwt_required
    def delete_role(self):
        rolename = request.json['rolename']

        success = self.storage.delete_role(rolename)
        if success:
            return jsonify({'message': 'Role was deleted!.'}), 200
        else:
            return jsonify({'message': 'Filed to delete role!.'}), 400

    def start(self):
        self.app.run(port=self.port)

    def __repr__(self):

        return f"FractalServer(name='{self.name}' uri='{self._address}')"

    def _run_in_thread(self, func, timeout=5):
        """
        Runs a function in a background thread
        """
        if self.executor is None:
            raise AttributeError("No Executor was created, but run_in_thread was called.")

        fut = self.loop.run_in_executor(self.executor, func)
        return fut

    # Start/stop functionality
    def start_old(self, start_loop: bool = True, start_periodics: bool = True) -> None:
        """
        Starts up the IOLoop and periodic calls.

        Parameters
        ----------
        start_loop : bool, optional
            If False, does not start the IOLoop
        start_periodics : bool, optional
            If False, does not start the server periodic updates such as
            Service iterations and Manager heartbeat checking.
        """
        if "queue_manager_future" in self.futures:

            def start_manager():
                self._check_manager("manager_build")
                self.objects["queue_manager"].start()

            # Call this after the loop has started
            self._run_in_thread(start_manager)

        # Add services callback
        if start_periodics:
            nanny_services = tornado.ioloop.PeriodicCallback(self.update_services, self.service_frequency * 1000)
            nanny_services.start()
            self.periodic["update_services"] = nanny_services

            # Check Manager heartbeats, 5x heartbeat frequency
            heartbeats = tornado.ioloop.PeriodicCallback(
                self.check_manager_heartbeats, self.heartbeat_frequency * 1000 * 0.2
            )
            heartbeats.start()
            self.periodic["heartbeats"] = heartbeats

            # Log can take some time, update in thread
            def run_log_update_in_thread():
                self._run_in_thread(self.update_server_log)

            server_log = tornado.ioloop.PeriodicCallback(run_log_update_in_thread, self.heartbeat_frequency * 1000)

            server_log.start()
            self.periodic["server_log"] = server_log

        # Build callbacks which are always required
        public_info = tornado.ioloop.PeriodicCallback(self.update_public_information, self.heartbeat_frequency * 1000)
        public_info.start()
        self.periodic["public_info"] = public_info

        # Soft quit with a keyboard interrupt
        self.logger.info("FractalServer successfully started.\n")
        if start_loop:
            self.loop_active = True
            self.loop.start()

    def stop(self, stop_loop: bool = True) -> None:
        """
        Shuts down the IOLoop and periodic updates.

        Parameters
        ----------
        stop_loop : bool, optional
            If False, does not shut down the IOLoop. Useful if the IOLoop is externally managed.
        """

        # Shut down queue manager
        if "queue_manager" in self.objects:
            self._run_in_thread(self.objects["queue_manager"].stop)

        # Close down periodics
        for cb in self.periodic.values():
            cb.stop()

        # Call exit callbacks
        for func, args, kwargs in self.exit_callbacks:
            func(*args, **kwargs)

        # Shutdown executor and futures
        for k, v in self.futures.items():
            v.cancel()

        if self.executor is not None:
            self.executor.shutdown()

        # Final: shutdown flask server
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    def add_exit_callback(self, callback, *args, **kwargs):
        """Adds additional callbacks to perform when closing down the server.

        Parameters
        ----------
        callback : callable
            The function to call at exit
        *args
            Arguments to call with the function.
        **kwargs
            Kwargs to call with the function.

        """
        self.exit_callbacks.append((callback, args, kwargs))

    # Helpers
    def get_address(self, endpoint: Optional[str] = None) -> str:
        """Obtains the full URI for a given function on the FractalServer.

        Parameters
        ----------
        endpoint : Optional[str], optional
            Specifies a endpoint to provide the URI for. If None returns the server address.

        Returns
        -------
        str
            The endpoint URI

        """

        if endpoint and (endpoint not in self.endpoints):
            raise AttributeError("Endpoint '{}' not found.".format(endpoint))

        if endpoint:
            return self._address + endpoint
        else:
            return self._address

    # Updates
    def update_services(self) -> int:
        """Runs through all active services and examines their current status."""

        # Grab current services
        current_services = self.storage.get_services(status="RUNNING")["data"]

        # Grab new services if we have open slots
        open_slots = max(0, self.max_active_services - len(current_services))
        if open_slots > 0:
            new_services = self.storage.get_services(status="WAITING", limit=open_slots)["data"]
            current_services.extend(new_services)
            if len(new_services):
                self.logger.info(f"Starting {len(new_services)} new services.")

        self.logger.debug(f"Updating {len(current_services)} services.")

        # Loop over the services and iterate
        running_services = 0
        completed_services = []
        for data in current_services:

            # Attempt to iteration and get message
            try:
                service = construct_service(self.storage, self.logger, data)
                finished = service.iterate()
            except Exception:
                error_message = "FractalServer Service Build and Iterate Error:\n{}".format(traceback.format_exc())
                self.logger.error(error_message)
                service.status = "ERROR"
                service.error = {"error_type": "iteration_error", "error_message": error_message}
                finished = False

            self.storage.update_services([service])

            # Mark procedure and service as error
            if service.status == "ERROR":
                self.storage.update_service_status("ERROR", id=service.id)

            if finished is not False:
                # Add results to procedures, remove complete_ids
                completed_services.append(service)
            else:
                running_services += 1

        if len(completed_services):
            self.logger.info(f"Completed {len(completed_services)} services.")

        # Add new procedures and services
        self.storage.services_completed(completed_services)

        return running_services

    def update_server_log(self) -> Dict[str, Any]:
        """
        Updates the servers internal log
        """

        return self.storage.log_server_stats()

    def update_public_information(self) -> None:
        """
        Updates the public information data
        """
        data = self.storage.get_server_stats_log(limit=1)["data"]

        counts = {"collection": 0, "molecule": 0, "result": 0, "kvstore": 0}
        if len(data):
            counts["collection"] = data[0].get("collection_count", 0)
            counts["molecule"] = data[0].get("molecule_count", 0)
            counts["result"] = data[0].get("result_count", 0)
            counts["kvstore"] = data[0].get("kvstore_count", 0)

        update = {"counts": counts}
        self.objects["public_information"].update(update)

    def check_manager_heartbeats(self) -> None:
        """
        Checks the heartbeats and kills off managers that have not been heard from.
        """

        dt = datetime.utcnow() - timedelta(seconds=self.heartbeat_frequency)
        ret = self.storage.get_managers(status="ACTIVE", modified_before=dt)

        for blob in ret["data"]:
            nshutdown = self.storage.queue_reset_status(manager=blob["name"], reset_running=True)
            self.storage.manager_update(blob["name"], returned=nshutdown, status="INACTIVE")

            self.logger.info(
                "Hearbeat missing from {}. Shutting down, recycling {} incomplete tasks.".format(
                    blob["name"], nshutdown
                )
            )

    def list_managers(self, status: Optional[str] = None, name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Provides a list of managers associated with the server both active and inactive.

        Parameters
        ----------
        status : Optional[str], optional
            Filters managers by status.
        name : Optional[str], optional
            Filters managers by name

        Returns
        -------
        List[Dict[str, Any]]
            The requested Manager data.
        """

        return self.storage.get_managers(status=status, name=name)["data"]

    def client(self):
        """
        Builds a client from this server.
        """

        return FractalClient(self)

    # Functions only available if using a local queue_adapter

    def _check_manager(self, func_name: str) -> None:
        if self.queue_socket is None:
            raise AttributeError(
                "{} is only available if the server was initialized with a queue manager.".format(func_name)
            )

        # Wait up to one second for the queue manager to build
        if "queue_manager" not in self.objects:
            self.logger.info("Waiting on queue_manager to build.")
            for x in range(20):
                time.sleep(0.1)
                if "queue_manager" in self.objects:
                    break

            if "queue_manager" not in self.objects:
                raise AttributeError("QueueManager never constructed.")

    def update_tasks(self) -> bool:
        """Pulls tasks from the queue_adapter, inserts them into the database,
        and fills the queue_adapter with new tasks.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """
        self._check_manager("update_tasks")

        if self.loop_active:
            # Drop this in a thread so that we are not blocking each other
            self._run_in_thread(self.objects["queue_manager"].update)
        else:
            self.objects["queue_manager"].update()

        return True

    def await_results(self) -> bool:
        """A synchronous method for testing or small launches
        that awaits task completion before adding all queued results
        to the database and returning.

        Returns
        -------
        bool
            Return True if the operation completed successfully
        """
        self._check_manager("await_results")

        self.logger.info("Updating tasks")
        return self.objects["queue_manager"].await_results()

    def await_services(self, max_iter: int = 10) -> bool:
        """A synchronous method that awaits the completion of all services
        before returning.

        Parameters
        ----------
        max_iter : int, optional
            The maximum number of service iterations the server will run through. Will
            terminate early if all services have completed.

        Returns
        -------
        bool
            Return True if the operation completed successfully

        """
        self._check_manager("await_services")

        self.await_results()
        for x in range(1, max_iter + 1):
            self.logger.info("\nAwait services: Iteration {}\n".format(x))
            running_services = self.update_services()
            self.await_results()
            if running_services == 0:
                break

        return True

    def list_current_tasks(self) -> List[Any]:
        """Provides a list of tasks currently in the queue along
        with the associated keys.

        Returns
        -------
        ret : list of tuples
            All tasks currently still in the database
        """
        self._check_manager("list_current_tasks")

        return self.objects["queue_manager"].list_current_tasks()


if __name__ == '__main__':
    try:
        server = FractalServer()
        server.start()
    except Exception as e:
        print("Fatal during server startup:\n")
        print(str(e))
        print("\nFailed to start the server, shutting down.")
