from __future__ import annotations

import importlib
import logging
import queue
from typing import TYPE_CHECKING

from flask import Flask, current_app
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from werkzeug.local import LocalProxy
from werkzeug.routing import IntegerConverter

from .flask_session import QCFFlaskSessionInterface
from .flask_socket import FlaskStorageSocket
from .home_v1 import home_v1
from ..db_socket import SQLAlchemySocket

if TYPE_CHECKING:
    from ..config import FractalConfig
    from typing import Optional

app_storage_sockets = FlaskStorageSocket()


def _get_storage_socket() -> SQLAlchemySocket:
    return app_storage_sockets.get_socket(current_app)


storage_socket = LocalProxy(_get_storage_socket)

jwt = JWTManager()


# Some routes allow for negative integers (ie, list index)
# See https://github.com/pallets/flask/issues/2643
class SignedIntConverter(IntegerConverter):
    regex = r"-?\d+"


def create_flask_app(qcfractal_config: FractalConfig, finished_queue: Optional[queue.Queue] = None):
    app = Flask(__name__)

    app.url_map.converters["signed_int"] = SignedIntConverter

    app.logger = logging.getLogger("fractal_flask_app")
    app.logger.info(f"Creating flask app")

    # Read in and store the qcfractal configuration for later use
    app.config["QCFRACTAL_CONFIG"] = qcfractal_config

    # Configure the flask app

    # Some defaults (but can be overridden)
    # must be set to false to avoid restarting
    app.config["DEBUG"] = False

    # Never propagate exceptions. This uses the default error pages
    # which are HTML, but we are using json...
    app.config["PROPAGATE_EXCEPTIONS"] = False

    app.config["SECRET_KEY"] = qcfractal_config.api.secret_key
    app.config["JWT_SECRET_KEY"] = qcfractal_config.api.jwt_secret_key
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = qcfractal_config.api.jwt_access_token_expires
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = qcfractal_config.api.jwt_refresh_token_expires
    app.config["JWT_TOKEN_LOCATION"] = ["headers", "cookies"]
    app.config["SESSION_COOKIE_NAME"] = qcfractal_config.api.user_session_cookie_name
    app.config["PERMANENT_SESSION_LIFETIME"] = qcfractal_config.api.user_session_max_age

    # Where we store user-uploaded files for processing
    app.config["UPLOAD_FOLDER"] = qcfractal_config.upload_directory

    # Any additional configuration
    if qcfractal_config.api.extra_flask_options:
        app.config.update(**qcfractal_config.api.extra_flask_options)

    jwt.init_app(app)

    if qcfractal_config.cors.enabled:
        app.config["CORS_ORIGINS"] = qcfractal_config.cors.origins
        app.config["CORS_SUPPORTS_CREDENTIALS"] = qcfractal_config.cors.supports_credentials
        app.config["CORS_HEADERS"] = qcfractal_config.cors.headers
        CORS(app)

    # Initialize the database socket, API logger, and view handler
    app_storage_sockets.init_app(app, finished_queue=finished_queue)

    # Initialize the session interface after the storage socket
    app.session_interface = QCFFlaskSessionInterface(app)

    # Registers the various error and before/after request handlers
    importlib.import_module("qcfractal.flask_app.handlers")

    # Register all the routes in the other files.
    # Must be done before registering the blueprint
    importlib.import_module("qcfractal.flask_app.api_v1.routes")
    importlib.import_module("qcfractal.flask_app.auth_v1.routes")
    importlib.import_module("qcfractal.flask_app.compute_v1.routes")
    importlib.import_module("qcfractal.components.register_all")

    from .auth_v1.blueprint import auth_v1
    from .api_v1.blueprint import api_v1
    from .compute_v1.blueprint import compute_v1

    app.register_blueprint(home_v1)
    app.register_blueprint(api_v1)
    app.register_blueprint(auth_v1)
    app.register_blueprint(compute_v1)

    return app
