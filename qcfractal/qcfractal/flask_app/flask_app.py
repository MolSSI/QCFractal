from __future__ import annotations

import importlib
import logging
import queue
import threading
from typing import TYPE_CHECKING

from flask import Flask
from flask_jwt_extended import JWTManager
from werkzeug.routing import IntegerConverter
from werkzeug.serving import make_server

from qcfractal.db_socket.socket import SQLAlchemySocket
from .home import home_blueprint
from ..api_v1.blueprint import api_v1
from ..auth_v1.blueprint import auth_v1
from ..dashboard_v1.blueprint import dashboard_v1

if TYPE_CHECKING:
    from ..config import FractalConfig
    from typing import Optional


class _FlaskSQLAlchemySocket(SQLAlchemySocket):
    def __init__(self):
        pass

    def init(self, qcf_config):
        SQLAlchemySocket.__init__(self, qcf_config)


storage_socket = _FlaskSQLAlchemySocket()

jwt = JWTManager()

# Some routes allow for negative integers (ie, list index)
# See https://github.com/pallets/flask/issues/2643
class SignedIntConverter(IntegerConverter):
    regex = r"-?\d+"


def create_flask_app(qcfractal_config: FractalConfig, init_storage: bool = True):
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

    # Any additional configuration
    if qcfractal_config.api.extra_flask_options:
        app.config.update(**qcfractal_config.api.extra_flask_options)

    jwt.init_app(app)

    if init_storage:
        # Initialize the database socket, API logger, and view handler
        storage_socket.init(qcfractal_config)

    # Registers the various error and before/after request handlers
    importlib.import_module("qcfractal.flask_app.handlers")

    # Register all the routes in the other files.
    # Must be done before registering the blueprint
    importlib.import_module("qcfractal.api_v1.routes")
    importlib.import_module("qcfractal.auth_v1.routes")
    importlib.import_module("qcfractal.dashboard_v1.routes")
    importlib.import_module("qcfractal.components.register_all")

    app.register_blueprint(home_blueprint)
    app.register_blueprint(api_v1)
    app.register_blueprint(auth_v1)
    app.register_blueprint(dashboard_v1)

    return app


def create_flask_app_dummy():
    from ..config import FractalConfig

    cfg = {
        "base_folder": "/tmp",
        "api": {
            "secret_key": "abcd",
            "jwt_secret_key": "abcd",
        },
    }

    qcf_cfg = FractalConfig(**cfg)
    return create_flask_app(qcf_cfg, init_storage=False)


# From https://stackoverflow.com/questions/15562446/how-to-stop-flask-application-without-using-ctrl-c/45017691#45017691
class SimpleFlask:
    def __init__(
        self,
        qcf_config: FractalConfig,
        finished_queue: Optional[queue.Queue] = None,
        started_event: Optional[threading.Event] = None,
    ):
        self.started_event = started_event
        self.app = create_flask_app(qcf_config)

        if finished_queue is not None:
            storage_socket.set_finished_watch(finished_queue)

        self.server = make_server(qcf_config.api.host, qcf_config.api.port, self.app)

    def start(self):
        if self.started_event is not None:
            with self.app.app_context():
                self.started_event.set()

        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()
