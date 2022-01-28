from __future__ import annotations

import importlib
import logging
import multiprocessing
import os
import sys
from typing import TYPE_CHECKING

from flask import Flask, Blueprint
from flask_jwt_extended import JWTManager

from qcfractal.db_socket.socket import SQLAlchemySocket
from qcfractal.process_runner import ProcessBase
from .config import config

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

main = Blueprint("main", __name__)


def create_qcfractal_flask_app(qcfractal_config: FractalConfig):
    config_name = qcfractal_config.api.config_name

    app = Flask(__name__)
    app.logger = logging.getLogger("fractal_flask_app")
    app.logger.info(f"Creating app with config '{config_name}'")

    # Load the defaults for the Flask configuration
    app.config.from_object(config[config_name])

    config[config_name].init_app(app)

    # Read in and store the qcfractal configuration for later use
    app.config["QCFRACTAL_CONFIG"] = qcfractal_config

    jwt.init_app(app)

    # Initialize the database socket, API logger, and view handler
    storage_socket.init(qcfractal_config)

    app.config["SECRET_KEY"] = qcfractal_config.api.secret_key
    app.config["JWT_SECRET_KEY"] = qcfractal_config.api.jwt_secret_key
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = qcfractal_config.api.jwt_access_token_expires
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = qcfractal_config.api.jwt_refresh_token_expires

    # Register all the routes in the other files.
    # Must be done before registering the blueprint
    importlib.import_module("qcfractal.components.register_all")
    importlib.import_module("qcfractal.app.routes")

    app.register_blueprint(main)

    return app


class FlaskProcess(ProcessBase):
    """
    Flask running in a separate process
    """

    def __init__(
        self,
        qcf_config: FractalConfig,
        completed_queue: Optional[multiprocessing.Queue] = None,
        running_event: Optional[multiprocessing.synchronize.Event] = None,
    ):
        self._qcf_config = qcf_config
        self._completed_queue = completed_queue
        self._running_event = running_event

    def setup(self):
        self._flask_app = create_qcfractal_flask_app(self._qcf_config)

        # Get the global storage socket and set up the queue
        storage_socket.set_completed_watch(self._completed_queue)

        # Disable printing "Environment: ... WARNING: This is a development server...
        os.environ["WERKZEUG_RUN_MAIN"] = "true"

        # Get the werkzeug logger to shut up by setting its level to the root level
        # I don't know what flask does but it seems to override it to INFO if not set
        # on this particular logger
        logging.getLogger("werkzeug").setLevel(logging.getLogger().level)

    def run(self):
        # see https://stackoverflow.com/a/55573732
        with self._flask_app.app_context():
            if self._running_event is not None:
                self._running_event.set()

        self._flask_app.run(host=self._qcf_config.api.host, port=self._qcf_config.api.port)

    def interrupt(self) -> None:
        # We got here via SIGINT or SIGTERM. Convert both to SIGTERM and let flask handle it
        logging.getLogger(__name__).debug("Exiting flask process")
        sys.exit(0)
