from __future__ import annotations

import os
import sys

from flask import Flask
import multiprocessing

from .config import config

from flask_jwt_extended import JWTManager
import logging

from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from qcfractal.storage_sockets import ViewHandler
from qcfractal.process_runner import ProcessBase

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import FractalConfig
    from typing import Optional


class _FlaskSQLAlchemySocket(SQLAlchemySocket):
    def __init__(self):
        pass

    def init(self, qcf_config):
        SQLAlchemySocket.__init__(self, qcf_config)


class _FlaskViewHandler(ViewHandler):
    def __init__(self):
        pass

    def init(self, qcf_config):
        ViewHandler.__init__(self, qcf_config)


storage_socket = _FlaskSQLAlchemySocket()
view_handler = _FlaskViewHandler()

jwt = JWTManager()


def create_qcfractal_flask_app(qcfractal_config: FractalConfig):
    config_name = qcfractal_config.flask.config_name

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
    view_handler.init(qcfractal_config)

    app.config["JWT_ENABLED"] = qcfractal_config.enable_security
    app.config["ALLOW_UNAUTHENTICATED_READ"] = qcfractal_config.allow_unauthenticated_read

    app.config["SECRET_KEY"] = qcfractal_config.flask.secret_key
    app.config["JWT_SECRET_KEY"] = qcfractal_config.flask.jwt_secret_key
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = qcfractal_config.flask.jwt_access_token_expires
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = qcfractal_config.flask.jwt_refresh_token_expires

    # logger.debug("Adding blueprints..")

    # Register all the routes in the other files
    from ..components.molecule import routes
    from ..components.outputstore import routes
    from ..components.wavefunction import routes
    from ..components.keywords import routes

    from .routes import collections
    from .routes import collections
    from .routes import manager
    from .routes import manager_info
    from .routes import optimization
    from .routes import permissions
    from .routes import records
    from .routes import server_info
    from .routes import service
    from .routes import singlepoint
    from .routes import tasks
    from .routes import users
    from .routes import main

    app.register_blueprint(main.main)

    return app


class FlaskProcess(ProcessBase):
    """
    Flask running in a separate process
    """

    def __init__(self, qcf_config: FractalConfig, completed_queue: Optional[multiprocessing.Queue] = None):
        self._qcf_config = qcf_config
        self._completed_queue = completed_queue

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
        self._flask_app.run(host=self._qcf_config.flask.host, port=self._qcf_config.flask.port)

    def interrupt(self) -> None:
        # We got here via SIGINT or SIGTERM. Convert both to SIGTERM and let flask handle it
        logging.getLogger(__name__).debug("Exiting flask process")
        sys.exit(0)
