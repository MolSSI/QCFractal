from __future__ import annotations

import os
from flask import Flask
import multiprocessing

from .config import config
# from flask_cors import CORS
from flask_jwt_extended import JWTManager
import logging

from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from qcfractal.storage_sockets import ViewHandler, API_AccessLogger
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

class _FlaskAPILogger(API_AccessLogger):
    def __init__(self):
        pass

    def init(self, qcf_config):
        API_AccessLogger.__init__(self, qcf_config)

class _FlaskViewHandler(ViewHandler):
    def __init__(self):
        pass

    def init(self, qcf_config):
        ViewHandler.__init__(self, qcf_config)


storage_socket = _FlaskSQLAlchemySocket()
api_logger = _FlaskAPILogger()
view_handler = _FlaskViewHandler()

jwt = JWTManager()
# cors = CORS()

def create_qcfractal_flask_app(qcfractal_config: FractalConfig):
    config_name = qcfractal_config.flask.config_name

    app = Flask(__name__)
    app.logger = logging.getLogger('fractal_flask_app')
    app.logger.info(f"Creating app with config '{config_name}'")

    # Load the defaults for the Flask configuration
    app.config.from_object(config[config_name])

    config[config_name].init_app(app)

    # Read in and store the qcfractal configuration for later use
    app.config["QCFRACTAL_CONFIG"] = qcfractal_config

    # cors.init_app(app)
    jwt.init_app(app)

    # Initialize the database socket, API logger, and view handler
    storage_socket.init(qcfractal_config)
    api_logger.init(qcfractal_config)
    view_handler.init(qcfractal_config)

    app.config.JWT_ENABLED = qcfractal_config.enable_security
    app.config.ALLOW_UNAUTHENTICATED_READ = qcfractal_config.allow_unauthenticated_read

    #logger.debug("Adding blueprints..")

    # The main application entry
    from .routes import main

    app.register_blueprint(main)

    return app


class FlaskProcess(ProcessBase):
    """
    Flask running in a separate process
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
            completed_queue: Optional[multiprocessing.Queue] = None
    ):
        self._qcf_config = qcf_config
        self._completed_queue = completed_queue


    def run(self):
        flask_app = create_qcfractal_flask_app(self._qcf_config)

        # Get the global storage socket and set up the queue
        storage_socket.set_completed_watch(self._completed_queue)

        # Disable printing "Environment: ... WARNING: This is a development server...
        os.environ['WERKZEUG_RUN_MAIN'] = 'true'

        flask_app.run(host=self._qcf_config.flask.bind, port=self._qcf_config.flask.port)

    def finalize(self) -> None:
        pass
