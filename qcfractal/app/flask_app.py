from __future__ import annotations

from flask import Flask
import multiprocessing

from .config import config
# from flask_cors import CORS
from flask_jwt_extended import JWTManager
import logging

from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from qcfractal.storage_sockets import ViewHandler, API_AccessLogger
from qcfractal.fractal_proc import FractalProcessBase

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..config import FractalConfig
    from typing import Optional

storage_socket = SQLAlchemySocket()
api_logger = API_AccessLogger()
view_handler = ViewHandler()

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
    storage_socket.init_app(qcfractal_config)
    api_logger.init_app(qcfractal_config)
    view_handler.init_app(qcfractal_config)

    app.config.JWT_ENABLED = qcfractal_config.enable_security
    app.config.ALLOW_UNAUTHENTICATED_READ = qcfractal_config.allow_unauthenticated_read

    #logger.debug("Adding blueprints..")

    # The main application entry
    from .routes import main

    app.register_blueprint(main)

    return app


class FractalFlaskProcess(FractalProcessBase):
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



#    def wait_for_results(self, ids, timeout=None) -> bool:
#        logger = logging.getLogger(__name__)
#
#        if self._completed_queue is None:
#            raise RuntimeError("Cannot wait for results when the completed queue is not enabled. See the 'enable_watching' argument to the constructor")
#
#        ids = set(int(x) for x in ids)
#
#        while len(ids) > 0:
#            # The queue stores a tuple of (id, type, status)
#            try:
#                base_result_info = self._completed_queue.get(True, timeout)
#            except self._mp_ctx.Queue.Empty as e:
#                logger.debug(f'No tasks finished in {timeout} seconds')
#                return False
#
#            logger.debug("Task finished: id={}, type={}, status={}".format(*base_result_info))
#            self._all_completed.append(base_result_info[0])
#            ids.remove(base_result_info[0])
#
#        return True

    def run(self):
        flask_app = create_qcfractal_flask_app(self._qcf_config)

        if self._completed_queue is not None:
            # Get the global storage socket and set up the queue
            from .flask_app import storage_socket as flask_storage_socket
            flask_storage_socket.set_completed_watch(self._completed_queue)

        flask_app.run(host=self._qcf_config.flask.bind, port=self._qcf_config.flask.port)

    def finalize(self) -> None:
        pass