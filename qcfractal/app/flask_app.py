from __future__ import annotations

from flask import Flask
import multiprocessing

from .config import config
# from flask_cors import CORS
from flask_jwt_extended import JWTManager
import logging
import traceback
import signal
import os

from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from qcfractal.storage_sockets import ViewHandler, API_AccessLogger

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..config import FractalConfig

storage_socket = SQLAlchemySocket()
api_logger = API_AccessLogger()
view_handler = ViewHandler()

jwt = JWTManager()
# cors = CORS()

# Signalling of process termination via an exception
class EndProcess(RuntimeError):
    pass

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



class FractalFlaskProcess:
    """
    Creates a flask app in a separate process, and allows for control
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
            mp_context: multiprocessing.context.BaseContext,
            log_queue: multiprocessing.queues.Queue,
            start: bool = True,
            enable_watching: bool = False
    ):
        self._qcf_config = qcf_config
        self._mp_ctx = mp_context

        # Store logs into a queue. This will be passed to the subprocesses, and they
        # will log to this
        self._log_queue = log_queue
        self._completed_queue = None

        if enable_watching:
            self._completed_queue = self._mp_ctx.Queue()
            self._all_completed = []

        self._flask_process = self._mp_ctx.Process(name="flask_proc", target=FractalFlaskProcess._run_flask, args=(self._qcf_config, self._log_queue, self._completed_queue))
        if start:
            self.start()

    def start(self):
        if self._flask_process.is_alive():
            raise RuntimeError("Flask process is already running")
        else:
            self._flask_process.start()


    def stop(self):
        self._flask_process.terminate()
        self._flask_process.join()

    def __del__(self):
        self.stop()

    def wait_for_results(self, ids, timeout=None) -> bool:
        logger = logging.getLogger(__name__)

        if self._completed_queue is None:
            raise RuntimeError("Cannot wait for results when the completed queue is not enabled. See the 'enable_watching' argument to the constructor")

        ids = set(int(x) for x in ids)

        while len(ids) > 0:
            # The queue stores a tuple of (id, type, status)
            try:
                base_result_info = self._completed_queue.get(True, timeout)
            except self._mp_ctx.Queue.Empty as e:
                logger.debug(f'No tasks finished in {timeout} seconds')
                return False

            logger.debug("Task finished: id={}, type={}, status={}".format(*base_result_info))
            self._all_completed.append(base_result_info[0])
            ids.remove(base_result_info[0])

        return True

    #
    # These cannot be full class members (with 'self') because then this class would need to
    # be pickleable (I think). But the Process objects are not.
    #

    @staticmethod
    def _run_flask(qcf_config: FractalConfig, log_queue, completed_queue):
        # This runs in a separate process, so we can modify the global logger
        # which will only affect that process
        qh = logging.handlers.QueueHandler(log_queue)
        logger = logging.getLogger()
        logger.handlers = [qh]
        logger.setLevel(qcf_config.loglevel)

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_flask. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        # Disable a few lines of flask output that always gets sent to stdout/stderr
        os.environ["WERKZEUG_RUN_MAIN"] = 'true'

        # Start the flask server
        try:
            flask_app = create_qcfractal_flask_app(qcf_config)

            if completed_queue is not None:
                from .flask_app import storage_socket as flask_storage_socket
                flask_storage_socket.set_completed_watch(completed_queue)

            flask_app.run(host=qcf_config.flask.bind, port=qcf_config.flask.port)
        except EndProcess as e:
            logger.debug("_run_flask received EndProcess: " + str(e))
            exit(0)
        except Exception as e:
            tb = ''.join(traceback.format_exception(None, e, e.__traceback__))
            logger.critical(f"Exception while running flask app:\n{tb}")
            exit(1)
