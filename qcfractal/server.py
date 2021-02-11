from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import FractalConfig

import os
import signal
import time
import multiprocessing
import logging
import logging.handlers
from io import StringIO
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from .qc_queue import QueueManager
from .interface import FractalClient
from .storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from .app import create_qcfractal_flask_app
from .config import FractalConfig
from .periodics import FractalPeriodics


class EndProcess(RuntimeError):
    pass


class StandaloneFractalServer:
    '''
    A FLASK server, periodics, and optional manager all in one!
    '''

    def __init__(self, fractal_config: FractalConfig, enable_watching: bool = False):
        self._config = fractal_config

        # "Spawn" is better than fork, since fork will copy this entire process
        # (and spawn is the default now in some python versions)
        self._mp_ctx = multiprocessing.get_context('spawn')

        # Store logs into a queue. This will be passed to the subprocesses, and they
        # will log to this
        self._log_queue = self._mp_ctx.Queue()

        # Set up logging. Subprocesses will send logs to the queue above, which are
        # then processed by a QueueListener
        self._log_stream = StringIO()
        log_str_handler = logging.StreamHandler(self._log_stream)
        formatter = logging.Formatter(fmt='[%(asctime)s] (%(processName)14s) %(levelname)8s: %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')
        log_str_handler.setFormatter(formatter)

        self._log_listener = logging.handlers.QueueListener(self._log_queue, log_str_handler, respect_handler_level=True)
        self._log_listener.start()

        ######################################
        # Now start the various subprocesses #
        ######################################
        self._completed_queue = None
        if enable_watching:
            self._completed_queue = self._mp_ctx.Queue()

        # Start up a flask instance
        self._flask_process = self._mp_ctx.Process(name="flask_proc", target=StandaloneFractalServer._run_flask_server, args=(self._config, self._log_queue, self._completed_queue))
        self._flask_process.start()

        # Start the periodics
        self._periodics_process = self._mp_ctx.Process(name="periodics_proc", target=StandaloneFractalServer._run_periodics, args=(self._config, self._log_queue))
        self._periodics_process.start()

        # Start the compute worker (which actually performs computations)
        time.sleep(2)
        self._compute_process = self._mp_ctx.Process(name="compute_proc", target=StandaloneFractalServer._run_compute_worker, args=(self._config, self._log_queue))
        self._compute_process.start()


    def stop(self):
        # Send all our processes SIGTERM
        self._compute_process.terminate()
        self._compute_process.join()

        self._flask_process.terminate()
        self._flask_process.join()

        self._periodics_process.terminate()
        self._periodics_process.join()

    def __del__(self):
        self.stop()

    def get_log(self):
        return self._log_stream.getvalue()


    #
    # These cannot be full class members (with 'self') because then this class would need to
    # be pickleable (I think). But the Process objects are not.
    #

    @staticmethod
    def _run_flask_server(config: FractalConfig, log_queue, completed_queue):
        # This runs in a separate process, so we can modify the global logger
        # which will only affect that process
        qh = logging.handlers.QueueHandler(log_queue)
        logger = logging.getLogger()
        logger.handlers = [qh]
        logger.setLevel("DEBUG")

        # Disable a few lines of flask output that always gets sent to stdout/stderr
        os.environ["WERKZEUG_RUN_MAIN"] = 'true'

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_flask_server. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        # Start the flask server
        flask_app = create_flask_app(config)

        if completed_queue is not None:
            from .app import storage_socket as flask_storage_socket
            flask_storage_socket.set_completed_watch(completed_queue)

        try:
            flask_app.run(host=config.bind_ip, port=config.port)
        except EndProcess as e:
            logger.debug("_run_flask_server received EndProcess: " + str(e))

    @staticmethod
    def _run_periodics(log_queue, storage_uri, qcfractal_config):
        # This runs in a separate process, so we can modify the global logger
        # which will only affect that process
        qh = logging.handlers.QueueHandler(log_queue)
        logger = logging.getLogger()
        logger.handlers = [qh]
        logger.setLevel("DEBUG")

        storage_socket = SQLAlchemySocket()
        storage_socket.init(storage_uri)

        periodics = FractalPeriodics(storage_socket, qcfractal_config)

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_periodics. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        try:
            periodics.start()
        except EndProcess as e:
            logger.debug("_run_periodics received EndProcess: " + str(e))

        # Periodics are now running in the background. But we need to keep this process alive
        try:
            while True:
                time.sleep(3600)
        except EndProcess as e:
            logger.debug("Caught EndProcess: " + str(e))
            periodics.stop()


    @staticmethod
    def _run_compute_worker(log_queue, flask_uri):
        # This runs in a separate process, so we can modify the global logger
        # which will only affect that process
        qh = logging.handlers.QueueHandler(log_queue)
        logger = logging.getLogger()
        logger.handlers = [qh]
        logger.setLevel("DEBUG")

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_compute_worker. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        client = FractalClient(flask_uri)
        worker_pool = ProcessPoolExecutor(2)
        queue_manager = QueueManager(client, worker_pool, manager_name="fractal_aio_manager")
        try:
            queue_manager.start()
        except EndProcess as e:
            logger.debug("_run_compute_worker received EndProcess: " + str(e))
            queue_manager.stop(str(e))


    def await_results(self, ids, timeout=None):
        logger = logging.getLogger(__name__)

        if self._completed_queue is None:
            raise RuntimeError("Cannot wait for results when the completed queue is not enabled. See the 'enable_watching' argument to the constructor")

        ids = set(int(x) for x in ids)

        while len(ids) > 0:
            # The queue stores a tuple of (id, type, status)
            base_result_info = self._completed_queue.get(True, timeout)
            logger.debug("Task finished: id={}, type={}, status={}".format(*base_result_info))
            ids.remove(base_result_info[0])
