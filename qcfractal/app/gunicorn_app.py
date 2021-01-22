from __future__ import annotations
import logging
import logging.handlers
import signal
import multiprocessing
import gunicorn.app.base
from gunicorn.glogging import Logger as GLogger
from .flask_app import create_qcfractal_flask_app

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..config import FractalConfig, Optional

# Signalling of process termination via an exception
class EndProcess(RuntimeError):
    pass

#####################################################
# See https://docs.gunicorn.org/en/stable/custom.html
#####################################################

class FractalGunicornApp(gunicorn.app.base.BaseApplication):

    def __init__(self,  qcfractal_config: FractalConfig):
        self.qcfractal_config = qcfractal_config
        self.application = create_qcfractal_flask_app(qcfractal_config)

        super().__init__()

    def load(self):
        return self.application

    def load_config(self):
        # This must be provided. It is called from the superclass, and we need to
        # populate self.cfg
        bind = self.qcfractal_config.flask.bind
        port = self.qcfractal_config.flask.port

        config = {'bind': f'{bind}:{port}',
                  'workers': self.qcfractal_config.flask.num_workers,
                  'loglevel': self.qcfractal_config.loglevel}

        for key, value in config.items():
            self.cfg.set(key.lower(), value)


class FractalGunicornProcess:
    """
    Creates a gunicorn app in a separate process, and allows for control
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
            mp_context: multiprocessing.context.SpawnContext,
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

        self._gunicorn_process = self._mp_ctx.Process(name="gunicorn_proc", target=FractalGunicornProcess._run_gunicorn, args=(self._qcf_config, self._log_queue, self._completed_queue))
        if start:
            self.start()

    def start(self):
        if self._gunicorn_process.is_alive():
            raise RuntimeError("Gunicorn process is already running")
        else:
            self._gunicorn_process.start()


    def stop(self):
        self._gunicorn_process.terminate()
        self._gunicorn_process.join()

    def __del__(self):
        self.stop()

    def wait_for_results(self, ids, timeout=None):
        logger = logging.getLogger(__name__)

        if self._completed_queue is None:
            raise RuntimeError("Cannot wait for results when the completed queue is not enabled. See the 'enable_watching' argument to the constructor")

        ids = set(int(x) for x in ids)

        while len(ids) > 0:
            # The queue stores a tuple of (id, type, status)
            base_result_info = self._completed_queue.get(True, timeout)
            logger.debug("Task finished: id={}, type={}, status={}".format(*base_result_info))
            self._all_completed.append(base_result_info[0])
            ids.remove(base_result_info[0])

    #
    # These cannot be full class members (with 'self') because then this class would need to
    # be pickleable (I think). But the Process objects are not.
    #

    @staticmethod
    def _run_gunicorn(qcf_config: FractalConfig, log_queue, completed_queue):
        # This runs in a separate process, so we can modify the global logger
        # which will only affect that process
        qh = logging.handlers.QueueHandler(log_queue)
        logger = logging.getLogger()
        logger.handlers = [qh]
        logger.setLevel(qcf_config.loglevel)

        # Disable a few lines of flask output that always gets sent to stdout/stderr
        #os.environ["WERKZEUG_RUN_MAIN"] = 'true'

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_gunicorn. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        # Start the gunicorn/flask server
        try:
            gunicorn_app = FractalGunicornApp(qcf_config)
        except Exception as e:
            logger.critical(f"Exception while starting gunicorn app: str{e}")
            exit(1)

        if completed_queue is not None:
            from .flask_app import storage_socket as flask_storage_socket
            flask_storage_socket.set_completed_watch(completed_queue)

        try:

            gunicorn_app.run()
        except EndProcess as e:
            logger.debug("_run_gunicorn received EndProcess: " + str(e))
            exit(0)
        except Exception as e:
            logger.critical(f"Exception while running gunicorn app: str{e}")
            exit(1)
