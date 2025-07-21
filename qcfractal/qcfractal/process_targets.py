from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import queue
import threading
import weakref
from typing import Optional

from werkzeug.serving import make_server

from qcfractal.config import FractalConfig
from qcfractal.flask_app import create_flask_app
from qcfractal.flask_app.waitress_app import FractalWaitressApp
from qcfractal.job_runner import FractalJobRunner


def api_process(
    qcf_config: FractalConfig,
    logging_queue: multiprocessing.Queue,
    finished_queue: Optional[multiprocessing.Queue],
    initialized_event: Optional[multiprocessing.Event] = None,
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)
    logger.setLevel(qcf_config.loglevel)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    api = FractalWaitressApp(qcf_config, finished_queue=finished_queue)

    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if initialized_event is not None:
        initialized_event.set()

    try:
        api.run()
    except KeyboardInterrupt:  # Swallow ugly output on CTRL-C
        pass
    finally:
        logging_queue.close()
        logging_queue.join_thread()


class QCFAPIThread:
    """
    A class that runs the QCFractal API in a separate thread

    This does not use waitress, but instead runs the Flask app directly. This is useful for debugging and testing
    (such as with the snowflake) but shouldn't be used in user-facing production.
    """

    def __init__(
        self,
        qcf_config: FractalConfig,
        finished_queue: Optional[queue.Queue] = None,
    ):
        self._qcf_config = qcf_config
        self._flask_app = create_flask_app(qcf_config, finished_queue=finished_queue)
        self._server = make_server(self._qcf_config.api.host, self._qcf_config.api.port, self._flask_app)
        self._api_thread = None
        self._finalizer = None

    # Classmethod because finalizer can't handle bound methods
    @classmethod
    def _stop(cls, server, api_thread):
        if server is not None:
            server.shutdown()
            api_thread.join()

    def start(self, initialized_event: Optional[threading.Event] = None) -> None:
        if self._api_thread is not None:
            raise RuntimeError("API already started")

        # We use daemon=True
        # This means that the main python process can exit, calling various destructors
        # and finalizers (rather than waiting for those threads to finish before doing so)
        self._api_thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._api_thread.start()

        if initialized_event is not None:
            initialized_event.set()

        self._finalizer = weakref.finalize(
            self,
            self._stop,
            self._server,
            self._api_thread,
        )

    def stop(self) -> None:
        if self._finalizer is not None:
            self._finalizer()

        self._api_thread = None

    def is_alive(self) -> bool:
        if self._api_thread is None:
            return False
        return self._api_thread.is_alive()


def job_runner_process(
    qcf_config: FractalConfig,
    logging_queue: multiprocessing.Queue,
    finished_queue: Optional[multiprocessing.Queue],
    initialized_event: Optional[multiprocessing.Event] = None,
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)
    logger.setLevel(qcf_config.loglevel)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    job_runner = FractalJobRunner(qcf_config, finished_queue=finished_queue)

    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        job_runner.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if initialized_event is not None:
        initialized_event.set()

    try:
        job_runner.start()
    except KeyboardInterrupt:  # Swallow ugly output on CTRL-C
        pass
    finally:
        logging_queue.close()
        logging_queue.join_thread()


class QCFJobRunnerThread:
    def __init__(
        self,
        qcf_config: FractalConfig,
        finished_queue: Optional[queue.Queue] = None,
    ):
        self._qcf_config = qcf_config
        self._job_runner: Optional[FractalJobRunner] = None
        self._job_runner_thread = None
        self._finalizer = None
        self._finished_queue = finished_queue

    # Classmethod because finalizer can't handle bound methods
    @classmethod
    def _stop(cls, job_runner, job_runner_thread):
        if job_runner is not None:
            job_runner.stop()
            job_runner_thread.join()

    def start(self, initialized_event: Optional[threading.Event] = None) -> None:
        if self._job_runner is not None:
            raise RuntimeError("Job Runner already started")

        self._job_runner = FractalJobRunner(self._qcf_config, self._finished_queue)

        # We use daemon=True
        # This means that the main python process can exit, calling various destructors
        # and finalizers (rather than waiting for those threads to finish before doing so)
        self._job_runner_thread = threading.Thread(target=self._job_runner.start, daemon=True)
        self._job_runner_thread.start()

        if initialized_event is not None:
            initialized_event.set()

        self._finalizer = weakref.finalize(
            self,
            self._stop,
            self._job_runner,
            self._job_runner_thread,
        )

    def stop(self) -> None:
        if self._finalizer is not None:
            self._finalizer()

        self._job_runner = None
        self._job_runner_thread = None

    def is_alive(self) -> bool:
        if self._job_runner_thread is None:
            return False
        return self._job_runner_thread.is_alive()
