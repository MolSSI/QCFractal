from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import os
import secrets
import tempfile
import threading
import time
import weakref
from queue import Empty  # Just for exception handling
from typing import TYPE_CHECKING

import requests

from qcportal import PortalClient
from qcportal.record_models import RecordStatusEnum
from .config import FractalConfig, DatabaseConfig, update_nested_dict
from .flask_app.gunicorn_app import FractalGunicornApp
from .job_runner import FractalJobRunner
from .port_util import find_open_port
from .postgres_harness import create_snowflake_postgres

try:
    from qcfractalcompute.compute_manager import ComputeManager
    from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig
except ImportError:
    raise RuntimeError("qcfractalcompute is not installed. Snowflake is useless without it")

if TYPE_CHECKING:
    from typing import Dict, Any, Sequence, Optional, Set


def _api_process(
    qcf_config: FractalConfig,
    logging_queue: multiprocessing.Queue,
    finished_queue: multiprocessing.Queue,
    started_event: multiprocessing.Event,
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    api = FractalGunicornApp(qcf_config, finished_queue, started_event)

    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    try:
        api.run()
    finally:
        logging_queue.close()
        logging_queue.join_thread()


def _compute_process(compute_config: FractalComputeConfig, logging_queue: multiprocessing.Queue) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    compute = ComputeManager(compute_config)
    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        compute.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        compute.start()
    finally:
        logging_queue.close()
        logging_queue.join_thread()


def _job_runner_process(
    qcf_config: FractalConfig, logging_queue: multiprocessing.Queue, finished_queue: multiprocessing.Queue
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    job_runner = FractalJobRunner(qcf_config, finished_queue)

    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        job_runner.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        job_runner.start()
    finally:
        logging_queue.close()
        logging_queue.join_thread()


def _logging_thread(logging_queue, logging_thread_stop):
    while True:
        try:
            record = logging_queue.get(timeout=0.5)
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Empty:
            if logging_thread_stop.is_set():
                break
            else:
                continue


class FractalSnowflake:
    def __init__(
        self,
        start: bool = True,
        compute_workers: int = 2,
        database_config: Optional[DatabaseConfig] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        """A temporary, self-contained server

        A snowflake contains the server and compute manager, and can be used to test
        QCFractal/QCPortal, or to experiment.

        All data is lost when the server is shutdown.

        This can also be used as a context manager (`with FractalSnowflake(...) as s:`)
        """

        # Multiprocessing context - generally use fork
        self._mp_context = multiprocessing.get_context("fork")

        self._logger = logging.getLogger("fractal_snowflake")

        # Configure logging
        # We receive log entries from various processes via a queue
        # See https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes

        self._logging_queue = self._mp_context.Queue()
        self._logging_thread_stop = threading.Event()
        self._logging_thread = threading.Thread(
            target=_logging_thread, args=(self._logging_queue, self._logging_thread_stop), daemon=True
        )
        self._logging_thread.start()

        # Create a temporary directory for everything
        self._tmpdir = tempfile.TemporaryDirectory()

        # also parsl run directory and scratch directories
        parsl_run_dir = os.path.join(self._tmpdir.name, "parsl_run_dir")
        compute_scratch_dir = os.path.join(self._tmpdir.name, "compute_scratch_dir")
        os.makedirs(parsl_run_dir, exist_ok=True)
        os.makedirs(compute_scratch_dir, exist_ok=True)

        if database_config is None:
            # db and socket are subdirs of the base temporary directory
            db_dir = os.path.join(self._tmpdir.name, "db")
            self._pg_harness = create_snowflake_postgres(db_dir)
            self._pg_harness.create_database(True)
            db_config = self._pg_harness.config
        else:
            db_config = database_config

        api_host = "127.0.0.1"
        api_port = find_open_port()
        self._fractal_uri = f"http://{api_host}:{api_port}"

        # Create a configuration for QCFractal
        # Assign the log level for subprocesses. Use the same level as what is assigned for this object
        loglevel = self._logger.getEffectiveLevel()

        qcf_cfg: Dict[str, Any] = {}
        qcf_cfg["base_folder"] = self._tmpdir.name
        qcf_cfg["loglevel"] = logging.getLevelName(loglevel)
        qcf_cfg["database"] = db_config.dict()
        qcf_cfg["enable_security"] = False
        qcf_cfg["hide_internal_errors"] = False
        qcf_cfg["service_frequency"] = 10
        qcf_cfg["heartbeat_frequency"] = 5
        qcf_cfg["heartbeat_max_missed"] = 3
        qcf_cfg["api"] = {
            "host": api_host,
            "port": api_port,
            "secret_key": secrets.token_urlsafe(32),
            "jwt_secret_key": secrets.token_urlsafe(32),
        }

        # Add in any options passed to this Snowflake
        if extra_config is not None:
            update_nested_dict(qcf_cfg, extra_config)

        self._qcf_config = FractalConfig(**qcf_cfg)

        ######################################
        # Set up the various components      #
        ######################################

        # For Flask
        self._api_started = multiprocessing.Event()
        self._finished_queue = self._mp_context.Queue()
        self._all_completed: Set[int] = set()
        self._api_proc = None

        # For the compute manager
        uri = f"http://{self._qcf_config.api.host}:{self._qcf_config.api.port}"
        self._compute_config = FractalComputeConfig(
            base_folder=self._tmpdir.name,
            parsl_run_dir=parsl_run_dir,
            cluster="snowflake_compute",
            update_frequency=5,
            server=FractalServerSettings(
                fractal_uri=uri,
                verify=False,
            ),
            executors={
                "local": LocalExecutorConfig(
                    cores_per_worker=1,
                    memory_per_worker=1,
                    max_workers=compute_workers,
                    queue_tags=["*"],
                    scratch_directory=compute_scratch_dir,
                )
            },
        )
        self._compute_enabled = compute_workers > 0
        self._compute_proc = None

        # Job runner
        self._job_runner_proc = None

        # This is updated when starting components
        self._finalizer = None

        # Update now because of the logging thread
        self._update_finalizer()

        if start:
            self.start()

    def _update_finalizer(self):
        if self._finalizer is not None:
            self._finalizer.detach()

        self._finalizer = weakref.finalize(
            self,
            self._stop,
            self._compute_proc,
            self._api_proc,
            self._job_runner_proc,
            self._logging_queue,
            self._logging_thread,
            self._logging_thread_stop,
        )

    def _start_api(self):
        if self._api_proc is None:
            self._api_proc = self._mp_context.Process(
                target=_api_process,
                args=(self._qcf_config, self._logging_queue, self._finished_queue, self._api_started),
            )
            self._api_proc.start()

        self._update_finalizer()
        self.wait_for_api()

    def _stop_api(self):
        if self._api_proc is not None:
            self._api_proc.terminate()
            self._api_proc.join()
            self._api_proc = None
            self._api_started.clear()
            self._update_finalizer()

    def _start_compute(self):
        if not self._compute_enabled:
            return

        if self._compute_proc is None:
            self._compute_proc = self._mp_context.Process(
                target=_compute_process, args=(self._compute_config, self._logging_queue)
            )
            self._compute_proc.start()
            self._update_finalizer()

    def _stop_compute(self):
        if self._compute_proc is not None:
            self._compute_proc.terminate()
            self._compute_proc.join()
            self._compute_proc = None
            self._update_finalizer()

    def _start_job_runner(self):
        if self._job_runner_proc is None:
            self._job_runner_proc = self._mp_context.Process(
                target=_job_runner_process, args=(self._qcf_config, self._logging_queue, self._finished_queue)
            )
            self._job_runner_proc.start()
            self._update_finalizer()

    def _stop_job_runner(self):
        if self._job_runner_proc is not None:
            self._job_runner_proc.terminate()
            self._job_runner_proc.join()
            self._job_runner_proc = None
            self._update_finalizer()

    @classmethod
    def _stop(cls, compute_proc, api_proc, job_runner_proc, logging_queue, logging_thread, logging_thread_stop):
        ####################################################################################
        # This is written as a class method so that it can be called by a weakref finalizer
        ####################################################################################

        # Stop these in a particular order
        # First the compute, since it will communicate its demise to the api server
        # Flask must be last. It was started first and owns the db

        if compute_proc is not None:
            compute_proc.terminate()
            compute_proc.join()

        if job_runner_proc is not None:
            job_runner_proc.terminate()
            job_runner_proc.join()

        if api_proc is not None:
            api_proc.terminate()
            api_proc.join()

        logging_thread_stop.set()
        logging_thread.join()
        logging_queue.close()
        logging_queue.join_thread()

    def wait_for_api(self):
        """
        Wait for the flask/api server to come up and then exit

        If it does not come up after some time, an exception will be raised
        """

        running = self._api_started.wait(10.0)
        assert running

        # Seems there still may be a small time after the event is triggered and before
        # it can handle requests
        # Can't use ping that is part of the client - we haven't instantiated one yet
        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}/api/v1/ping"

        max_iter = 50
        iter = 0
        while True:
            try:
                r = requests.get(uri)
                if r.status_code != 200:
                    raise RuntimeError("Error pinging snowflake fractal server: ", r.text)
                break

            except requests.exceptions.ConnectionError:
                time.sleep(0.05)
                iter += 1
                if iter >= max_iter:
                    raise

    def start(self):
        """
        Starts all the components of the snowflake
        """

        self._start_api()
        self._start_compute()
        self._start_job_runner()

    def stop(self):
        """
        Stops all components of the snowflake
        """

        if self._compute_proc is not None:
            self._compute_proc.terminate()
            self._compute_proc.join()
            self._compute_proc = None

        if self._job_runner_proc is not None:
            self._job_runner_proc.terminate()
            self._job_runner_proc.join()
            self._job_runner_proc = None

        if self._api_proc is not None:
            self._api_proc.terminate()
            self._api_proc.join()
            self._api_proc = None

        self._update_finalizer()

    def get_uri(self) -> str:
        """
        Obtain the URI/address of the REST interface of this server

        Returns
        -------
        :
            Address/URI of the rest interface (ie, 'http://127.0.0.1:1234')
        """

        return f"http://{self._qcf_config.api.host}:{self._qcf_config.api.port}"

    def await_results(self, ids: Optional[Sequence[int]] = None, timeout: Optional[float] = None) -> bool:
        """
        Wait for computations to complete

        This function will block until the specified computations are complete (either success for failure).
        If timeout is given, that is the maximum amount of time to wait for a result. This timer is reset
        after each completed result.

        Parameters
        ----------
        ids
            Result/Procedure IDs to wait for. If not specified, all currently incomplete tasks
            will be waited for.

        timeout
            Maximum time to wait for a single result.


        Returns
        -------
        :
            True if all the results were received, False if timeout has elapsed without receiving a completed computation
        """
        logger = logging.getLogger(__name__)

        if ids is None:
            c = self.client()
            query_iter = c.query_records(status=[RecordStatusEnum.waiting, RecordStatusEnum.running])
            ids = [x.id for x in query_iter]

        # Remove any we have already marked as completed
        remaining_ids = set(ids) - self._all_completed

        if len(remaining_ids) == 0:
            logger.debug("All tasks are already finished")
            return True

        logger.debug("Waiting for ids: " + str(remaining_ids))

        while len(remaining_ids) > 0:
            # The queue stores a tuple of (id, status)
            try:
                finished_id, status = self._finished_queue.get(True, timeout)
            except Empty:
                logger.debug(f"Not all records finished in {timeout} seconds")
                return False

            logger.debug(f"Record finished: id={finished_id}, status={status}")

            # Add it to the list of all completed results we have seen
            self._all_completed.add(finished_id)

            # We may not be watching for this id, but if we are, remove it from the list
            # we are watching
            if finished_id in remaining_ids:
                remaining_ids.remove(finished_id)
                remaining_str = "None" if len(remaining_ids) == 0 else str(remaining_ids)
                logger.debug(f"Removed id={finished_id}. Remaining ids: {remaining_str}")

        return True

    def client(self) -> PortalClient:
        """
        Obtain a PortalClient connected to this server
        """

        # Shorten the timeout parameter - should be pretty quick in a snowflake
        c = PortalClient(self.get_uri())
        c._timeout = 2
        return c

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
