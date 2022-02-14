from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import os
import tempfile
import time
import weakref
from concurrent.futures import ProcessPoolExecutor
from queue import Empty  # Just for exception handling
from typing import TYPE_CHECKING

import requests

from qcfractalcompute import ComputeManager
from qcportal import PortalClient
from qcportal.records import RecordStatusEnum
from .app.flask_app import FlaskProcess
from .config import FractalConfig, DatabaseConfig, update_nested_dict
from .periodics import PeriodicsProcess
from .port_util import find_open_port
from .postgres_harness import TemporaryPostgres
from .process_runner import ProcessBase, ProcessRunner

if TYPE_CHECKING:
    from typing import Dict, Any, Sequence, Optional, Set


class SnowflakeComputeProcess(ProcessBase):
    """
    Runs  a compute manager in a separate process
    """

    def __init__(self, qcf_config: FractalConfig, compute_workers: int = 2):
        self._qcf_config = qcf_config
        self._compute_workers = compute_workers

        # Don't initialize the worker pool here. It must be done in setup(), because
        # that is run in the separate process

    def setup(self) -> None:
        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}"

        self._worker_pool = ProcessPoolExecutor(self._compute_workers)
        self._queue_manager = ComputeManager(self._worker_pool, fractal_uri=uri, manager_name="snowflake_compute")

    def run(self) -> None:
        self._queue_manager.start()

    def interrupt(self) -> None:
        self._queue_manager.stop()
        self._worker_pool.shutdown()


class FractalSnowflake:
    def __init__(
        self,
        start: bool = True,
        compute_workers: int = 2,
        enable_watching: bool = True,
        database_config: Optional[DatabaseConfig] = None,
        flask_config: str = "snowflake",
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        """A temporary FractalServer that can be used to run complex workflows or try new computations.

        ! Warning ! All data is lost when the server is shutdown.
        """

        self._logger = logging.getLogger("fractal_snowflake")

        # Create a temporary directory for everything
        self._tmpdir = tempfile.TemporaryDirectory()

        # db is in a subdir of that
        db_dir = os.path.join(self._tmpdir.name, "db")

        if database_config is None:
            # Make this part of the class so it is kept alive
            self._tmp_pgdb = TemporaryPostgres(data_dir=db_dir)
            self._tmp_pgdb._harness.create_database()
            self._storage_uri = self._tmp_pgdb.database_uri(safe=False)
            db_config = self._tmp_pgdb._config
        else:
            self._storage_uri = database_config.uri
            db_config = database_config

        fractal_host = "127.0.0.1"
        fractal_port = find_open_port()
        self._fractal_uri = f"http://{fractal_host}:{fractal_port}"

        # Create a configuration for QCFractal
        # Assign the log level for subprocesses. Use the same level as what is assigned for this object
        loglevel = self._logger.getEffectiveLevel()

        qcf_cfg: Dict[str, Any] = {}
        qcf_cfg["base_folder"] = self._tmpdir.name
        qcf_cfg["loglevel"] = logging.getLevelName(loglevel)
        qcf_cfg["database"] = db_config.dict()
        qcf_cfg["api"] = {"config_name": flask_config, "host": fractal_host, "port": fractal_port}
        qcf_cfg["enable_security"] = False
        qcf_cfg["hide_internal_errors"] = False
        qcf_cfg["service_frequency"] = 10

        # Add in any options passed to this Snowflake
        if extra_config is None:
            extra_config = {}

        update_nested_dict(qcf_cfg, extra_config)
        self._qcf_config = FractalConfig(**qcf_cfg)
        self._compute_workers = compute_workers

        # Do we want to enable watching/waiting for finished tasks?
        self._completed_queue = None
        if enable_watching:
            # We use fork inside ProcessRunner, so we must use for here to set up the Queues
            # This must be changed if the ProcessRunner is ever changed to use seomthing else
            mp_ctx = multiprocessing.get_context("fork")
            self._completed_queue = mp_ctx.Queue()
            self._all_completed: Set[int] = set()

        ######################################
        # Now start the various subprocesses #
        ######################################
        # For notification that flask is now ready to accept connections
        self._flask_started = multiprocessing.Event()
        flask = FlaskProcess(self._qcf_config, self._completed_queue, self._flask_started)

        periodics = PeriodicsProcess(self._qcf_config, self._completed_queue)

        # Don't auto start here. we will handle it later
        self._flask_proc = ProcessRunner("snowflake_flask", flask, False)

        self._periodics_proc = ProcessRunner("snowflake_periodics", periodics, False)

        compute = SnowflakeComputeProcess(self._qcf_config, self._compute_workers)
        self._compute_proc = ProcessRunner("snowflake_compute", compute, False)

        if start:
            self.start()

        self._finalizer = weakref.finalize(self, self._stop, self._compute_proc, self._flask_proc, self._periodics_proc)

    @classmethod
    def _stop(cls, compute_proc, flask_proc, periodics_proc):
        ####################################################################################
        # This is written as a class method so that it can be called by a weakref finalizer
        ####################################################################################

        # Stop these in a particular order
        # First the compute, since it will communicate its demise to the api server
        # Flask must be last. It was started first and owns the db
        compute_proc.stop()
        periodics_proc.stop()
        flask_proc.stop()

    def wait_for_flask(self):
        running = self._flask_started.wait(10.0)
        assert running

        # Seems there still may be a small time after the event is triggered and before
        # it can handle requests
        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}/ping"

        max_iter = 50
        iter = 0
        while True:
            try:
                requests.get(uri)
                break
            except requests.exceptions.ConnectionError:
                time.sleep(0.05)
                iter += 1
                if iter >= max_iter:
                    raise

    def stop(self):
        self._stop(self._compute_proc, self._flask_proc, self._periodics_proc)
        self._flask_started.clear()

    def start(self):
        if not self._flask_proc.is_alive():
            self._flask_proc.start()

        self.wait_for_flask()

        if not self._periodics_proc.is_alive():
            self._periodics_proc.start()
        if self._compute_workers > 0 and not self._compute_proc.is_alive():
            self._compute_proc.start()

    def get_uri(self) -> str:
        """
        Obtain the URI/address of the REST interface of this server

        Returns
        -------
        str
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
        ids: Optional[Sequence[int]]
            Result/Procedure IDs to wait for. If not specified, all currently incomplete tasks
            will be waited for.

        timeout: float
            Maximum time to wait for a single result.


        Returns
        -------
        bool
            True if all the results were received, False if timeout has elapsed without receiving a completed computation
        """
        logger = logging.getLogger(__name__)

        if self._completed_queue is None:
            raise RuntimeError(
                "Cannot wait for results when the completed queue is not enabled. See the 'enable_watching' argument to the constructor"
            )

        if ids is None:
            c = self.client()
            _, proc = c.query_records(status=[RecordStatusEnum.waiting, RecordStatusEnum.running])
            ids = [x.id for x in proc]

        # Remove any we have already marked as completed
        remaining_ids = set(ids) - self._all_completed

        if len(remaining_ids) == 0:
            logger.debug("All tasks are already finished")
            return True

        logger.debug("Waiting for ids: " + str(remaining_ids))

        while len(remaining_ids) > 0:
            # The queue stores a tuple of (id, type, status)
            try:
                base_result_info = self._completed_queue.get(True, timeout)
            except Empty:
                logger.warning(f"No tasks finished in {timeout} seconds")
                return False

            logger.debug("Task finished: id={}, status={}".format(*base_result_info))
            finished_id = base_result_info[0]

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

        return PortalClient(self.get_uri())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
