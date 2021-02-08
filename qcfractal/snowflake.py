from __future__ import annotations
import os
import tempfile
import time
import multiprocessing
import logging
import logging.handlers
from concurrent.futures import ProcessPoolExecutor
from typing import Optional
from .qc_queue import QueueManager

from .interface import FractalClient
from .postgres_harness import TemporaryPostgres
from .port_util import find_port
from .config import FractalConfig, DatabaseConfig, updated_nested_dict
from .periodics import FractalPeriodicsProcess
from .app.flask_app import FractalFlaskProcess
from .fractal_proc import FractalProcessBase, FractalProcessRunner

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Dict, Any


def attempt_client_connect(host: str, port: int) -> FractalClient:
    """
    Attempt to obtain a FractalClient for a host and port

    This will make several attempts in case the server hasn't been completely booted yet

    If a connection is successful, the FractalClient is returned. Otherwise an exception
    is raised representing the exception raised from the last attempt at FractalClient construction.

    Parameters
    ----------
    host: str
        Host IP/hostname to attempt to connect to
    port: int
        Port to attempt to connect to

    Returns
    -------
    FractalClient
        A client connected to the given host and port
    """

    # Try to connect 20 times (~2 seconds). If it fails after that, raise the last exception
    for i in range(21):
        try:
            return FractalClient(f'http://{host}:{port}')
        except Exception:
            if i == 20:
                # Out of attempts. Just give the last exception
                raise
            else:
                time.sleep(0.2)

    raise RuntimeError("PROGRAMMER ERROR - should never get here")


class SnowflakeComputeProcess(FractalProcessBase):
    """
    Runs  a compute manager in a separate process
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
            max_workers: int = 2
    ):
        self._qcf_config = qcf_config
        self._max_workers = max_workers

        # We cannot instantiate these here. The .run() function will be run in a separate process
        # and so instantiation must happen there
        self.worker_pool = None
        self.queue_manager = None

    def run(self) -> None:
        host = self._qcf_config.flask.bind
        port = self._qcf_config.flask.port
        client = attempt_client_connect(host, port)

        self.worker_pool = ProcessPoolExecutor(self._max_workers)
        self.queue_manager = QueueManager(client, self.worker_pool, manager_name="snowflake_compute")
        self.queue_manager.start()

    def finalize(self) -> None:
        if self.worker_pool is not None:
            self.worker_pool.shutdown()
        if self.queue_manager is not None:
            self.queue_manager.stop()


class FractalSnowflake:
    def __init__(
        self,
        start: bool = True,
        max_workers: Optional[int] = 2,
        enable_watching: bool = True,
        database_config: Optional[DatabaseConfig] = None,
        flask_config: str = "snowflake",
        extra_config: Optional[Dict[str, Any]] = None
    ):
        """A temporary FractalServer that can be used to run complex workflows or try new computations.

        ! Warning ! All data is lost when the server is shutdown.
        """

        self._logger = logging.getLogger('fractal_snowflake')

        # Create a temporary directory for everything
        self._tmpdir = tempfile.TemporaryDirectory()

        # db is in a subdir of that
        db_dir = os.path.join(self._tmpdir.name, 'db')

        if database_config is None:
            self._storage = TemporaryPostgres(data_dir=db_dir)
            self._storage.harness.create_database()
            self._storage_uri = self._storage.database_uri(safe=False)
            db_config = self._storage.config
        else:
            self._storage = None
            self._storage_uri = database_config.uri
            db_config = database_config

        fractal_host = "127.0.0.1"
        fractal_port = find_port()
        self._fractal_uri = f'http://{fractal_host}:{fractal_port}'

        # Create a configuration for QCFractal
        # Assign the log level for subprocesses. Use the same level as what is assigned for this object
        loglevel = self._logger.getEffectiveLevel()

        qcf_cfg = {}
        qcf_cfg["base_directory"] = self._tmpdir.name
        qcf_cfg["loglevel"] = logging.getLevelName(loglevel)
        qcf_cfg["database"] = db_config.dict()
        qcf_cfg["enable_views"] = False
        qcf_cfg["flask"] = {'config_name': flask_config, 'bind': fractal_host, 'port': fractal_port}
        qcf_cfg["enable_security"] = False

        # Add in any options passed to this Snowflake
        updated_nested_dict(qcf_cfg, extra_config)
        self._qcfractal_config = FractalConfig(**qcf_cfg)

        # Always use fork. This is generally a sane default until problems crop up
        # In particular, logging will be inherited from the parent process
        mp_ctx = multiprocessing.get_context('fork')

        # Do we want to enable watching/waiting for finished tasks?
        self._completed_queue = None
        if enable_watching:
            self._completed_queue = mp_ctx.Queue()
            self._all_completed = []

        ######################################
        # Now start the various subprocesses #
        ######################################
        flask = FractalFlaskProcess(self._qcfractal_config, self._completed_queue)
        compute = SnowflakeComputeProcess(self._qcfractal_config, max_workers)
        periodics = FractalPeriodicsProcess(self._qcfractal_config)

        self._flask_proc = FractalProcessRunner('snowflake_flask', mp_ctx, flask, start)
        self._periodics_proc = FractalProcessRunner('snowflake_periodics', mp_ctx, periodics, start)
        self._compute_proc = FractalProcessRunner('snowflake_compute', mp_ctx, compute, start)

    def stop(self):
        # Send all our processes SIGTERM
        self._compute_proc.stop()
        self._flask_proc.stop()
        self._periodics_proc.stop()

    def start(self):
        if not self._compute_proc.is_alive():
            self._compute_proc.start()
        if not self._flask_proc.is_alive():
            self._flask_proc.start()
        if not self._periodics_proc.is_alive():
            self._periodics_proc.start()


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
            except multiprocessing.queue.Empty as e:
                logger.debug(f'No tasks finished in {timeout} seconds')
                return False

            logger.debug("Task finished: id={}, type={}, status={}".format(*base_result_info))
            self._all_completed.append(base_result_info[0])
            ids.remove(base_result_info[0])

        return True

    def client(self) -> FractalClient:
        '''
        Obtain a FractalClient connected to this server
        '''

        # Try to connect 20 times (~2 seconds). If it fails after that, raise the last exception
        host = self._qcfractal_config.flask.bind
        port = self._qcfractal_config.flask.port

        return attempt_client_connect(host, port)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
