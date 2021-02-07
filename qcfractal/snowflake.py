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
from .config import FractalConfig, DatabaseConfig
from .periodics import FractalPeriodicsProcess
from .app.flask_app import FractalFlaskProcess
from .fractal_proc import FractalProcessBase, FractalProcessRunner


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

    def run(self) -> None:
        host = self._qcf_config.flask.bind
        port = self._qcf_config.flask.port

        # Try to connect 20 times (~2 seconds). If it fails after that, raise the last exception
        for i in range(21):
            try:
                client = FractalClient(f'http://{host}:{port}')
                break
            except Exception:
                if i == 10:
                    raise
                else:
                    time.sleep(0.2)

        worker_pool = ProcessPoolExecutor(self._max_workers)
        queue_manager = QueueManager(client, worker_pool, manager_name="snowflake_compute")
        queue_manager.start()

    def finalize(self) -> None:
        pass


class FractalSnowflake:
    def __init__(
        self,
        start: bool = True,
        max_workers: Optional[int] = 2,
        enable_watching: bool = True,
        storage_uri: Optional[str] = None,
        flask_config: str = "snowflake",
    ):
        """A temporary FractalServer that can be used to run complex workflows or try new computations.

        ! Warning ! All data is lost when the server is shutdown.
        """

        self._logger = logging.getLogger('fractal_snowflake')
        self._flask_config = flask_config

        # Create a temporary directory for everything
        self._tmpdir = tempfile.TemporaryDirectory()

        # db is in a subdir of that
        db_dir = os.path.join(self._tmpdir.name, 'db')

        if storage_uri is None:
            self._storage = TemporaryPostgres(data_dir=db_dir)
            self._storage_uri = self._storage.database_uri(safe=False)
            db_config = self._storage.config
        else:
            self._storage = None
            self._storage_uri = storage_uri
            db_config = DatabaseConfig(base_directory=self._tmpdir.name, uri=self._storage_uri)

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
        self._qcfractal_config = FractalConfig(**qcf_cfg)

        # Always use fork. This is generally a sane default until problems crop up
        # In particular, logging will be inherited from the parent process
        mp_ctx = multiprocessing.get_context('fork')

        ######################################
        # Now start the various subprocesses #
        ######################################
        flask = FractalFlaskProcess(self._qcfractal_config)
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

    def __del__(self):
        self.stop()

    def wait_for_results(self, ids, timeout=None):
        raise RuntimeError("TODO")
        self._flask_proc.wait_for_results(ids, timeout)

    def client(self):
        return FractalClient(address=self._fractal_uri)
