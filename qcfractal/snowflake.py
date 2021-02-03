import os
import signal
import tempfile
import time
import multiprocessing
import logging
import logging.handlers
import traceback
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Union
from .qc_queue import QueueManager

from .interface import FractalClient
from .postgres_harness import TemporaryPostgres
from .port_util import find_port
from .config import FractalConfig, DatabaseConfig
from .periodics import FractalPeriodicsProcess
from .app.flask_app import FractalFlaskProcess

class EndProcess(RuntimeError):
    pass

class SnowflakeComputeProcess:
    """
    Runs  a compute manager in a separate process
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
            mp_context: multiprocessing.context.BaseContext,
            start: bool = True,
            max_workers: int = 2
    ):
        self._qcf_config = qcf_config
        self._mp_ctx = mp_context
        self._max_workers = max_workers

        self._compute_process = self._mp_ctx.Process(name="compute_proc", target=SnowflakeComputeProcess._run_compute_worker, args=(self._qcf_config, self._max_workers))
        if start:
            self.start()

    def start(self):
        if self._compute_process.is_alive():
            raise RuntimeError("Compute manager process is already running")
        else:
            self._compute_process.start()


    def stop(self):
        self._compute_process.terminate()
        self._compute_process.join()

    def __del__(self):
        self.stop()

    @staticmethod
    def _run_compute_worker(qcf_config: FractalConfig, max_workers):
        # This runs in a separate process, so we can modify the global logger
        # which will only affect that process
        logger = logging.getLogger()

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_compute_worker. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        host = qcf_config.flask.bind
        port = qcf_config.flask.port

        queue_manager = None
        try:
            # Try to connect 20 times (~2 seconds). If it fails after that, raise the last exception
            for i in range(21):
                try:
                    client = FractalClient(f'http://{host}:{port}')
                    break
                except Exception as e:
                    if i == 10:
                        raise
                    else:
                        time.sleep(0.2)

            worker_pool = ProcessPoolExecutor(max_workers)
            queue_manager = QueueManager(client, worker_pool, manager_name="snowflake_compute")
            queue_manager.start()
        except EndProcess as e:
            logger.debug("_run_compute_worker received EndProcess: " + str(e))
            if queue_manager is not None:
                queue_manager.stop(str(e))
        except Exception as e:
            tb = ''.join(traceback.format_exception(None, e, e.__traceback__))
            logger.critical(f"Exception while running compute worker:\n{tb}")
            exit(1)




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
        self._mp_ctx = multiprocessing.get_context('fork')

        ######################################
        # Now start the various subprocesses #
        ######################################
        self._flask_proc = FractalFlaskProcess(self._qcfractal_config, self._mp_ctx, start, enable_watching)

        # Try to connect 10 times. If it fails after that, raise an exception
        for i in range(11):
            try:
                FractalClient(address=self._fractal_uri)
            except Exception as e:
                if i == 10:
                    logger = logging.getLogger('snowflake_init')
                    logger.critical("The flask process has not started correctly. View the logs for more detail")
                else:
                    time.sleep(0.2)

        self._periodics_proc = FractalPeriodicsProcess(self._qcfractal_config, self._mp_ctx, start)
        self._compute_proc = SnowflakeComputeProcess(self._qcfractal_config, self._mp_ctx, start, max_workers)


    def stop(self):
        # Send all our processes SIGTERM
        self._compute_proc.stop()
        self._flask_proc.stop()
        self._periodics_proc.stop()

    def __del__(self):
        self.stop()

    def wait_for_results(self, ids, timeout=None):
        self._flask_proc.wait_for_results(ids, timeout)

    def client(self):
        return FractalClient(address=self._fractal_uri)
