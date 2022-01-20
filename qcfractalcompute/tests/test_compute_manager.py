from __future__ import annotations

import time
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

from qcfractal.config import FractalConfig
from qcfractal.process_runner import ProcessBase, ProcessRunner
from qcfractalcompute.managers import QueueManager
from qcportal.managers import ManagerStatusEnum

if TYPE_CHECKING:
    from qcfractal.testing_helpers import TestingSnowflake, SQLAlchemySocket


class ComputeProcess(ProcessBase):
    """
    Runs  a compute manager in a separate process
    """

    def __init__(self, qcf_config: FractalConfig):
        self._qcf_config = qcf_config

        # Don't initialize the worker pool here. It must be done in setup(), because
        # that is run in the separate process

    def setup(self) -> None:
        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}"

        self._worker_pool = ProcessPoolExecutor(1)
        self._queue_manager = QueueManager(
            self._worker_pool, fractal_uri=uri, manager_name="test_compute", update_frequency=2
        )

    def run(self) -> None:
        self._queue_manager.start()

    def interrupt(self) -> None:
        self._queue_manager.stop()
        self._worker_pool.shutdown()


def test_manager_keepalive(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):

    snowflake.start_periodics()

    compute = ComputeProcess(snowflake._qcf_config)
    compute_proc = ProcessRunner("test_compute", compute)

    time.sleep(2)  # wait for manager to register

    meta, managers = storage_socket.managers.query()
    assert meta.n_found == 1
    manager_name = managers[0]["name"]

    sleep_time = snowflake._qcf_config.heartbeat_frequency
    max_missed = snowflake._qcf_config.heartbeat_max_missed

    for i in range(max_missed * 2):
        time.sleep(sleep_time)
        m = storage_socket.managers.get([manager_name])
        assert m[0]["status"] == ManagerStatusEnum.active

    # miss too many heartbeats
    compute_proc.stop()

    time.sleep(sleep_time * (max_missed + 1))
    m = storage_socket.managers.get([manager_name])
    assert m[0]["status"] == ManagerStatusEnum.inactive


def test_manager_claim_inactive(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    snowflake.start_periodics()

    compute = ComputeProcess(snowflake._qcf_config)
    compute_proc = ProcessRunner("test_compute", compute)

    time.sleep(2)  # wait for manager to register
    assert compute_proc.is_alive() is True

    meta, managers = storage_socket.managers.query()
    assert meta.n_found == 1
    manager_name = managers[0]["name"]

    storage_socket.managers.deactivate([manager_name])

    # Next update should kill the process
    time.sleep(2 + 2)  # update_frequency is 2, wait another two seconds as well

    # Should have killed the manager process
    assert compute_proc.is_alive() is False
