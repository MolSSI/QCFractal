from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcfractal.config import FractalConfig
from qcfractal.process_runner import ProcessBase, ProcessRunner
from qcfractalcompute.managers import QueueManager
from qcportal.managers import ManagerStatusEnum
from qcfractaltesting import load_procedure_data
from qcportal.records import PriorityEnum
import qcengine as qcng

# For testing only! We just make all available programs/procedures the same as all of them
qcng.list_available_programs = qcng.list_all_programs
qcng.list_available_procedures = qcng.list_all_procedures

if TYPE_CHECKING:
    from typing import Dict
    from qcportal.records import AllResultTypes
    from qcfractal.testing_helpers import TestingSnowflake, SQLAlchemySocket


class MockTestingExecutor:
    def __init__(self, result_data: Dict[int, AllResultTypes]):
        self._result_data = result_data


class ComputeProcess(ProcessBase):
    """
    Runs  a compute manager in a separate process
    """

    def __init__(self, qcf_config: FractalConfig, result_data: Dict[int, AllResultTypes]):
        self._qcf_config = qcf_config
        self._result_data = result_data

        # Don't initialize the worker pool here. It must be done in setup(), because
        # that is run in the separate process

    def setup(self) -> None:
        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}"

        self._worker = MockTestingExecutor(self._result_data)
        self._queue_manager = QueueManager(
            self._worker, fractal_uri=uri, manager_name="test_compute", update_frequency=2
        )

    def run(self) -> None:
        self._queue_manager.start()

    def interrupt(self) -> None:
        self._queue_manager.stop()


def test_manager_keepalive(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):

    snowflake.start_periodics()

    compute = ComputeProcess(snowflake._qcf_config, {})
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

    compute = ComputeProcess(snowflake._qcf_config, {})
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


def test_manager_claim_return(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    input_spec_0, molecule_0, result_data_0 = load_procedure_data("psi4_methane_opt_sometraj")

    meta, id_0 = storage_socket.records.optimization.add(input_spec_0, [molecule_0], "tag0", PriorityEnum.normal)

    result_data = {id_0[0]: result_data_0}

    compute = ComputeProcess(snowflake._qcf_config, result_data)
    compute_proc = ProcessRunner("test_compute", compute)

    time.sleep(2)  # wait for manager to register
    assert compute_proc.is_alive() is True

    meta, managers = storage_socket.managers.query()
    assert meta.n_found == 1
    manager_name = managers[0]["name"]

    time.sleep(3)  # update_frequency is 2, wait another second as well
