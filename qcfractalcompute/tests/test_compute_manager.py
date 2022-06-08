from __future__ import annotations

import time
from typing import TYPE_CHECKING

import qcengine as qcng

from qcfractal.components.records.optimization.testing_helpers import submit_test_data as submit_opt_test_data
from qcfractal.components.records.singlepoint.testing_helpers import submit_test_data as submit_sp_test_data
from qcfractal.config import FractalConfig
from qcfractal.process_runner import ProcessBase, ProcessRunner
from qcfractalcompute.managers import ComputeManager
from qcportal.managers import ManagerStatusEnum, ManagerQueryFilters
from qcportal.records import PriorityEnum, RecordStatusEnum

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
    Runs a compute manager in a separate process
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
        self._queue_manager = ComputeManager(
            self._worker,
            fractal_uri=uri,
            manager_name="test_compute",
            update_frequency=2,
            max_tasks=self._qcf_config.api_limits.manager_tasks_claim,
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

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
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


def test_manager_tag_none(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):

    worker = MockTestingExecutor(result_data={})
    manager = ComputeManager(
        worker,
        fractal_uri=snowflake.get_uri(),
        manager_name="test_compute",
        update_frequency=2,
        max_tasks=snowflake._qcf_config.api_limits.manager_tasks_claim,
        queue_tag=None,
    )

    time.sleep(2)  # wait for manager to register

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    assert managers[0]["tags"] == ["*"]


def test_manager_tag_single(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):

    worker = MockTestingExecutor(result_data={})
    manager = ComputeManager(
        worker,
        fractal_uri=snowflake.get_uri(),
        manager_name="test_compute",
        update_frequency=2,
        max_tasks=snowflake._qcf_config.api_limits.manager_tasks_claim,
        queue_tag="test_tag",
    )

    time.sleep(2)  # wait for manager to register

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    assert managers[0]["tags"] == ["test_tag"]


def test_manager_tag_multi(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):

    worker = MockTestingExecutor(result_data={})
    manager = ComputeManager(
        worker,
        fractal_uri=snowflake.get_uri(),
        manager_name="test_compute",
        update_frequency=2,
        max_tasks=snowflake._qcf_config.api_limits.manager_tasks_claim,
        queue_tag=["test_tag_1", "test_tag_2", "*"],
    )

    time.sleep(2)  # wait for manager to register

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    assert managers[0]["tags"] == ["test_tag_1", "test_tag_2", "*"]


def test_manager_claim_inactive(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    snowflake.start_periodics()

    compute = ComputeProcess(snowflake._qcf_config, {})
    compute_proc = ProcessRunner("test_compute", compute)

    time.sleep(2)  # wait for manager to register
    assert compute_proc.is_alive() is True

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    manager_name = managers[0]["name"]

    storage_socket.managers.deactivate([manager_name])

    # Next update should kill the process
    time.sleep(2 + 2)  # update_frequency is 2, wait another two seconds as well

    # Should have killed the manager process
    assert compute_proc.is_alive() is False


def populate_db(storage_socket: SQLAlchemySocket):
    # explicitly load enough so we have to do chunking on the return
    id_0, result_data_0 = submit_opt_test_data(storage_socket, "psi4_methane_opt_sometraj", "tag0", PriorityEnum.normal)
    id_1, result_data_1 = submit_sp_test_data(storage_socket, "psi4_water_gradient", "tag1", PriorityEnum.high)
    id_2, result_data_2 = submit_sp_test_data(storage_socket, "psi4_water_hessian", "tag2", PriorityEnum.high)
    id_3, result_data_3 = submit_sp_test_data(storage_socket, "psi4_peroxide_energy_wfn", "tag3", PriorityEnum.high)
    id_4, result_data_4 = submit_sp_test_data(storage_socket, "rdkit_water_energy", "tag4", PriorityEnum.normal)
    id_5, result_data_5 = submit_sp_test_data(storage_socket, "psi4_benzene_energy_2", "tag5", PriorityEnum.normal)
    id_6, result_data_6 = submit_sp_test_data(storage_socket, "psi4_water_energy", "tag6", PriorityEnum.normal)
    all_id = [id_0, id_1, id_2, id_3, id_4, id_5, id_6]

    result_data = {
        id_0: result_data_0,
        id_1: result_data_1,
        id_2: result_data_2,
        id_3: result_data_3,
        id_4: result_data_4,
        id_5: result_data_5,
        id_6: result_data_6,
    }

    return all_id, result_data


def test_manager_claim_return(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    all_id, result_data = populate_db(storage_socket)

    compute = ComputeProcess(snowflake._qcf_config, result_data)
    compute_proc = ProcessRunner("test_compute", compute)

    time.sleep(2)  # wait for manager to register
    assert compute_proc.is_alive() is True

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1

    r = snowflake.await_results(all_id, 10.0)
    assert r is True


def test_manager_deferred_return(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    all_id, result_data = populate_db(storage_socket)

    # Don't use a compute process so we can update, etc, manually
    worker = MockTestingExecutor(result_data)
    manager = ComputeManager(
        worker,
        fractal_uri=snowflake.get_uri(),
        manager_name="test_compute",
        update_frequency=2,
        max_tasks=snowflake._qcf_config.api_limits.manager_tasks_claim,
    )

    time.sleep(2)  # wait for manager to register
    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1

    manager.update(new_tasks=True)
    ntasks = manager.active
    assert ntasks == snowflake._qcf_config.api_limits.manager_tasks_claim
    assert manager.n_deferred_tasks == 0

    # Sever goes down
    snowflake.stop_flask()

    # Manager tries to update again
    manager.update(new_tasks=True)
    assert manager.active == 0
    assert manager.n_deferred_tasks == ntasks

    # Now server comes back
    snowflake.start_flask()
    manager.update(new_tasks=True)

    # Now we have more tasks, and no more deferred
    assert manager.active > 0
    assert manager.n_deferred_tasks == 0

    # Finish the rest
    while manager.active > 0:
        manager.update(new_tasks=True)

    recs = storage_socket.records.get(all_id)
    assert all(x["status"] != RecordStatusEnum.waiting for x in recs)
    assert all(x["manager_name"] == manager.name for x in recs)


def test_manager_deferred_drop(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    all_id, result_data = populate_db(storage_socket)

    # Don't use a compute process so we can update, etc, manually
    worker = MockTestingExecutor(result_data)
    manager = ComputeManager(
        worker,
        fractal_uri=snowflake.get_uri(),
        manager_name="test_compute",
        update_frequency=2,
        max_tasks=snowflake._qcf_config.api_limits.manager_tasks_claim,
        server_error_retries=3,
    )

    time.sleep(2)  # wait for manager to register
    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1

    manager.update(new_tasks=True)
    ntasks = manager.active
    assert ntasks == snowflake._qcf_config.api_limits.manager_tasks_claim
    assert manager.n_deferred_tasks == 0

    # Sever goes down
    snowflake.stop_flask()

    # Manager tries to update several times
    for i in range(3 + 1):  # 3 = "server_error_retries" argument given to QueueManager()
        manager.update(new_tasks=True)

    recs = storage_socket.records.get(all_id)
    assert all(
        x["status"] == RecordStatusEnum.running if x["manager_name"] == manager.name else RecordStatusEnum.waiting
        for x in recs
    )

    assert manager.n_deferred_tasks == 0
    assert manager.active == 0
