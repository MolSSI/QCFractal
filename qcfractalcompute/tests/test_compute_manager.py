from __future__ import annotations

import time
from typing import TYPE_CHECKING

import qcengine as qcng

from qcfractal.config import FractalConfig
from qcfractal.process_runner import ProcessBase, ProcessRunner
from qcfractalcompute.managers import ComputeManager
from qcfractaltesting import load_procedure_data
from qcportal.managers import ManagerStatusEnum
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

    meta, managers = storage_socket.managers.query()
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

    meta, managers = storage_socket.managers.query()
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

    meta, managers = storage_socket.managers.query()
    assert meta.n_found == 1
    assert managers[0]["tags"] == ["test_tag_1", "test_tag_2", "*"]


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
    # explicitly load enough so we have to do chunking on the return
    input_spec_0, molecule_0, result_data_0 = load_procedure_data("psi4_methane_opt_sometraj")
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_water_gradient")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_water_hessian")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("rdkit_water_energy")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("psi4_benzene_energy_2")
    input_spec_6, molecule_6, result_data_6 = load_procedure_data("psi4_water_energy")

    meta, id_0 = storage_socket.records.optimization.add([molecule_0], input_spec_0, "tag0", PriorityEnum.normal)
    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.high)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.high)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "tag3", PriorityEnum.high)
    meta, id_4 = storage_socket.records.singlepoint.add([molecule_4], input_spec_4, "tag4", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag5", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag6", PriorityEnum.normal)
    all_id = id_0 + id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    result_data = {
        id_0[0]: result_data_0,
        id_1[0]: result_data_1,
        id_2[0]: result_data_2,
        id_3[0]: result_data_3,
        id_4[0]: result_data_4,
        id_5[0]: result_data_5,
        id_6[0]: result_data_6,
    }

    compute = ComputeProcess(snowflake._qcf_config, result_data)
    compute_proc = ProcessRunner("test_compute", compute)

    time.sleep(2)  # wait for manager to register
    assert compute_proc.is_alive() is True

    meta, managers = storage_socket.managers.query()
    assert meta.n_found == 1

    snowflake.await_results(all_id, 10.0)


def test_manager_deferred_return(snowflake: TestingSnowflake, storage_socket: SQLAlchemySocket):
    # explicitly load enough so we have to do chunking on the return
    input_spec_0, molecule_0, result_data_0 = load_procedure_data("psi4_methane_opt_sometraj")
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_water_gradient")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_water_hessian")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("rdkit_water_energy")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("psi4_benzene_energy_2")
    input_spec_6, molecule_6, result_data_6 = load_procedure_data("psi4_water_energy")

    meta, id_0 = storage_socket.records.optimization.add([molecule_0], input_spec_0, "tag0", PriorityEnum.normal)
    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.high)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.high)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "tag3", PriorityEnum.high)
    meta, id_4 = storage_socket.records.singlepoint.add([molecule_4], input_spec_4, "tag4", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag5", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag6", PriorityEnum.normal)
    all_id = id_0 + id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    result_data = {
        id_0[0]: result_data_0,
        id_1[0]: result_data_1,
        id_2[0]: result_data_2,
        id_3[0]: result_data_3,
        id_4[0]: result_data_4,
        id_5[0]: result_data_5,
        id_6[0]: result_data_6,
    }

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
    meta, managers = storage_socket.managers.query()
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
    # explicitly load enough so we have to do chunking on the return
    input_spec_0, molecule_0, result_data_0 = load_procedure_data("psi4_methane_opt_sometraj")
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_water_gradient")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_water_hessian")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("rdkit_water_energy")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("psi4_benzene_energy_2")
    input_spec_6, molecule_6, result_data_6 = load_procedure_data("psi4_water_energy")

    meta, id_0 = storage_socket.records.optimization.add([molecule_0], input_spec_0, "tag0", PriorityEnum.normal)
    meta, id_1 = storage_socket.records.singlepoint.add([molecule_1], input_spec_1, "tag1", PriorityEnum.high)
    meta, id_2 = storage_socket.records.singlepoint.add([molecule_2], input_spec_2, "tag2", PriorityEnum.high)
    meta, id_3 = storage_socket.records.singlepoint.add([molecule_3], input_spec_3, "tag3", PriorityEnum.high)
    meta, id_4 = storage_socket.records.singlepoint.add([molecule_4], input_spec_4, "tag4", PriorityEnum.normal)
    meta, id_5 = storage_socket.records.singlepoint.add([molecule_5], input_spec_5, "tag5", PriorityEnum.normal)
    meta, id_6 = storage_socket.records.singlepoint.add([molecule_6], input_spec_6, "tag6", PriorityEnum.normal)
    all_id = id_0 + id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    result_data = {
        id_0[0]: result_data_0,
        id_1[0]: result_data_1,
        id_2[0]: result_data_2,
        id_3[0]: result_data_3,
        id_4[0]: result_data_4,
        id_5[0]: result_data_5,
        id_6[0]: result_data_6,
    }

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
    meta, managers = storage_socket.managers.query()
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