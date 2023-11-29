from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from qcfractalcompute.compute_manager import ComputeManager
from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig
from qcfractalcompute.testing_helpers import QCATestingComputeThread, populate_db
from qcportal.managers import ManagerStatusEnum, ManagerQueryFilters
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_manager_keepalive(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()

    snowflake.start_job_runner()

    compute = QCATestingComputeThread(snowflake._qcf_config, {})
    compute.start(manual_updates=True)

    time.sleep(1)  # wait for manager to register

    managers = storage_socket.managers.query(ManagerQueryFilters())
    assert len(managers) == 1
    manager_name = managers[0]["name"]

    sleep_time = snowflake._qcf_config.heartbeat_frequency
    max_missed = snowflake._qcf_config.heartbeat_max_missed

    for i in range(max_missed * 2):
        time_0 = now_at_utc()
        compute._compute.heartbeat()
        time_1 = now_at_utc()
        time.sleep(sleep_time)
        m = storage_socket.managers.get([manager_name])
        assert m[0]["status"] == ManagerStatusEnum.active
        assert time_0 < m[0]["modified_on"] < time_1

    # No more updates, server should eventually mark as inactive
    time.sleep(sleep_time * (max_missed + 1))
    m = storage_socket.managers.get([manager_name])
    assert m[0]["status"] == ManagerStatusEnum.inactive


def test_manager_tags(snowflake: QCATestingSnowflake, tmp_path):
    storage_socket = snowflake.get_storage_socket()

    compute_config = FractalComputeConfig(
        base_folder=str(tmp_path),
        cluster="testing_compute",
        update_frequency=5,
        server=FractalServerSettings(
            fractal_uri=snowflake.get_uri(),
            verify=False,
        ),
        executors={
            "local": LocalExecutorConfig(
                cores_per_worker=1,
                memory_per_worker=1,
                max_workers=1,
                queue_tags=["tag1", "tag2", "*"],
            ),
            "local2": LocalExecutorConfig(
                cores_per_worker=1, memory_per_worker=1, max_workers=1, queue_tags=["tag3", "tag4"]
            ),
        },
    )

    compute = ComputeManager(compute_config)
    compute_thread = threading.Thread(target=compute.start)
    compute_thread.start()
    time.sleep(2)
    compute.stop()
    compute_thread.join()

    managers = storage_socket.managers.query(ManagerQueryFilters())
    assert len(managers) == 1
    assert set(managers[0]["tags"]) == {"tag1", "tag2", "tag3", "tag4", "*"}


@pytest.mark.filterwarnings("ignore:Exception in thread")
def test_manager_claim_inactive(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake.start_job_runner()

    compute = QCATestingComputeThread(snowflake._qcf_config, {})
    compute.start(manual_updates=False)

    time.sleep(2)  # wait for manager to register
    assert compute.is_alive() is True

    managers = storage_socket.managers.query(ManagerQueryFilters())
    assert len(managers) == 1
    manager_name = managers[0]["name"]

    # Mark as inactive from the server side
    storage_socket.managers.deactivate([manager_name])

    # Next update should kill the process
    time.sleep(compute._compute._compute_config.update_frequency + 2)

    # Should have killed the manager process
    assert compute.is_alive() is False


def test_manager_claim_return(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    all_id, result_data = populate_db(storage_socket)

    compute = QCATestingComputeThread(snowflake._qcf_config, result_data)
    compute.start(manual_updates=False)

    time.sleep(1)  # wait for manager to register
    assert compute.is_alive() is True

    managers = storage_socket.managers.query(ManagerQueryFilters())
    assert len(managers) == 1

    r = snowflake.await_results(all_id, 30.0)
    assert r is True


def test_manager_deferred_return(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    all_id, result_data = populate_db(storage_socket)

    compute_thread = QCATestingComputeThread(snowflake._qcf_config, result_data)
    compute_thread.start(manual_updates=True)
    compute = compute_thread._compute

    time.sleep(1)  # wait for manager to register
    managers = storage_socket.managers.query(ManagerQueryFilters())
    assert len(managers) == 1
    assert compute.n_total_active_tasks == 0  # haven't updated - we are doing manual updates

    # Get some tasks
    compute.update(new_tasks=True)
    assert compute.n_total_active_tasks > 0
    assert compute.n_deferred_tasks == 0

    # Sever goes down
    snowflake.stop_api()

    # Let manager complete some tasks
    time.sleep(3)  # Mock testing adapter waits for two seconds before returning result
    compute.update(new_tasks=True)
    assert compute.n_deferred_tasks > 0
    deferred_task_ids = list(compute._deferred_tasks[0].keys())
    deferred_record_ids = [compute._record_id_map[x] for x in deferred_task_ids]

    # Now server comes back
    snowflake.start_api()

    # Manager can now update
    compute.update(new_tasks=True)

    # No more deferred tasks
    assert compute.n_deferred_tasks == 0
    assert compute.n_total_active_tasks > 0

    # Record is complete on the server
    r = storage_socket.records.get(deferred_record_ids)
    assert all(x["status"] == "complete" for x in r)
    assert all(x["manager_name"] == compute.name for x in r)


def test_manager_missed_heartbeats_shutdown(snowflake: QCATestingSnowflake):
    compute_thread = QCATestingComputeThread(snowflake._qcf_config)
    compute_thread.start(manual_updates=False)

    snowflake.stop_api()

    for i in range(60):
        time.sleep(1)

        if not compute_thread.is_alive():
            break
    else:
        raise RuntimeError("Compute thread did not stop in 60 seconds")

    compute_thread._compute_thread.join(5)
    assert compute_thread.is_alive() is False
