from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from qcfractalcompute.compute_manager import ComputeManager
from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig
from qcfractalcompute.testing_helpers import QCATestingComputeThread, populate_db
from qcportal.managers import ManagerStatusEnum, ManagerQueryFilters
from qcportal.record_models import RecordStatusEnum
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
                compute_tags=["tag1", "tag2", "*"],
            ),
            "local2": LocalExecutorConfig(
                cores_per_worker=1, memory_per_worker=1, max_workers=1, compute_tags=["tag3", "tag4"]
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


def test_manager_tags_missing(snowflake: QCATestingSnowflake, tmp_path):
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
                compute_tags=["tag1", "tag2", "*"],
            ),
            "local2": LocalExecutorConfig(cores_per_worker=1, memory_per_worker=1, max_workers=1, compute_tags=[]),
        },
    )

    with pytest.raises(ValueError, match="local2 has no compute tags"):
        ComputeManager(compute_config)


def test_manager_tags_duplicate(snowflake: QCATestingSnowflake, tmp_path):
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
                compute_tags=["tag1", "tag2", "*"],
            ),
            "local2": LocalExecutorConfig(
                cores_per_worker=1, memory_per_worker=1, max_workers=1, compute_tags=["tag2", "tag1"]
            ),
        },
    )

    compute = ComputeManager(compute_config)
    assert compute.all_compute_tags == ["tag1", "tag2", "*"]


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

    for i in range(90):
        time.sleep(1)

        if not compute_thread.is_alive():
            break
    else:
        raise RuntimeError("Compute thread did not stop in 90 seconds")

    compute_thread._compute_thread.join(5)
    assert compute_thread.is_alive() is False


def test_manager_idle_shutdown_0(snowflake: QCATestingSnowflake):
    add_config = {"max_idle_time": 0}
    compute_thread = QCATestingComputeThread(snowflake._qcf_config, additional_manager_config=add_config)
    compute_thread.start(manual_updates=False)

    for i in range(10):
        time.sleep(1)
        if not compute_thread.is_alive():
            break
    else:
        raise RuntimeError("Compute thread did not stop in 10 seconds")

    compute_thread._compute_thread.join(5)
    assert compute_thread.is_alive() is False


def test_manager_idle_shutdown_5(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()

    max_idle_time = 5
    add_config = {"max_idle_time": max_idle_time}
    compute_thread = QCATestingComputeThread(snowflake._qcf_config, additional_manager_config=add_config)
    compute_thread.start(manual_updates=False)

    time.sleep(2)
    assert compute_thread.is_alive()

    all_id, _ = populate_db(storage_socket)

    # The manager must stay alive for as long as it has work to do. How long that takes is not
    # something we can predict (the mock executor runs the tasks one at a time, and CI machines
    # are slow and unevenly loaded), so poll instead of sleeping a fixed amount
    deadline = time.monotonic() + 180
    while True:
        statuses = [r["status"] for r in storage_socket.records.get(all_id, include=["status"])]
        if all(s in (RecordStatusEnum.complete, RecordStatusEnum.error, RecordStatusEnum.invalid) for s in statuses):
            break

        assert compute_thread.is_alive(), "Manager shut down while it still had tasks to run"
        assert time.monotonic() < deadline, f"Manager did not finish the tasks in time (statuses: {statuses})"
        time.sleep(0.5)

    work_done = time.monotonic()

    # The manager only starts counting idle time once the last task is returned, so it must still
    # be alive well before max_idle_time has elapsed from this point
    time.sleep(max_idle_time / 2.0)
    assert compute_thread.is_alive(), "Manager shut down before max_idle_time elapsed"

    # ... and it must shut itself down shortly after
    deadline = work_done + max_idle_time + 30
    while compute_thread.is_alive():
        assert time.monotonic() < deadline, "Manager did not shut down after being idle"
        time.sleep(0.5)

    compute_thread._compute_thread.join(5)
    assert compute_thread.is_alive() is False
