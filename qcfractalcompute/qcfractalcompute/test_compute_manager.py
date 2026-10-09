from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
import requests

from qcarchivetesting import wait_until
from qcfractalcompute.compute_manager import ComputeManager
from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig
from qcfractalcompute.testing_helpers import QCATestingComputeThread, populate_db
from qcportal.managers import ManagerStatusEnum, ManagerQueryFilters
from qcportal.record_models import RecordStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


@contextmanager
def server_outage(snowflake: QCATestingSnowflake, compute: ComputeManager, error):
    """
    Simulates the server being unavailable to the manager

    error is either "stop_api" (actually stop the server, so connections are refused), an HTTP status
    code (returned as an HTML error page, as a reverse proxy in front of a down server would), or an
    exception to raise from the connection
    """

    if error == "stop_api":
        snowflake.stop_api()
        try:
            yield
        finally:
            snowflake.start_api()
        return

    def _send(*args, **kwargs):
        if isinstance(error, int):
            r = requests.Response()
            r.status_code = error
            r.reason = "Gateway Problems"
            r.headers["Content-Type"] = "text/html"
            r._content = b"<html><body>The server is unavailable</body></html>"
            return r
        raise error

    session = compute.client._req_session
    session.send = _send
    try:
        yield
    finally:
        del session.send


# Errors the manager should ride out
outage_errors = [
    "stop_api",
    500,
    502,
    503,
    504,
    429,
    requests.exceptions.ChunkedEncodingError("Connection broken"),
    requests.exceptions.ReadTimeout("Read timed out"),
]


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

    try:
        # Starting up (parsl in particular) can take a while on a loaded machine
        managers = wait_until(
            lambda: storage_socket.managers.query(ManagerQueryFilters()),
            timeout=60,
            message="Manager never registered with the server",
        )
    finally:
        compute.stop()
        compute_thread.join()

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


@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
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

    # Next update should cleanly shut down the manager
    wait_until(
        lambda: not compute.is_alive(),
        timeout=compute._compute._compute_config.update_frequency + 60,
        message="Manager was not shut down after being deactivated",
    )
    assert compute._compute._is_stopping


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


@pytest.mark.parametrize("error", outage_errors)
def test_manager_deferred_return(snowflake: QCATestingSnowflake, error):
    # The server becomes unavailable in various ways (not accepting connections at all, a reverse
    # proxy returning 502/503/504, etc). The manager should hold on to its results and return them
    # once the server is back
    storage_socket = snowflake.get_storage_socket()
    all_id, result_data = populate_db(storage_socket)

    compute_thread = QCATestingComputeThread(snowflake._qcf_config, result_data)
    compute_thread.start(manual_updates=True)
    compute = compute_thread._compute

    time.sleep(1)  # wait for manager to register

    compute.update(new_tasks=True)
    assert compute.n_total_active_tasks > 0
    assert compute.n_deferred_tasks == 0

    time.sleep(3)  # Mock testing adapter waits for two seconds before returning result

    with server_outage(snowflake, compute, error):
        compute.update(new_tasks=True)
        assert compute.n_deferred_tasks > 0
        deferred_task_ids = list(compute._deferred_tasks[0].keys())

        # Retrying the deferred tasks also fails, but that is fine too
        compute.update(new_tasks=True)
        assert set(compute._deferred_tasks[1].keys()) == set(deferred_task_ids)

        # Missed heartbeats are counted, but the manager keeps going
        compute.heartbeat()
        assert compute._failed_heartbeats == 1

    assert compute_thread.is_alive()
    assert not compute._is_stopping

    compute.heartbeat()
    assert compute._failed_heartbeats == 0

    compute.update(new_tasks=True)
    assert compute.n_deferred_tasks == 0
    assert compute.n_total_active_tasks > 0  # claimed more tasks

    deferred_record_ids = [compute._record_id_map[x] for x in deferred_task_ids]
    r = storage_socket.records.get(deferred_record_ids)
    assert all(x["status"] == "complete" for x in r)
    assert all(x["manager_name"] == compute.name for x in r)

    compute_thread.stop()


@pytest.mark.parametrize("error", [502, 504])
def test_manager_claim_outage(snowflake: QCATestingSnowflake, error):
    storage_socket = snowflake.get_storage_socket()

    compute_thread = QCATestingComputeThread(snowflake._qcf_config, {})
    compute_thread.start(manual_updates=True)
    compute = compute_thread._compute

    time.sleep(1)  # wait for manager to register

    with server_outage(snowflake, compute, error):
        compute.update(new_tasks=True)

    assert compute_thread.is_alive()
    assert compute.n_total_active_tasks == 0

    # Claims work again once the server is back
    all_id, _ = populate_db(storage_socket)
    compute.update(new_tasks=True)
    assert compute.n_total_active_tasks > 0

    compute_thread.stop()


def test_manager_outage_missed_heartbeats_shutdown(snowflake: QCATestingSnowflake):
    compute_thread = QCATestingComputeThread(snowflake._qcf_config)
    compute_thread.start(manual_updates=True)
    compute = compute_thread._compute

    max_missed = compute.client.server_info["manager_heartbeat_max_missed"]

    with server_outage(snowflake, compute, 504):
        for i in range(max_missed):
            compute.heartbeat()
            assert not compute._is_stopping

        compute.heartbeat()
        assert compute._is_stopping

    compute_thread._compute_thread.join(30)
    assert compute_thread.is_alive() is False


@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
def test_manager_deactivated_during_outage(snowflake: QCATestingSnowflake):
    # The server was down long enough that it deactivated the manager (missed heartbeats) once it
    # came back. The manager can no longer return its results, and should shut down cleanly
    storage_socket = snowflake.get_storage_socket()
    all_id, result_data = populate_db(storage_socket)

    compute_thread = QCATestingComputeThread(snowflake._qcf_config, result_data)
    compute_thread.start(manual_updates=True)
    compute = compute_thread._compute

    time.sleep(1)  # wait for manager to register

    compute.update(new_tasks=True)
    assert compute.n_total_active_tasks > 0

    time.sleep(3)  # Mock testing adapter waits for two seconds before returning result

    with server_outage(snowflake, compute, 503):
        compute.update(new_tasks=True)
        assert compute.n_deferred_tasks > 0

    storage_socket.managers.deactivate([compute.name])

    compute.update(new_tasks=True)
    assert compute._is_stopping

    compute_thread._compute_thread.join(30)
    assert compute_thread.is_alive() is False


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
    # Submit the work before the manager starts. The manager starts its idle timer the moment it
    # starts up, so anything done between startup and the first claimable task counts against
    # max_idle_time - populate_db is not fast enough to rely on winning that race
    all_id, _ = populate_db(storage_socket)

    compute_thread = QCATestingComputeThread(snowflake._qcf_config, additional_manager_config=add_config)
    compute_thread.start(manual_updates=False)

    time.sleep(2)
    assert compute_thread.is_alive()

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
