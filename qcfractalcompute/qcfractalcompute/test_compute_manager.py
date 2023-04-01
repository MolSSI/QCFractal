from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import pytest
import qcengine as qcng

from qcarchivetesting import caplog_handler_at_level
from qcfractalcompute.compute_manager import ComputeManager
from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig
from qcfractalcompute.testing_helpers import QCATestingComputeThread, populate_db
from qcportal.managers import ManagerStatusEnum, ManagerQueryFilters
from qcportal.record_models import RecordStatusEnum

# For testing only! We just make all available programs/procedures the same as all of them
qcng.list_available_programs = qcng.list_all_programs
qcng.list_available_procedures = qcng.list_all_procedures

if TYPE_CHECKING:
    from qcfractal.testing_helpers import QCATestingSnowflake, SQLAlchemySocket


def test_manager_keepalive(snowflake: QCATestingSnowflake, storage_socket: SQLAlchemySocket):

    snowflake.start_job_runner()

    compute = QCATestingComputeThread(snowflake._qcf_config, {})
    compute.start()

    time.sleep(1)  # wait for manager to register

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    manager_name = managers[0]["name"]

    sleep_time = snowflake._qcf_config.heartbeat_frequency
    max_missed = snowflake._qcf_config.heartbeat_max_missed

    for i in range(max_missed * 2):
        time.sleep(sleep_time)
        m = storage_socket.managers.get([manager_name])
        assert m[0]["status"] == ManagerStatusEnum.active

    # force missing too many heartbeats
    compute.stop()

    time.sleep(sleep_time * (max_missed + 1))
    m = storage_socket.managers.get([manager_name])
    assert m[0]["status"] == ManagerStatusEnum.inactive


def test_manager_tags(snowflake: QCATestingSnowflake, storage_socket: SQLAlchemySocket, tmp_path):

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

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    assert set(managers[0]["tags"]) == {"tag1", "tag2", "tag3", "tag4", "*"}


@pytest.mark.filterwarnings("ignore:Exception in thread")
def test_manager_claim_inactive(snowflake: QCATestingSnowflake, storage_socket: SQLAlchemySocket):
    snowflake.start_job_runner()

    compute = QCATestingComputeThread(snowflake._qcf_config, {})
    compute.start()

    time.sleep(2)  # wait for manager to register
    assert compute.is_alive() is True

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    manager_name = managers[0]["name"]

    storage_socket.managers.deactivate([manager_name])

    # Next update should kill the process
    time.sleep(2 + 2)  # update_frequency is 2, wait another two seconds as well

    # Should have killed the manager process
    assert compute.is_alive() is False


def test_manager_claim_return(snowflake: QCATestingSnowflake, storage_socket: SQLAlchemySocket):
    all_id, result_data = populate_db(storage_socket)

    compute = QCATestingComputeThread(snowflake._qcf_config, result_data)
    compute.start()

    time.sleep(1)  # wait for manager to register
    assert compute.is_alive() is True

    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1

    r = snowflake.await_results(all_id, 30.0)
    assert r is True


def test_manager_deferred_return(snowflake: QCATestingSnowflake, storage_socket: SQLAlchemySocket):
    all_id, result_data = populate_db(storage_socket)

    compute_thread = QCATestingComputeThread(snowflake._qcf_config, result_data)
    compute_thread.start()
    compute = compute_thread._compute

    time.sleep(1)  # wait for manager to register
    meta, managers = storage_socket.managers.query(ManagerQueryFilters())
    assert meta.n_found == 1
    assert compute.n_total_active_tasks > 0

    # Sever goes down
    snowflake.stop_flask()

    # Let manager try to update
    time.sleep(compute._compute_config.update_frequency + 1)
    assert compute.n_deferred_tasks > 0

    # Now server comes back
    snowflake.start_flask()

    time.sleep(compute._compute_config.update_frequency + 1)

    # Now we have more tasks, and no more deferred
    assert compute.n_deferred_tasks == 0

    # Finish the rest
    snowflake.await_results()

    recs = storage_socket.records.get(all_id)
    assert all(x["status"] != RecordStatusEnum.waiting for x in recs)
    assert all(x["manager_name"] == compute.name for x in recs)


def test_manager_deferred_drop(snowflake: QCATestingSnowflake, storage_socket: SQLAlchemySocket, caplog):

    with caplog_handler_at_level(caplog, logging.WARNING):
        all_id, result_data = populate_db(storage_socket)

        compute_thread = QCATestingComputeThread(snowflake._qcf_config, result_data)
        compute_thread.start()
        compute = compute_thread._compute

        time.sleep(1)  # wait for manager to register
        meta, managers = storage_socket.managers.query(ManagerQueryFilters())
        assert meta.n_found == 1

        # Wait for manager to claim tasks
        time.sleep(compute._compute_config.update_frequency + 1)
        assert compute.n_total_active_tasks > 0
        assert compute.n_deferred_tasks == 0

        # Sever goes down
        snowflake.stop_flask()

        # Manager tries to update several times, eventually giving up on returning those tasks
        time.sleep(compute._compute_config.update_frequency * (compute._compute_config.server_error_retries + 1))

        # server comes back up
        snowflake.start_flask()
        time.sleep(compute._compute_config.update_frequency + 1)

        recs = storage_socket.records.get(all_id)
        assert all(
            (x["status"] in (RecordStatusEnum.complete, RecordStatusEnum.running) and x["manager_name"] == compute.name)
            or (x["status"] == RecordStatusEnum.waiting)
            for x in recs
        )

        assert compute.n_deferred_tasks == 0

    assert "updates ago and over attempt limit. Dropping" in caplog.text


def test_manager_missed_heartbeats_shutdown(snowflake: QCATestingSnowflake):

    compute_thread = QCATestingComputeThread(snowflake._qcf_config)
    compute_thread.start()

    snowflake.stop_flask()

    time.sleep(snowflake._qcf_config.heartbeat_frequency * (snowflake._qcf_config.heartbeat_max_missed + 2))

    compute_thread._compute_thread.join(10)
    assert compute_thread.is_alive() is False
