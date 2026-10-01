from __future__ import annotations

import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import wait_until
from qcfractal.components.gridoptimization.testing_helpers import submit_procedure_data as submit_go_procedure_data
from qcfractal.components.torsiondrive.testing_helpers import submit_procedure_data as submit_td_procedure_data
from qcportal.managers import ManagerName, ManagerStatusEnum
from qcportal.record_models import RecordStatusEnum
from qcportal.serverinfo.models import AccessLogQueryFilters
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

pytestmark = pytest.mark.slow


def test_periodics_manager_heartbeats(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()

    heartbeat = snowflake._qcf_config.heartbeat_frequency
    max_missed = snowflake._qcf_config.heartbeat_max_missed

    snowflake.start_job_runner()

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # Taken before activating, so that the elapsed time measured below is never longer than
    # the time since the manager's modified_on
    time_0 = time.monotonic()
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "qchem": ["v3.0"]},
        compute_tags=["tag1"],
    )

    # This manager never sends a heartbeat, so the job runner should eventually deactivate it.
    # The check itself only runs once per heartbeat interval, and may be queued behind other
    # internal jobs, so be generous about how long that is allowed to take
    def _is_inactive():
        return storage_socket.managers.get([mname1.fullname])[0]["status"] == ManagerStatusEnum.inactive

    wait_until(_is_inactive, timeout=heartbeat * max_missed + 60, message="Manager was never deactivated")

    # It must not have been deactivated before max_missed heartbeats were actually missed
    assert time.monotonic() - time_0 >= heartbeat * max_missed


def test_periodics_service_iteration(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()

    id_1, _ = submit_td_procedure_data(storage_socket, "td_H2O2_mopac_pm6")

    service_freq = snowflake._qcf_config.service_frequency

    rec = storage_socket.records.get([id_1])
    assert rec[0]["status"] == RecordStatusEnum.waiting

    snowflake.start_job_runner()

    # Services are iterated once at startup, picking up the record submitted above
    wait_until(
        lambda: storage_socket.records.get([id_1])[0]["status"] == RecordStatusEnum.running,
        timeout=service_freq + 60,
        message="The first service was never iterated",
    )

    # Added after that iteration, so it should not be running yet
    id_2, _ = submit_go_procedure_data(storage_socket, "go_H2O2_psi4_b3lyp")
    assert storage_socket.records.get([id_2])[0]["status"] == RecordStatusEnum.waiting

    # ... but the next iteration should pick it up
    wait_until(
        lambda: storage_socket.records.get([id_2])[0]["status"] == RecordStatusEnum.running,
        timeout=service_freq + 60,
        message="The second service was never iterated",
    )

    assert storage_socket.records.get([id_1])[0]["status"] == RecordStatusEnum.running


def test_periodics_delete_old_access_logs(secure_snowflake: QCATestingSnowflake):
    storage_socket = secure_snowflake.get_storage_socket()

    read_id = storage_socket.users.get("read_user")["id"]

    access1 = {
        "module": "api",
        "method": "GET",
        "full_uri": "/api/v1/datasets",
        "ip_address": "127.0.0.1",
        "user_agent": "Fake user agent",
        "request_duration": 0.24,
        "user_id": read_id,
        "request_bytes": 123,
        "response_bytes": 18273,
        "timestamp": now_at_utc() - timedelta(days=2),
    }

    access2 = {
        "module": "api",
        "method": "POST",
        "full_uri": "/api/v1/records",
        "ip_address": "127.0.0.2",
        "user_agent": "Fake user agent",
        "request_duration": 0.45,
        "user_id": read_id,
        "request_bytes": 456,
        "response_bytes": 12671,
        "timestamp": now_at_utc(),
    }

    storage_socket.serverinfo.save_access(access1)
    storage_socket.serverinfo.save_access(access2)

    accesses = storage_socket.serverinfo.query_access_log(AccessLogQueryFilters())
    n_access = len(accesses)

    # There's a set delay at startup before we delete old logs
    secure_snowflake.start_job_runner()

    # we only removed the really "old" (manually added) one
    wait_until(
        lambda: len(storage_socket.serverinfo.query_access_log(AccessLogQueryFilters())) == n_access - 1,
        timeout=60,
        message="Old access logs were never deleted",
    )
