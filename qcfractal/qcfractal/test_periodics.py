from __future__ import annotations

import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.gridoptimization.testing_helpers import submit_test_data as submit_go_test_data
from qcfractal.components.torsiondrive.testing_helpers import submit_test_data as submit_td_test_data
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

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "qchem": ["v3.0"]},
        compute_tags=["tag1"],
    )

    snowflake.start_job_runner()

    for i in range(max_missed + 1):
        time.sleep(heartbeat)
        manager = storage_socket.managers.get([mname1.fullname])

        if i < max_missed:
            assert manager[0]["status"] == ManagerStatusEnum.active
        else:
            assert manager[0]["status"] == ManagerStatusEnum.inactive


def test_periodics_service_iteration(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()

    id_1, _ = submit_td_test_data(storage_socket, "td_H2O2_mopac_pm6")

    service_freq = snowflake._qcf_config.service_frequency

    rec = storage_socket.records.get([id_1])
    assert rec[0]["status"] == RecordStatusEnum.waiting

    snowflake.start_job_runner()

    time.sleep(1.0)

    # added after startup
    id_2, _ = submit_go_test_data(storage_socket, "go_H2O2_psi4_b3lyp")

    # The first services iterated at startup
    rec = storage_socket.records.get([id_1, id_2])
    assert rec[0]["status"] == RecordStatusEnum.running
    assert rec[1]["status"] == RecordStatusEnum.waiting

    # wait for the next iteration. Then both should be running
    time.sleep(service_freq + 0.5)
    rec = storage_socket.records.get([id_1, id_2])
    assert rec[0]["status"] == RecordStatusEnum.running
    assert rec[1]["status"] == RecordStatusEnum.running


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

    secure_snowflake.start_job_runner()
    # There's a set delay at startup before we delete old logs
    time.sleep(5.0)

    # we only removed the really "old" (manually added) one
    accesses = storage_socket.serverinfo.query_access_log(AccessLogQueryFilters())
    assert len(accesses) == n_access - 1
