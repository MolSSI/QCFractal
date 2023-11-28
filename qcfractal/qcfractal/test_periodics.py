from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from qcfractal.components.gridoptimization.testing_helpers import submit_test_data as submit_go_test_data
from qcfractal.components.torsiondrive.testing_helpers import submit_test_data as submit_td_test_data
from qcportal.managers import ManagerName, ManagerStatusEnum
from qcportal.record_models import RecordStatusEnum
from qcportal.serverinfo import ServerStatsQueryFilters
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

pytestmark = pytest.mark.slow


def test_periodics_server_stats(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()

    stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert len(stats) == 0

    sleep_time = snowflake._qcf_config.statistics_frequency

    snowflake.start_job_runner()
    time.sleep(sleep_time * 0.8)

    for i in range(5):
        time_0 = now_at_utc()
        time.sleep(sleep_time)
        time_1 = now_at_utc()

        filters = ServerStatsQueryFilters(before=time_1, after=time_0)
        stats = storage_socket.serverinfo.query_server_stats(filters)
        assert len(stats) == 1
        assert time_0 < stats[0]["timestamp"] < time_1


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
        tags=["tag1"],
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
