from __future__ import annotations

import pytest

from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.testing_helpers import DummyJobProgress


@pytest.fixture(scope="module")
def queryable_stats_client(postgres_server, pytestconfig):
    pg_harness = postgres_server.get_new_harness("serverinfo_test_stats")
    encoding = pytestconfig.getoption("--client-encoding")

    # Don't log accesses
    with QCATestingSnowflake(pg_harness, encoding, log_access=False) as server:
        # generate a bunch of test data
        storage_socket = server.get_storage_socket()
        with storage_socket.session_scope() as session:
            for i in range(100):
                storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

        yield server.client()


def test_serverinfo_client_query_stats(queryable_stats_client):
    query_res = queryable_stats_client.query_server_stats()
    stats = list(query_res)

    test_time = stats[21].timestamp
    query_res = queryable_stats_client.query_server_stats(before=test_time)
    stats = list(query_res)
    assert len(stats) == 79

    query_res = queryable_stats_client.query_server_stats(after=test_time)
    stats = list(query_res)
    assert len(stats) == 22


def test_serverinfo_client_query_stats_empty_iter(queryable_stats_client):
    query_res = queryable_stats_client.query_server_stats()
    assert len(query_res._current_batch) < queryable_stats_client.api_limits["get_server_stats"]

    stats = list(query_res)
    assert len(stats) == 100


def test_serverinfo_client_query_stats_limit(queryable_stats_client):
    query_res = queryable_stats_client.query_server_stats(limit=77)
    stats = list(query_res)
    assert len(stats) == 77
