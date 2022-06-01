from __future__ import annotations, annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from ...testing_helpers import TestingSnowflake
from qcportal.molecules import Molecule

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


@pytest.fixture(scope="module")
def queryable_stats_client(module_temporary_database):
    db_config = module_temporary_database.config

    # Don't log accesses
    with TestingSnowflake(db_config, encoding="application/json", log_access=False) as server:

        # generate a bunch of test data
        storage_socket = server.get_storage_socket()
        with storage_socket.session_scope() as session:
            for i in range(1000):
                storage_socket.serverinfo.update_server_stats(session=session)

        yield server.client()


def test_serverinfo_client_delete_stats(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    time_0 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()

    query_res = snowflake_client.query_server_stats()
    stats = list(query_res)
    assert query_res.current_meta.success
    assert query_res.current_meta.n_found == 2

    n_deleted = snowflake_client.delete_server_stats(before=time_0)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_server_stats(before=time_12)
    assert n_deleted == 1

    query_res = snowflake_client.query_server_stats()
    assert query_res.current_meta.n_found == 1
    stats2 = list(query_res)
    assert stats2[0] == stats[0]


def test_serverinfo_client_query_stats(queryable_stats_client):
    query_res = queryable_stats_client.query_server_stats(limit=100)
    stats = list(query_res)

    test_time = stats[50].timestamp
    query_res = queryable_stats_client.query_server_stats(before=test_time)
    stats = list(query_res)
    assert query_res.current_meta.n_found == 950
    assert len(stats) == 950

    query_res = queryable_stats_client.query_server_stats(after=test_time)
    stats = list(query_res)
    assert query_res.current_meta.n_found == 51
    assert len(stats) == 51


def test_serverinfo_client_query_stats_empty_iter(queryable_stats_client):
    # Future-proof against changes to test infrastructure
    assert queryable_stats_client.api_limits["get_server_stats"] <= 1500

    query_res = queryable_stats_client.query_server_stats()
    assert len(query_res.current_batch) < 1000

    stats = list(query_res)
    assert len(stats) == 1000


def test_serverinfo_client_query_stats_limit(queryable_stats_client):
    query_res = queryable_stats_client.query_server_stats(limit=800)
    stats = list(query_res)
    assert len(stats) == 800
