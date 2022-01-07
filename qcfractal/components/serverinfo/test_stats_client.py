from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


def test_serverinfo_client_query_stats(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):

    meta, stats = snowflake_client.query_server_stats()
    assert meta.success
    assert meta.n_found == 0

    time_0 = datetime.utcnow()

    # Force saving the stats
    storage_socket.serverinfo.update_server_stats()

    time_12 = datetime.utcnow()

    meta, stats = snowflake_client.query_server_stats()
    assert meta.success
    assert meta.n_found == 1
    assert meta.n_returned == 1

    assert stats[0]["molecule_count"] == 0
    assert stats[0]["outputstore_count"] == 0
    assert stats[0]["record_count"] == 0

    # Force saving the stats again
    storage_socket.serverinfo.update_server_stats()

    # Should get the latest now
    meta, stats = snowflake_client.query_server_stats()
    assert meta.success
    assert meta.n_found == 2
    assert meta.n_returned == 2

    # Should return newest first
    assert stats[0]["timestamp"] > stats[1]["timestamp"]

    time_23 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()

    # Query by times
    meta, stats = storage_socket.serverinfo.query_server_stats(before=time_0)
    assert meta.n_found == 0

    meta, stats = storage_socket.serverinfo.query_server_stats(before=time_12)
    assert meta.n_found == 1

    meta, stats = storage_socket.serverinfo.query_server_stats(after=time_12)
    assert meta.n_found == 2

    meta, stats = storage_socket.serverinfo.query_server_stats(after=datetime.utcnow())
    assert meta.n_found == 0

    meta, stats = storage_socket.serverinfo.query_server_stats(before=datetime.utcnow())
    assert meta.n_found == 3

    meta, stats = storage_socket.serverinfo.query_server_stats(after=time_12, before=time_23)
    assert meta.n_found == 1


def test_serverinfo_client_delete_stats(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    time_0 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()

    meta, errors = snowflake_client.query_server_stats()
    assert meta.success
    assert meta.n_returned == 2
    assert meta.n_found == 2

    n_deleted = snowflake_client.delete_server_stats(before=time_0)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_server_stats(before=time_12)
    assert n_deleted == 1

    meta, errors2 = snowflake_client.query_server_stats()
    assert meta.n_found == 1
    assert errors2[0] == errors[0]
