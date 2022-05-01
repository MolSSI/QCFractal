from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from qcportal.serverinfo import ServerStatsQueryFilters

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_serverinfo_socket_update_query_stats(storage_socket: SQLAlchemySocket):

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.n_found == 0
    assert meta.n_returned == 0

    time_0 = datetime.utcnow()

    # Force saving the stats
    storage_socket.serverinfo.update_server_stats()

    time_1 = datetime.utcnow()

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.success
    assert meta.n_found == 1
    assert meta.n_returned == 1

    assert stats[0]["molecule_count"] == 0
    assert stats[0]["outputstore_count"] == 0
    assert stats[0]["record_count"] == 0

    time_0 = datetime.utcnow()

    # Force saving the stats again
    storage_socket.serverinfo.update_server_stats()

    # Should get the latest now
    meta, stats2 = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.success
    assert meta.n_found == 2
    assert meta.n_returned == 2

    # Should return newest first
    assert stats2[1] == stats[0]
    assert stats2[0]["timestamp"] > time_0
    assert stats2[1]["timestamp"] < time_0

    # one more update
    storage_socket.serverinfo.update_server_stats()

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=datetime.utcnow()))
    assert meta.n_found == 3

    meta, stats = storage_socket.serverinfo.query_server_stats(
        ServerStatsQueryFilters(before=datetime.utcnow(), after=time_1)
    )
    assert meta.n_found == 2

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=time_1))
    assert meta.n_found == 1


def test_serverinfo_socket_query_stats(storage_socket: SQLAlchemySocket):

    meta, _ = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.n_found == 0

    time_0 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()
    time_23 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.success
    assert meta.n_found == 3
    assert meta.n_returned == 3

    # Should return newest first
    assert stats[0]["timestamp"] > time_23
    assert stats[1]["timestamp"] < time_23
    assert stats[1]["timestamp"] > time_12

    # Query by times
    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=time_0))
    assert meta.n_found == 0

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=time_12))
    assert meta.n_found == 1

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(after=time_12))
    assert meta.n_found == 2

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(after=datetime.utcnow()))
    assert meta.n_found == 0

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=datetime.utcnow()))
    assert meta.n_found == 3

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(after=time_12, before=time_23))
    assert meta.n_found == 1


def test_serverinfo_socket_delete_stats(storage_socket: SQLAlchemySocket):

    meta, _ = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.n_found == 0

    time_0 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()
    time_23 = datetime.utcnow()
    storage_socket.serverinfo.update_server_stats()

    n_deleted = storage_socket.serverinfo.delete_server_stats(before=time_0)
    assert n_deleted == 0

    n_deleted = storage_socket.serverinfo.delete_server_stats(before=time_12)
    assert n_deleted == 1
    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.n_found == 2
    assert stats[0]["timestamp"] > time_12
    assert stats[1]["timestamp"] > time_12

    n_deleted = storage_socket.serverinfo.delete_server_stats(before=time_12)
    assert n_deleted == 0

    n_deleted = storage_socket.serverinfo.delete_server_stats(before=datetime.utcnow())
    assert n_deleted == 2
    meta, _ = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.n_found == 0
