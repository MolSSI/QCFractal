from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from qcfractal.testing_helpers import DummyJobProgress
from qcportal.serverinfo import ServerStatsQueryFilters

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_serverinfo_socket_update_stats(storage_socket: SQLAlchemySocket):

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.n_found == 0
    assert len(stats) == 0

    time_0 = datetime.utcnow()

    # Force saving the stats
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    time_1 = datetime.utcnow()

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.success
    assert meta.n_found == 1
    assert len(stats) == 1

    assert stats[0]["molecule_count"] == 0
    assert stats[0]["outputstore_count"] == 0
    assert stats[0]["record_count"] == 0

    time_0 = datetime.utcnow()

    # Force saving the stats again
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    # Should get the latest now
    meta, stats2 = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert meta.success
    assert meta.n_found == 2
    assert len(stats2) == 2

    # Should return newest first
    assert stats2[1] == stats[0]
    assert stats2[0]["timestamp"] > time_0
    assert stats2[1]["timestamp"] < time_0

    # one more update
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=datetime.utcnow()))
    assert meta.n_found == 3

    meta, stats = storage_socket.serverinfo.query_server_stats(
        ServerStatsQueryFilters(before=datetime.utcnow(), after=time_1)
    )
    assert meta.n_found == 2

    meta, stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=time_1))
    assert meta.n_found == 1
