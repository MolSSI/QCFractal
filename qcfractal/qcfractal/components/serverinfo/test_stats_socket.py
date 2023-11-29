from __future__ import annotations

from typing import TYPE_CHECKING

from qcfractal.testing_helpers import DummyJobProgress
from qcportal.serverinfo import ServerStatsQueryFilters
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_serverinfo_socket_update_stats(storage_socket: SQLAlchemySocket):
    stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert len(stats) == 0

    time_0 = now_at_utc()

    # Force saving the stats
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    time_1 = now_at_utc()

    stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert len(stats) == 1

    assert stats[0]["molecule_count"] == 0
    assert stats[0]["outputstore_count"] == 0
    assert stats[0]["record_count"] == 0

    time_0 = now_at_utc()

    # Force saving the stats again
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    # Should get the latest now
    stats2 = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters())
    assert len(stats2) == 2

    # Should return newest first
    assert stats2[1] == stats[0]
    assert stats2[0]["timestamp"] > time_0
    assert stats2[1]["timestamp"] < time_0

    # one more update
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=now_at_utc()))
    assert len(stats) == 3

    stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=now_at_utc(), after=time_1))
    assert len(stats) == 2

    stats = storage_socket.serverinfo.query_server_stats(ServerStatsQueryFilters(before=time_1))
    assert len(stats) == 1
