from __future__ import annotations, annotations

from typing import TYPE_CHECKING

from qcfractal.testing_helpers import DummyJobProgress
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_serverinfo_client_delete_stats(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    time_0 = now_at_utc()
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())
    time_12 = now_at_utc()

    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.update_server_stats(session=session, job_progress=DummyJobProgress())

    query_res = snowflake_client.query_server_stats()
    stats = list(query_res)
    assert len(stats) == 2

    n_deleted = snowflake_client.delete_server_stats(before=time_0)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_server_stats(before=time_12)
    assert n_deleted == 1

    query_res = snowflake_client.query_server_stats()
    stats2 = list(query_res)
    assert len(stats2) == 1
    assert stats2[0] == stats[0]
