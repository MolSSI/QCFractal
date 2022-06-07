from __future__ import annotations, annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


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
