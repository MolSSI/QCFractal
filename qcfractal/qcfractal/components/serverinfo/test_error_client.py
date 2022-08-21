from __future__ import annotations, annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


def test_serverinfo_client_delete_error(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    error_data_1 = {
        "error_text": "This is a test error",
        "user": "admin_user",
        "request_path": "/v1/molecule",
        "request_headers": "fake_headers",
        "request_body": "fake body",
    }

    error_data_2 = {
        "error_text": "This is another test error",
        "user": "read_user",
        "request_path": "/v1/molecule",
        "request_headers": "fake_headers",
        "request_body": "fake body",
    }

    time_0 = datetime.utcnow()
    storage_socket.serverinfo.save_error(error_data_1)
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.save_error(error_data_2)

    n_deleted = snowflake_client.delete_error_log(before=time_0)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_error_log(before=time_12)
    assert n_deleted == 1

    query_res = snowflake_client.query_error_log()
    assert query_res.current_meta.n_found == 1
    errors = list(query_res)
    assert errors[0].user == "read_user"
