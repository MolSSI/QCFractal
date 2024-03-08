from __future__ import annotations, annotations

from typing import TYPE_CHECKING

from qcarchivetesting import test_users
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_serverinfo_client_delete_error(secure_snowflake: QCATestingSnowflake):
    client = secure_snowflake.client("admin_user", test_users["admin_user"]["pw"])
    storage_socket = secure_snowflake.get_storage_socket()

    admin_id = client.get_user("admin_user").id
    read_id = client.get_user("read_user").id

    error_data_1 = {
        "error_text": "This is a test error",
        "user_id": admin_id,
        "request_path": "/api/v1/molecule",
        "request_headers": "fake_headers",
        "request_body": "fake body",
    }

    error_data_2 = {
        "error_text": "This is another test error",
        "user_id": read_id,
        "request_path": "/api/v1/molecule",
        "request_headers": "fake_headers",
        "request_body": "fake body",
    }

    time_0 = now_at_utc()
    storage_socket.serverinfo.save_error(error_data_1)
    time_12 = now_at_utc()
    storage_socket.serverinfo.save_error(error_data_2)

    n_deleted = client.delete_error_log(before=time_0)
    assert n_deleted == 0

    n_deleted = client.delete_error_log(before=time_12)
    assert n_deleted == 1

    query_res = client.query_error_log()
    errors = list(query_res)
    assert len(errors) == 1
    assert errors[0].user == "read_user"
