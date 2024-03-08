from __future__ import annotations

from typing import TYPE_CHECKING

from qcportal.serverinfo import ErrorLogQueryFilters
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_serverinfo_socket_save_error(secure_snowflake: QCATestingSnowflake):
    storage_socket = secure_snowflake.get_storage_socket()
    admin_id = storage_socket.users.get("admin_user")["id"]
    read_id = storage_socket.users.get("read_user")["id"]

    userid_map = {admin_id: "admin_user", read_id: "read_user"}

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

    all_errors = [error_data_1, error_data_2]
    id_1 = storage_socket.serverinfo.save_error(error_data_1)
    time_12 = now_at_utc()
    id_2 = storage_socket.serverinfo.save_error(error_data_2)

    errors = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters())
    assert len(errors) == 2

    # Returned in chrono order, newest first
    assert errors[0]["id"] == id_2
    assert errors[1]["id"] == id_1
    assert errors[0]["error_date"] > errors[1]["error_date"]

    for in_err, db_err in zip(reversed(all_errors), errors):
        assert in_err["error_text"] == db_err["error_text"]
        assert userid_map[in_err["user_id"]] == db_err["user"]
        assert in_err["request_path"] == db_err["request_path"]
        assert in_err["request_headers"] == db_err["request_headers"]
        assert in_err["request_body"] == db_err["request_body"]

    # Query by id
    err = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters(error_id=[id_2]))
    assert len(err) == 1
    assert err[0]["error_text"] == error_data_2["error_text"]

    # query by time
    err = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters(before=time_12))
    assert len(err) == 1
    assert err[0]["error_text"] == error_data_1["error_text"]

    err = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters(after=now_at_utc()))
    assert len(err) == 0

    err = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters(before=now_at_utc(), after=time_12))
    assert len(err) == 1

    err = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters(after=now_at_utc(), before=time_12))
    assert len(err) == 0

    # query by user
    err = storage_socket.serverinfo.query_error_log(ErrorLogQueryFilters(user=["read_user"]))
    assert len(err) == 1
