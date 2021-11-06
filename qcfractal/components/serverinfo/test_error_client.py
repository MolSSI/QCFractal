"""
Tests the keywords subsocket
"""
from datetime import datetime

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.portal import PortalClient


def test_serverinfo_client_query_error(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
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

    id_1 = storage_socket.serverinfo.save_error(error_data_1)
    time_12 = datetime.utcnow()
    id_2 = storage_socket.serverinfo.save_error(error_data_2)

    meta, errors = snowflake_client.query_error_log()
    assert meta.success
    assert meta.n_returned == 2
    assert meta.n_found == 2

    # Order should be latest access first
    assert errors[0]["error_date"] > errors[1]["error_date"]

    # Query by user
    meta, errors = snowflake_client.query_error_log(username="read_user")
    assert meta.n_returned == 1

    meta, errors = snowflake_client.query_error_log(username="read_user", before=time_12)
    assert meta.n_returned == 0

    meta, errors = snowflake_client.query_error_log(username="read_user", after=time_12)
    assert meta.n_returned == 1


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

    meta, errors = snowflake_client.query_error_log()
    assert meta.success
    assert meta.n_returned == 2
    assert meta.n_found == 2

    n_deleted = snowflake_client.delete_error_log(before=time_0)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_error_log(before=time_12)
    assert n_deleted == 1
    meta, errors = snowflake_client.query_error_log()
    assert meta.n_found == 1
    assert errors[0]["user"] == "read_user"
