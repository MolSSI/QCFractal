import datetime

from qcfractal.db_socket import SQLAlchemySocket


def test_serverinfo_socket_save_query_error(storage_socket: SQLAlchemySocket):

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

    all_errors = [error_data_1, error_data_2]
    id_1 = storage_socket.serverinfo.save_error(error_data_1)
    time_12 = datetime.datetime.utcnow()
    id_2 = storage_socket.serverinfo.save_error(error_data_2)

    meta, errors = storage_socket.serverinfo.query_error_log()
    assert meta.n_found == 2
    assert meta.n_returned == 2

    # Returned in chrono order, newest first
    assert errors[0]["id"] == id_2
    assert errors[1]["id"] == id_1
    assert errors[0]["error_date"] > errors[1]["error_date"]

    for in_err, db_err in zip(reversed(all_errors), errors):
        assert in_err["error_text"] == db_err["error_text"]
        assert in_err["user"] == db_err["user"]
        assert in_err["request_path"] == db_err["request_path"]
        assert in_err["request_headers"] == db_err["request_headers"]
        assert in_err["request_body"] == db_err["request_body"]

    # Query by id
    meta, err = storage_socket.serverinfo.query_error_log(id=[id_2])
    assert meta.n_found == 1
    assert err[0]["error_text"] == error_data_2["error_text"]

    # query by time
    meta, err = storage_socket.serverinfo.query_error_log(before=time_12)
    assert meta.n_found == 1
    assert err[0]["error_text"] == error_data_1["error_text"]

    meta, err = storage_socket.serverinfo.query_error_log(after=datetime.datetime.utcnow())
    assert meta.n_found == 0

    meta, err = storage_socket.serverinfo.query_error_log(before=datetime.datetime.utcnow(), after=time_12)
    assert meta.n_found == 1

    meta, err = storage_socket.serverinfo.query_error_log(after=datetime.datetime.utcnow(), before=time_12)
    assert meta.n_found == 0

    # query by user
    meta, err = storage_socket.serverinfo.query_error_log(username=["read_user"])
    assert meta.n_found == 1


def test_serverinfo_socket_delete_error(storage_socket: SQLAlchemySocket):

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

    time_0 = datetime.datetime.utcnow()
    storage_socket.serverinfo.save_error(error_data_1)
    time_12 = datetime.datetime.utcnow()
    storage_socket.serverinfo.save_error(error_data_2)

    meta, errors = storage_socket.serverinfo.query_error_log()
    assert meta.n_found == 2
    assert meta.n_returned == 2

    n_deleted = storage_socket.serverinfo.delete_error_logs(before=time_0)
    assert n_deleted == 0
    meta, errors = storage_socket.serverinfo.query_error_log()
    assert meta.n_found == 2

    n_deleted = storage_socket.serverinfo.delete_error_logs(before=time_12)
    assert n_deleted == 1
    meta, errors = storage_socket.serverinfo.query_error_log()
    assert errors[0]["error_date"] > time_12

    n_deleted = storage_socket.serverinfo.delete_error_logs(before=datetime.datetime.utcnow())
    assert n_deleted == 1
