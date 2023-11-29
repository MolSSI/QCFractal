from __future__ import annotations

import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcportal import PortalClient


@pytest.fixture(scope="module")
def queryable_error_client(postgres_server, pytestconfig):
    pg_harness = postgres_server.get_new_harness("serverinfo_test_errors")
    encoding = pytestconfig.getoption("--client-encoding")

    with QCATestingSnowflake(pg_harness, encoding, create_users=True, enable_security=True, log_access=False) as server:
        # generate a bunch of test data
        storage_socket = server.get_storage_socket()

        admin_id = storage_socket.users.get("admin_user")["id"]
        read_id = storage_socket.users.get("read_user")["id"]

        with storage_socket.session_scope() as session:
            for i in range(20):
                for endpoint in ["molecules", "records", "wavefunctions", "managers"]:
                    for user_id in [read_id, admin_id]:
                        error = {
                            "error_text": f"ERROR_{i}_{endpoint}_{user_id}",
                            "user_id": user_id,
                            "request_path": "/api/v1/endpoint",
                            "request_headers": "fake_headers",
                            "request_body": "fake body",
                        }
                        storage_socket.serverinfo.save_error(error, session=session)

        yield server.client("admin_user", test_users["admin_user"]["pw"])


def test_serverinfo_client_query_error(queryable_error_client: PortalClient):
    # Query by user
    query_res = queryable_error_client.query_error_log(user="read_user")
    errors = list(query_res)
    assert len(errors) == 80
    assert all(x.user == "read_user" for x in errors)

    # Get a time
    test_time = errors[20].error_date
    query_res = queryable_error_client.query_error_log(user="read_user", before=test_time)
    errors = list(query_res)
    assert len(errors) == 60
    assert all(x.error_date <= test_time for x in errors)

    query_res = queryable_error_client.query_error_log(user="read_user", after=test_time)
    errors = list(query_res)
    assert len(errors) == 21
    assert all(x.error_date >= test_time for x in errors)

    # query by id
    ids = [errors[12].id, errors[15].id]
    query_res = queryable_error_client.query_error_log(error_id=ids)
    errors = list(query_res)
    assert len(errors) == 2
    assert all(x.id in ids for x in errors)


def test_serverinfo_client_query_error_empty_iter(queryable_error_client: PortalClient):
    query_res = queryable_error_client.query_error_log()
    assert len(query_res._current_batch) < queryable_error_client.api_limits["get_error_logs"]

    all_entries = list(query_res)
    assert len(all_entries) == 160


def test_serverinfo_client_query_error_limit(queryable_error_client: PortalClient):
    query_res = queryable_error_client.query_error_log(limit=38)
    all_entries = list(query_res)
    assert len(all_entries) == 38
