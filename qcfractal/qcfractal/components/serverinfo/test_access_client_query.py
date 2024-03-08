from __future__ import annotations

import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.serverinfo.test_access_socket import test_ips
from qcportal import PortalClient
from qcportal.utils import now_at_utc


@pytest.fixture(scope="module")
def queryable_access_client(postgres_server, pytestconfig):
    pg_harness = postgres_server.get_new_harness("serverinfo_test_access")
    encoding = pytestconfig.getoption("--client-encoding")

    # Don't log accesses
    with QCATestingSnowflake(pg_harness, encoding, enable_security=True, create_users=True, log_access=False) as server:
        # generate a bunch of test data
        storage_socket = server.get_storage_socket()

        admin_uid = storage_socket.users.get("admin_user")["id"]
        read_uid = storage_socket.users.get("read_user")["id"]

        with storage_socket.session_scope() as session:
            for i in range(10):
                for user_id in [admin_uid, read_uid]:
                    for endpoint in ["molecules", "records", "wavefunctions", "managers"]:
                        for method in ["GET", "POST"]:
                            access = {
                                "module": "api" if i % 2 == 0 else "compute",
                                "full_uri": f"api/v1/{endpoint}",
                                "method": method,
                                "ip_address": test_ips[0][0],
                                "user_agent": "Fake user agent",
                                "request_duration": 0.25 * i,
                                "user_id": user_id,
                                "request_bytes": 2 * i,
                                "response_bytes": 4 * i,
                            }
                            storage_socket.serverinfo.save_access(access, session=session)

        yield server.client("admin_user", test_users["admin_user"]["pw"])


def test_serverinfo_client_query_access(queryable_access_client: PortalClient):
    query_res = queryable_access_client.query_access_log(method=["get"])
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(method=["POST"])
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(module=["api"])
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(module=["compute"])
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(user="admin_user")
    all_entries = list(query_res)
    assert len(all_entries) == 80

    read_id = queryable_access_client.get_user("read_user").id
    query_res = queryable_access_client.query_access_log(user=[read_id, "admin_user"])
    all_entries = list(query_res)
    assert len(all_entries) == 160

    query_res = queryable_access_client.query_access_log(user=["no_user"])
    all_entries = list(query_res)
    assert len(all_entries) == 0


def test_serverinfo_client_query_access_empty_iter(queryable_access_client: PortalClient):
    query_res = queryable_access_client.query_access_log()
    assert len(query_res._current_batch) < queryable_access_client.api_limits["get_access_logs"]

    all_entries = list(query_res)
    assert len(all_entries) == 160


def test_serverinfo_client_query_access_limit(queryable_access_client: PortalClient):
    query_res = queryable_access_client.query_access_log(limit=99)
    all_entries = list(query_res)
    assert len(all_entries) == 99


def test_serverinfo_client_access_summary(queryable_access_client: PortalClient):
    # Just test that it works
    # TODO - better way of testing. Prepopulated db?
    now = now_at_utc()
    r = queryable_access_client.query_access_summary()
    assert list(r.entries.keys())[0] == now.strftime("%Y-%m-%d")

    r = queryable_access_client.query_access_summary(group_by="user")
    assert set(r.entries.keys()) == {"admin_user", "read_user"}
