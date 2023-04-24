from __future__ import annotations

from datetime import datetime

import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.serverinfo.test_access_socket import test_ips
from qcportal import PortalClient


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
                                "access_type": f"v1/{endpoint}",
                                "access_method": method,
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

    query_res = queryable_access_client.query_access_log(access_method=["get"])
    assert query_res._current_meta.n_found == 80
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(access_method=["POST"])
    assert query_res._current_meta.n_found == 80
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(user="admin_user")
    assert query_res._current_meta.n_found == 80
    all_entries = list(query_res)
    assert len(all_entries) == 80

    read_id = queryable_access_client.get_user("read_user").id
    query_res = queryable_access_client.query_access_log(user=[read_id, "admin_user"])
    assert query_res._current_meta.n_found == 160
    all_entries = list(query_res)
    assert len(all_entries) == 160

    query_res = queryable_access_client.query_access_log(user=["no_user"])
    assert query_res._current_meta.n_found == 0

    query_res = queryable_access_client.query_access_log(access_type=["v1/molecules"], access_method="get")
    assert query_res._current_meta.n_found == 20
    all_entries = list(query_res)
    assert len(all_entries) == 20

    query_res = queryable_access_client.query_access_log(access_type=["v1/records"])
    all_entries = list(query_res)
    assert len(all_entries) == 40

    # get a date. note that results are returned in descending order based on access_date
    test_time = all_entries[10].access_date
    query_res = queryable_access_client.query_access_log(access_type=["v1/records"], before=test_time)
    all_entries = list(query_res)
    assert len(all_entries) == 30
    assert all(x.access_date <= test_time for x in all_entries)

    query_res = queryable_access_client.query_access_log(access_type=["v1/records"], after=test_time)
    all_entries = list(query_res)
    assert len(all_entries) == 11
    assert all(x.access_date >= test_time for x in all_entries)


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
    now = datetime.utcnow()
    r = queryable_access_client.query_access_summary()
    assert list(r.entries.keys())[0] == now.strftime("%Y-%m-%d")

    r = queryable_access_client.query_access_summary(group_by="user")
    assert set(r.entries.keys()) == {"admin_user", "read_user"}
