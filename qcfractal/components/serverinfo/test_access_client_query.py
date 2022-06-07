from __future__ import annotations

from datetime import datetime

import pytest

from qcfractal.components.serverinfo.test_access_socket import test_ips
from qcfractal.testing_helpers import TestingSnowflake
from qcportal import PortalClient


@pytest.fixture(scope="module")
def queryable_access_client(module_temporary_database):
    db_config = module_temporary_database.config

    # Don't log accesses
    with TestingSnowflake(db_config, encoding="application/json", log_access=False) as server:

        # generate a bunch of test data
        storage_socket = server.get_storage_socket()
        with storage_socket.session_scope() as session:
            for i in range(10):
                for user in ["admin_user", "read_user"]:
                    for endpoint in ["molecules", "records", "wavefunctions", "managers"]:
                        for method in ["GET", "POST"]:
                            access = {
                                "access_type": f"v1/{endpoint}",
                                "access_method": method,
                                "ip_address": test_ips[0][0],
                                "user_agent": "Fake user agent",
                                "request_duration": 0.25 * i,
                                "user": user,
                                "request_bytes": 2 * i,
                                "response_bytes": 4 * i,
                            }
                            storage_socket.serverinfo.save_access(access, session=session)

        yield server.client()


def test_serverinfo_client_query_access(queryable_access_client: PortalClient):

    query_res = queryable_access_client.query_access_log(access_method=["get"])
    assert query_res.current_meta.n_found == 80
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(access_method=["POST"])
    assert query_res.current_meta.n_found == 80
    all_entries = list(query_res)
    assert len(all_entries) == 80

    query_res = queryable_access_client.query_access_log(access_type=["v1/molecules"], access_method="get")
    assert query_res.current_meta.n_found == 20
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
    assert len(query_res.current_batch) < queryable_access_client.api_limits["get_access_logs"]

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
