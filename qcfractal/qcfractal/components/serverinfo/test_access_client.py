from __future__ import annotations

from typing import TYPE_CHECKING

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_serverinfo_client_access_logged(secure_snowflake_allow_read: QCATestingSnowflake):
    time_0 = now_at_utc()
    client = secure_snowflake_allow_read.client("admin_user", test_users["admin_user"]["pw"])
    read_client = secure_snowflake_allow_read.client()

    client.query_access_log()
    client.query_molecules(molecular_formula=["C"])

    read_client.get_molecules([123], missing_ok=True)
    time_0 = now_at_utc()

    # This will return 6, because the requests to /login and /information was done in constructing the clients
    query_res = client.query_access_log()

    # creating the client can add pings
    accesses = list(query_res)
    assert len(accesses) >= 6

    assert accesses[5].module == "auth"
    assert accesses[5].full_uri == "/auth/v1/login"
    assert accesses[5].user == "admin_user"

    assert accesses[4].module == "api"
    assert accesses[4].full_uri == "/api/v1/information"
    assert accesses[4].user == "admin_user"

    assert accesses[3].module == "api"
    assert accesses[3].full_uri == "/api/v1/information"
    assert accesses[3].user is None

    assert accesses[2].module == "api"
    assert accesses[2].full_uri == "/api/v1/access_logs/query"
    assert accesses[2].user == "admin_user"

    assert accesses[1].module == "api"
    assert accesses[1].full_uri == "/api/v1/molecules/query"
    assert accesses[1].user == "admin_user"

    assert accesses[0].module == "api"
    assert accesses[0].full_uri == "/api/v1/molecules/bulkGet"
    assert accesses[0].user is None

    assert accesses[0].response_bytes > 0
    assert accesses[1].response_bytes > 0
    assert accesses[2].response_bytes > 0
    assert accesses[3].response_bytes > 0
    assert accesses[0].request_bytes > 0
    assert accesses[1].request_bytes > 0
    assert accesses[2].request_bytes > 0
    assert accesses[3].request_bytes == 0


def test_serverinfo_client_access_not_logged(postgres_server, pytestconfig):
    pg_harness = postgres_server.get_new_harness("serverinfo_client_access_not_logged")
    encoding = pytestconfig.getoption("--client-encoding")

    with QCATestingSnowflake(pg_harness, encoding, log_access=False) as server:
        client = server.client()
        client.query_access_log()
        client.query_molecules(molecular_formula=["C"])

        client.get_molecules([])

        # This will return 0 because logging is disabled
        query_res = client.query_access_log()
        assert len(list(query_res)) == 0


def test_serverinfo_client_access_delete(snowflake_client: PortalClient):
    time_0 = now_at_utc()
    snowflake_client.query_access_log()
    time_12 = now_at_utc()
    snowflake_client.query_molecules(molecular_formula=["C"])
    time_23 = now_at_utc()
    snowflake_client.get_molecules([123], missing_ok=True)
    time_4 = now_at_utc()

    query_res = snowflake_client.query_access_log(after=time_0)
    query_res_l = list(query_res)
    assert len(query_res_l) == 3

    # Delete anything related to constructing the client
    snowflake_client.delete_access_log(time_0)

    n_deleted = snowflake_client.delete_access_log(time_12)
    assert n_deleted == 1

    n_deleted = snowflake_client.delete_access_log(time_12)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_access_log(time_4)
    assert n_deleted == 2

    # All of the above generated accesses!
    n_deleted = snowflake_client.delete_access_log(now_at_utc())
    assert n_deleted == 7
