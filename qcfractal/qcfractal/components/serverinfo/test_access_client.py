from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import valid_encodings, test_users
from ...testing_helpers import QCATestingSnowflake

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_serverinfo_client_access_logged(secure_snowflake_allow_read: QCATestingSnowflake):

    client = secure_snowflake_allow_read.client("admin_user", test_users["admin_user"]["pw"])
    read_client = secure_snowflake_allow_read.client()

    client.query_access_log()
    client.query_molecules(molecular_formula=["C"])

    read_client.get_molecules([123], missing_ok=True)

    # This will return 6, because the requests to /login and /information was done in constructing the clients
    query_res = client.query_access_log()
    assert query_res._current_meta.success
    assert query_res._current_meta.n_found == 6
    accesses = list(query_res)

    assert accesses[5].access_type == "/auth/v1/login"
    assert accesses[5].full_uri == "/auth/v1/login"
    assert accesses[5].user is None

    assert accesses[4].access_type == "/api/v1/information"
    assert accesses[4].full_uri == "/api/v1/information"
    assert accesses[4].user == "admin_user"

    assert accesses[3].access_type == "/api/v1/information"
    assert accesses[3].full_uri == "/api/v1/information"
    assert accesses[3].user is None

    assert accesses[2].access_type == "/api/v1/access_logs"
    assert accesses[2].full_uri == "/api/v1/access_logs/query"
    assert accesses[2].user == "admin_user"

    assert accesses[1].access_type == "/api/v1/molecules"
    assert accesses[1].full_uri == "/api/v1/molecules/query"
    assert accesses[1].user == "admin_user"

    assert accesses[0].access_type == "/api/v1/molecules"
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


@pytest.mark.parametrize("encoding", valid_encodings)
def test_serverinfo_client_access_not_logged(temporary_database, encoding: str):

    db_config = temporary_database.config
    with QCATestingSnowflake(db_config, encoding=encoding, log_access=False) as server:
        client = server.client()
        client.query_access_log()
        client.query_molecules(molecular_formula=["C"])

        client.get_molecules([])

        # This will return 0 because logging is disabled
        query_res = client.query_access_log()
        assert query_res._current_meta.success
        assert query_res._current_meta.n_found == 0
        assert len(list(query_res)) == 0


def test_serverinfo_client_access_delete(snowflake_client: PortalClient):

    time_0 = datetime.utcnow()
    snowflake_client.query_access_log()
    time_12 = datetime.utcnow()
    snowflake_client.query_molecules(molecular_formula=["C"])
    time_23 = datetime.utcnow()
    snowflake_client.get_molecules([123], missing_ok=True)
    time_4 = datetime.utcnow()

    # This will return 4, because the query to /information was done in constructing the client
    query_res = snowflake_client.query_access_log()
    assert query_res._current_meta.n_found == 4

    n_deleted = snowflake_client.delete_access_log(time_0)
    assert n_deleted == 1  # deleted our original /information query

    n_deleted = snowflake_client.delete_access_log(time_12)
    assert n_deleted == 1

    n_deleted = snowflake_client.delete_access_log(time_12)
    assert n_deleted == 0

    n_deleted = snowflake_client.delete_access_log(time_4)
    assert n_deleted == 2

    # All of the above generated accesses!
    n_deleted = snowflake_client.delete_access_log(datetime.utcnow())
    assert n_deleted == 5
