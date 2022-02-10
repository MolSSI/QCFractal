from __future__ import annotations

import ipaddress
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractaltesting import valid_encodings
from .test_access_socket import test_ips
from ...testing_helpers import TestingSnowflake

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal import PortalClient


def test_serverinfo_client_query_access(storage_socket: SQLAlchemySocket, snowflake_client: PortalClient):
    # Add some to test ips and other data
    access1 = {
        "access_type": "v1/molecules",
        "access_method": "GET",
        "ip_address": test_ips[0][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.24,
        "user": "admin_user",
        "request_bytes": 543,
        "response_bytes": 18273,
    }

    access2 = {
        "access_type": "v1/records/optimization",
        "access_method": "POST",
        "ip_address": test_ips[1][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.45,
        "user": "read_user",
        "request_bytes": 210,
        "response_bytes": 12671,
    }

    time_0 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access1)
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access2)

    meta, accesses = snowflake_client.query_access_log()

    # This will return 3, because the query to /information was done in constructing the client
    assert meta.success
    assert meta.n_returned == 3
    assert meta.n_found == 3

    # Order should be latest access first
    assert accesses[0]["access_date"] > accesses[1]["access_date"]
    assert accesses[1]["access_date"] > accesses[2]["access_date"]

    # These are ordered descending (newest accesses first). Reverse the order for testing
    assert accesses[0]["access_type"] == "v1/records/optimization"
    assert accesses[1]["access_type"] == "v1/molecules"
    assert accesses[2]["access_type"] == "v1/information"  # from constructing the client

    assert accesses[0]["access_method"] == "POST"
    assert accesses[1]["access_method"] == "GET"
    assert accesses[2]["access_method"] == "GET"

    assert accesses[0]["ip_address"] == test_ips[1][0]
    assert accesses[1]["ip_address"] == test_ips[0][0]
    assert ipaddress.ip_address(accesses[2]["ip_address"]).is_loopback

    meta, accesses = snowflake_client.query_access_log("v1/records/optimization", "GET")
    assert meta.n_found == 0

    meta, accesses = snowflake_client.query_access_log("v1/records/optimization")
    assert meta.n_found == 1

    meta, accesses = snowflake_client.query_access_log(access_method=["POST", "GET"], before=time_12)
    assert meta.n_found == 2

    meta, accesses = snowflake_client.query_access_log(access_method=["POST"], after=time_12)
    assert meta.n_found == 5  # includes previous queries


def test_serverinfo_client_access_logged(snowflake_client: PortalClient):
    snowflake_client.query_access_log()
    snowflake_client.query_molecules(molecular_formula=["C"])

    snowflake_client.get_molecules([123], missing_ok=True)

    # This will return 4, because the query to /information was done in constructing the client
    meta, accesses = snowflake_client.query_access_log()
    assert meta.success
    assert meta.n_returned == 4
    assert meta.n_found == 4

    assert accesses[3]["access_type"] == "v1/information"
    assert accesses[2]["access_type"] == "v1/access_logs/query"
    assert accesses[1]["access_type"] == "v1/molecules/query"
    assert accesses[0]["access_type"] == "v1/molecules/bulkGet"

    assert accesses[0]["response_bytes"] > 0
    assert accesses[1]["response_bytes"] > 0
    assert accesses[2]["response_bytes"] > 0
    assert accesses[3]["response_bytes"] > 0
    assert accesses[0]["request_bytes"] > 0
    assert accesses[1]["request_bytes"] > 0
    assert accesses[2]["request_bytes"] > 0
    assert accesses[3]["request_bytes"] == 0


@pytest.mark.parametrize("encoding", valid_encodings)
def test_serverinfo_client_access_not_logged(temporary_database, encoding: str):

    db_config = temporary_database.config
    with TestingSnowflake(db_config, encoding=encoding, log_access=False) as server:
        client = server.client()
        client.query_access_log()
        client.query_molecules(molecular_formula=["C"])

        client.get_molecules([])

        # This will return 0 because logging is disabled
        meta, accesses = client.query_access_log()
        assert meta.success
        assert meta.n_returned == 0
        assert meta.n_found == 0
        assert len(accesses) == 0


def test_serverinfo_client_access_delete(snowflake_client: PortalClient):

    time_0 = datetime.utcnow()
    snowflake_client.query_access_log()
    time_12 = datetime.utcnow()
    snowflake_client.query_molecules(molecular_formula=["C"])
    time_23 = datetime.utcnow()
    snowflake_client.get_molecules([123], missing_ok=True)
    time_4 = datetime.utcnow()

    # This will return 4, because the query to /information was done in constructing the client
    meta, accesses = snowflake_client.query_access_log()
    assert meta.success
    assert meta.n_returned == 4

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
