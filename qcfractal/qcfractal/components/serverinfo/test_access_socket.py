from __future__ import annotations

import ipaddress

import pytest

from qcarchivetesting import load_ip_test_data, ip_tests_enabled
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcportal.serverinfo.models import AccessLogQueryFilters
from qcportal.utils import now_at_utc

# First part of the tuple is the ip address
# second is the range, as stored in the MaxMind test JSON file
test_ips = [
    ("81.2.69.142", "::81.2.69.192/124"),
    ("175.16.199.2", "::175.16.199.0/120"),
    ("89.160.20.113", "::89.160.20.112/124"),
    ("2001:02b8:0000:0000:0000:0000:0000:123f", "2001:2b8::/32"),
    ("10.0.0.1", None),
    ("2.125.160.217", "::2.125.160.216/125"),
]


@pytest.mark.skipif(not ip_tests_enabled, reason="Test GeoIP data not found")
def test_serverinfo_socket_save_access(secure_snowflake: QCATestingSnowflake):
    storage_socket = secure_snowflake.get_storage_socket()

    time_0 = now_at_utc()

    ip_data = load_ip_test_data()

    admin_id = storage_socket.users.get("admin_user")["id"]
    read_id = storage_socket.users.get("read_user")["id"]
    monitor_id = storage_socket.users.get("monitor_user")["id"]

    userid_map = {admin_id: "admin_user", read_id: "read_user", monitor_id: "monitor_user"}

    access1 = {
        "module": "api",
        "method": "GET",
        "full_uri": "/api/v1/datasets",
        "ip_address": test_ips[0][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.24,
        "user_id": admin_id,
        "request_bytes": 123,
        "response_bytes": 18273,
    }

    access2 = {
        "module": "api",
        "method": "POST",
        "full_uri": "/api/v1/records",
        "ip_address": test_ips[1][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.45,
        "user_id": read_id,
        "request_bytes": 456,
        "response_bytes": 12671,
    }

    access3 = {
        "module": "api",
        "method": "GET",
        "full_uri": "/api/v1/me",
        "ip_address": test_ips[2][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.01,
        "user_id": read_id,
        "request_bytes": 789,
        "response_bytes": 1975,
    }

    access4 = {
        "module": "api",
        "method": "PUT",
        "full_uri": "/api/v1/users",
        "ip_address": test_ips[3][0],
        "user_agent": "Fake user agent",
        "request_duration": 2.18,
        "user_id": admin_id,
        "request_bytes": 101112,
        "response_bytes": 10029,
    }

    access5 = {
        "module": "compute",
        "full_uri": "/compute/v1/activate",
        "method": "POST",
        "ip_address": test_ips[4][0],
        "user_agent": "Fake user agent",
        "request_duration": 3.12,
        "user_id": monitor_id,
        "request_bytes": 151617,
        "response_bytes": 2719,
    }

    access6 = {
        "module": "auth",
        "full_uri": "/auth/v1/login",
        "method": "POST",
        "ip_address": test_ips[5][0],
        "user_agent": "Fake user agent",
        "request_duration": 1.28,
        "user_id": read_id,
        "request_bytes": 131415,
        "response_bytes": 718723,
    }

    all_accesses = [access1, access2, access3, access4, access5, access6]
    storage_socket.serverinfo.save_access(access1)
    storage_socket.serverinfo.save_access(access2)
    storage_socket.serverinfo.save_access(access3)
    storage_socket.serverinfo.save_access(access4)
    storage_socket.serverinfo.save_access(access5)
    storage_socket.serverinfo.save_access(access6)

    time_1 = now_at_utc()

    # Update the IP addresses with geo data
    with storage_socket.session_scope() as session:
        storage_socket.serverinfo.geolocate_accesses(session)

    accesses = storage_socket.serverinfo.query_access_log(AccessLogQueryFilters())
    assert len(accesses) >= 6

    accesses = storage_socket.serverinfo.query_access_log(AccessLogQueryFilters(before=time_1, after=time_0))
    assert len(accesses) == 6

    # Order should be latest access first
    assert accesses[0]["timestamp"] > accesses[1]["timestamp"]
    assert accesses[1]["timestamp"] > accesses[2]["timestamp"]
    assert accesses[2]["timestamp"] > accesses[3]["timestamp"]
    assert accesses[3]["timestamp"] > accesses[4]["timestamp"]
    assert accesses[4]["timestamp"] > accesses[5]["timestamp"]

    # These are ordered descending (newest accesses first). Reverse the order for testing
    for i, (ac_in, ac_db) in enumerate(zip(all_accesses, reversed(accesses))):
        assert ac_in["module"] == ac_db["module"]
        assert ac_in["method"] == ac_db["method"]
        assert ac_in["full_uri"] == ac_db["full_uri"]

        # IPV6 can have different representations, so use the ipaddress library for comparison
        assert ipaddress.ip_address(ac_in["ip_address"]) == ipaddress.ip_address(ac_db["ip_address"])

        assert ac_in["user_agent"] == ac_db["user_agent"]
        assert ac_in["request_duration"] == ac_db["request_duration"]
        assert userid_map[ac_in["user_id"]] == ac_db["user"]
        assert ac_in["request_bytes"] == ac_db["request_bytes"]
        assert ac_in["response_bytes"] == ac_db["response_bytes"]

        ip_lookup_key = test_ips[i][1]
        ip_ref_data = ip_data.get(ip_lookup_key)

        if ip_ref_data is not None:
            assert ac_db["country_code"] == ip_ref_data["country"]["iso_code"]
            assert ac_db["ip_lat"] == ip_ref_data["location"]["latitude"]
            assert ac_db["ip_long"] == ip_ref_data["location"]["longitude"]

            if ac_db.get("subdivision") is not None:
                assert ac_db["subdivision"] == ip_ref_data["subdivisions"][-1]["names"]["en"]
            if ac_db.get("city") is not None:
                assert ac_db["city"] == ip_ref_data["city"]["names"]["en"]
            if ac_db.get("ip_lat") is not None:
                assert ac_db["ip_lat"] == ip_ref_data["location"]["latitude"]
            if ac_db.get("ip_long") is not None:
                assert ac_db["ip_long"] == ip_ref_data["location"]["longitude"]
