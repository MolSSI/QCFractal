from __future__ import annotations

import datetime
import ipaddress
from datetime import datetime
from typing import TYPE_CHECKING

from qcfractaltesting import load_ip_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

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


def test_serverinfo_socket_geoip(storage_socket: SQLAlchemySocket):
    ip_data = load_ip_test_data()

    for ip, lookup_key in test_ips:
        our_data = storage_socket.serverinfo._get_geoip2_data(ip)
        ref_data = ip_data.get(lookup_key, None)

        if lookup_key is None:
            assert our_data.get("country_code") is None
            assert our_data.get("subdivision") is None
            assert our_data.get("city") is None
            assert our_data.get("ip_lat") is None
            assert our_data.get("ip_long") is None
        else:
            assert our_data["country_code"] == ref_data["country"]["iso_code"]

            if our_data.get("subdivision") is not None:
                assert our_data["subdivision"] == ref_data["subdivisions"][-1]["names"]["en"]
            if our_data.get("city") is not None:
                assert our_data["city"] == ref_data["city"]["names"]["en"]
            if our_data.get("ip_lat") is not None:
                assert our_data["ip_lat"] == ref_data["location"]["latitude"]
            if our_data.get("ip_long") is not None:
                assert our_data["ip_long"] == ref_data["location"]["longitude"]


def test_serverinfo_socket_save_query_access(storage_socket: SQLAlchemySocket):
    ip_data = load_ip_test_data()

    access1 = {
        "access_type": "v1/molecule",
        "access_method": "GET",
        "ip_address": test_ips[0][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.24,
        "user": "admin_user",
        "request_bytes": 123,
        "response_bytes": 18273,
    }

    access2 = {
        "access_type": "v1/wavefunction",
        "access_method": "POST",
        "ip_address": test_ips[1][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.45,
        "user": "read_user",
        "request_bytes": 456,
        "response_bytes": 12671,
    }

    access3 = {
        "access_type": "v1/me",
        "access_method": "GET",
        "ip_address": test_ips[2][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.01,
        "user": "read_user",
        "request_bytes": 789,
        "response_bytes": 1975,
    }

    access4 = {
        "access_type": "v1/users",
        "access_method": "PUT",
        "ip_address": test_ips[3][0],
        "user_agent": "Fake user agent",
        "request_duration": 2.18,
        "user": "admin_user",
        "request_bytes": 101112,
        "response_bytes": 10029,
    }

    access5 = {
        "access_type": "v1/keywords",
        "access_method": "POST",
        "ip_address": test_ips[4][0],
        "user_agent": "Fake user agent",
        "request_duration": 3.12,
        "user": "monitor_user",
        "request_bytes": 151617,
        "response_bytes": 2719,
    }

    access6 = {
        "access_type": "v1/information",
        "access_method": "GET",
        "ip_address": test_ips[5][0],
        "user_agent": "Fake user agent",
        "request_duration": 1.28,
        "user": "read_user",
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

    meta, accesses = storage_socket.serverinfo.query_access_log()
    assert meta.success
    assert meta.n_returned == 6
    assert meta.n_found == 6

    # Order should be latest access first
    assert accesses[0]["access_date"] > accesses[1]["access_date"]
    assert accesses[1]["access_date"] > accesses[2]["access_date"]
    assert accesses[2]["access_date"] > accesses[3]["access_date"]
    assert accesses[3]["access_date"] > accesses[4]["access_date"]
    assert accesses[4]["access_date"] > accesses[5]["access_date"]

    # These are ordered descending (newest accesses first). Reverse the order for testing
    for i, (ac_in, ac_db) in enumerate(zip(all_accesses, reversed(accesses))):
        assert ac_in["access_type"] == ac_db["access_type"]
        assert ac_in["access_method"] == ac_db["access_method"]

        # IPV6 can have different representations, so use the ipaddress library for comparison
        assert ipaddress.ip_address(ac_in["ip_address"]) == ipaddress.ip_address(ac_db["ip_address"])

        assert ac_in["user_agent"] == ac_db["user_agent"]
        assert ac_in["request_duration"] == ac_db["request_duration"]
        assert ac_in["user"] == ac_db["user"]
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


def test_serverinfo_socket_query_access(storage_socket: SQLAlchemySocket):
    access1 = {
        "access_type": "v1/molecule",
        "access_method": "GET",
        "ip_address": test_ips[0][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.24,
        "user": "admin_user",
        "request_bytes": 123,
        "response_bytes": 18273,
    }

    access2 = {
        "access_type": "v1/wavefunction",
        "access_method": "POST",
        "ip_address": test_ips[1][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.45,
        "user": "read_user",
        "request_bytes": 456,
        "response_bytes": 12671,
    }

    access3 = {
        "access_type": "v1/me",
        "access_method": "PUT",
        "ip_address": test_ips[2][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.01,
        "user": "read_user",
        "request_bytes": 789,
        "response_bytes": 1975,
    }

    time_0 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access1)
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access2)
    time_23 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access3)

    meta, accesses = storage_socket.serverinfo.query_access_log(before=time_0)
    assert meta.success
    assert meta.n_returned == 0
    assert meta.n_found == 0

    meta, accesses = storage_socket.serverinfo.query_access_log(before=time_12)
    assert meta.success
    assert meta.n_returned == 1
    assert meta.n_found == 1
    assert accesses[0]["access_type"] == "v1/molecule"

    meta, accesses = storage_socket.serverinfo.query_access_log(after=time_12)
    assert meta.success
    assert meta.n_returned == 2
    assert meta.n_found == 2
    assert accesses[0]["access_type"] == "v1/me"
    assert accesses[1]["access_type"] == "v1/wavefunction"

    meta, accesses = storage_socket.serverinfo.query_access_log(access_type=["v1/me"])
    assert meta.success
    assert meta.n_returned == 1
    assert meta.n_found == 1
    assert accesses[0]["access_type"] == "v1/me"

    meta, accesses = storage_socket.serverinfo.query_access_log(access_method=["POST"])
    assert meta.success
    assert meta.n_returned == 1
    assert meta.n_found == 1
    assert accesses[0]["access_type"] == "v1/wavefunction"

    meta, accesses = storage_socket.serverinfo.query_access_log(before=time_23, access_method=["PUT"])
    assert meta.success
    assert meta.n_returned == 0
    assert meta.n_found == 0

    meta, accesses = storage_socket.serverinfo.query_access_log(limit=2)
    assert meta.success
    assert meta.n_returned == 2
    assert meta.n_found == 3
    assert accesses[0]["access_type"] == "v1/me"
    assert accesses[1]["access_type"] == "v1/wavefunction"

    meta, accesses = storage_socket.serverinfo.query_access_log(limit=2, skip=2)
    assert meta.success
    assert meta.n_returned == 1
    assert meta.n_found == 3
    assert accesses[0]["access_type"] == "v1/molecule"

    meta, accesses = storage_socket.serverinfo.query_access_log(before=datetime.utcnow(), limit=1, skip=1)
    assert meta.success
    assert meta.n_returned == 1
    assert meta.n_found == 3
    assert accesses[0]["access_type"] == "v1/wavefunction"

    meta, accesses = storage_socket.serverinfo.query_access_log(after=time_12, before=time_23)
    assert meta.n_returned == 1
    assert accesses[0]["access_type"] == "v1/wavefunction"

    meta, accesses = storage_socket.serverinfo.query_access_log(after=time_23, before=time_12)
    assert meta.n_returned == 0

    meta, accesses = storage_socket.serverinfo.query_access_log(username=["read_user"], before=time_23)
    assert meta.n_found == 1


def test_serverinfo_socket_delete_access(storage_socket: SQLAlchemySocket):
    access1 = {
        "access_type": "v1/molecule",
        "access_method": "GET",
        "ip_address": test_ips[0][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.24,
        "user": "admin_user",
        "request_bytes": 789,
        "response_bytes": 18273,
    }

    access2 = {
        "access_type": "v1/wavefunction",
        "access_method": "POST",
        "ip_address": test_ips[1][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.45,
        "user": "read_user",
        "request_bytes": 876,
        "response_bytes": 12671,
    }

    access3 = {
        "access_type": "v1/me",
        "access_method": "PUT",
        "ip_address": test_ips[2][0],
        "user_agent": "Fake user agent",
        "request_duration": 0.01,
        "user": "read_user",
        "request_bytes": 543,
        "response_bytes": 1975,
    }

    time_0 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access1)
    time_12 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access2)
    time_23 = datetime.utcnow()
    storage_socket.serverinfo.save_access(access3)

    n_deleted = storage_socket.serverinfo.delete_access_logs(before=time_0)
    assert n_deleted == 0

    n_deleted = storage_socket.serverinfo.delete_access_logs(before=time_12)
    assert n_deleted == 1

    meta, accesses = storage_socket.serverinfo.query_access_log()
    assert meta.n_returned == 2
    assert accesses[0]["access_type"] == "v1/me"
    assert accesses[1]["access_type"] == "v1/wavefunction"

    n_deleted = storage_socket.serverinfo.delete_access_logs(before=datetime.utcnow())
    assert n_deleted == 2

    meta, accesses = storage_socket.serverinfo.query_access_log()
    assert meta.n_returned == 0
    assert meta.n_found == 0
