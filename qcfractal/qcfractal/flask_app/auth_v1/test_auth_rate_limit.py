"""
Tests for login rate limiting
"""

import time

import pytest
import requests

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.flask_app.csrf import CSRF_HEADER


@pytest.fixture
def rate_limited_snowflake(postgres_server, client_encoding, request):
    # Unique database per test, since this fixture is function-scoped and shared by several tests
    pg_harness = postgres_server.get_new_harness(f"login_rate_limit_{request.node.name}")
    with QCATestingSnowflake(
        pg_harness,
        encoding=client_encoding,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
        extra_config={
            "api": {
                "login_rate_limit_max_attempts": 3,
                "login_rate_limit_ip_max_attempts": 5,
                "login_rate_limit_window": 2,
            }
        },
    ) as snowflake:
        yield snowflake


def _jwt_login(uri, username, password):
    return requests.post(f"{uri}/auth/v1/login", json={"username": username, "password": password})


def _session_login(uri, username, password):
    headers = {CSRF_HEADER: "XMLHttpRequest"}
    return requests.post(
        f"{uri}/auth/v1/session_login", json={"username": username, "password": password}, headers=headers
    )


def test_login_rate_limit_blocks_after_max(rate_limited_snowflake):
    uri = rate_limited_snowflake.get_uri()
    good = test_users["admin_user"]["pw"]

    # Three failures are allowed; the fourth attempt is blocked (even with the correct password)
    for _ in range(3):
        r = _jwt_login(uri, "admin_user", "wrong password")
        assert r.status_code == 401

    r = _jwt_login(uri, "admin_user", "wrong password")
    assert r.status_code == 429
    assert "Retry-After" in r.headers

    # Correct password is refused while blocked
    r = _jwt_login(uri, "admin_user", good)
    assert r.status_code == 429


def test_login_rate_limit_success_resets(rate_limited_snowflake):
    uri = rate_limited_snowflake.get_uri()
    good = test_users["admin_user"]["pw"]

    for _ in range(2):
        assert _jwt_login(uri, "admin_user", "wrong password").status_code == 401

    # A success before the limit clears the counter, so a later run of failures starts fresh
    assert _jwt_login(uri, "admin_user", good).status_code == 200

    for _ in range(3):
        assert _jwt_login(uri, "admin_user", "wrong password").status_code == 401
    assert _jwt_login(uri, "admin_user", "wrong password").status_code == 429


def test_login_rate_limit_window_expires(rate_limited_snowflake):
    uri = rate_limited_snowflake.get_uri()
    good = test_users["admin_user"]["pw"]

    for _ in range(3):
        assert _jwt_login(uri, "admin_user", "wrong password").status_code == 401
    assert _jwt_login(uri, "admin_user", "wrong password").status_code == 429

    # After the window passes, attempts are allowed again. Sleep well past the window
    # (which is 2s in this fixture) so the test is not sensitive to load-dependent timing
    window = rate_limited_snowflake._qcf_config.api.login_rate_limit_window
    time.sleep(window + 3)
    assert _jwt_login(uri, "admin_user", good).status_code == 200


def test_login_rate_limit_per_ip_across_usernames(rate_limited_snowflake):
    # Spraying: one attempt each against several usernames from one address trips the per-address cap
    uri = rate_limited_snowflake.get_uri()

    for i in range(5):
        r = _jwt_login(uri, f"nosuchuser{i}", "wrong password")
        assert r.status_code == 401

    # The per-address cap (5) is now reached; a fresh username is blocked despite its own counter being empty
    r = _jwt_login(uri, "another_new_user", "wrong password")
    assert r.status_code == 429


def test_login_rate_limit_applies_to_session_login(rate_limited_snowflake):
    uri = rate_limited_snowflake.get_uri()

    for _ in range(3):
        assert _session_login(uri, "admin_user", "wrong password").status_code == 401
    assert _session_login(uri, "admin_user", "wrong password").status_code == 429


def test_login_rate_limit_can_be_disabled(postgres_server, client_encoding):
    pg_harness = postgres_server.get_new_harness("login_rate_limit_disabled")
    with QCATestingSnowflake(
        pg_harness,
        encoding=client_encoding,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
        extra_config={"api": {"login_rate_limit_enabled": False, "login_rate_limit_max_attempts": 3}},
    ) as snowflake:
        uri = snowflake.get_uri()
        for _ in range(6):
            assert _jwt_login(uri, "admin_user", "wrong password").status_code == 401


def test_login_rate_limit_is_per_app(postgres_server, client_encoding):
    # Rate limit state belongs to a single flask app. Two servers in one process (which happens
    # throughout the test suite) must not share failed-login counters, or failures against one
    # would lock out an unrelated one.
    good = test_users["admin_user"]["pw"]

    def make(name):
        return QCATestingSnowflake(
            postgres_server.get_new_harness(name),
            encoding=client_encoding,
            create_users=True,
            enable_security=True,
            allow_unauthenticated_read=False,
            # A long window, so any leak would still be in effect when the second server starts
            extra_config={"api": {"login_rate_limit_max_attempts": 3, "login_rate_limit_window": 300}},
        )

    with make("rate_limit_per_app_a") as server_a:
        uri_a = server_a.get_uri()
        for _ in range(3):
            assert _jwt_login(uri_a, "admin_user", "wrong password").status_code == 401
        assert _jwt_login(uri_a, "admin_user", good).status_code == 429

    # A separate app and database, with no failed logins of its own
    with make("rate_limit_per_app_b") as server_b:
        assert _jwt_login(server_b.get_uri(), "admin_user", good).status_code == 200
