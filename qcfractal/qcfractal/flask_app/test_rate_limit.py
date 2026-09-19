"""
Unit tests for the login rate limiter's bookkeeping

The end-to-end behaviour (HTTP 429, Retry-After, per-app isolation) is covered in
auth_v1/test_auth_rate_limit.py. These tests cover the in-memory accounting, which must stay
bounded no matter what an unauthenticated caller submits.
"""

import time
from types import SimpleNamespace

import pytest
from werkzeug.exceptions import TooManyRequests

from qcfractal.flask_app.rate_limit import LoginRateLimiter


def _config(window=1, max_attempts=3, ip_max_attempts=1_000_000):
    return SimpleNamespace(
        login_rate_limit_enabled=True,
        login_rate_limit_window=window,
        login_rate_limit_max_attempts=max_attempts,
        login_rate_limit_ip_max_attempts=ip_max_attempts,
    )


def test_rate_limiter_sweeps_expired_entries():
    # Counters are pruned when their own key is looked up again. A caller that never reuses a
    # username would otherwise leave an entry behind for the lifetime of the process
    limiter = LoginRateLimiter()
    config = _config(window=1)

    for i in range(200):
        limiter.record_failure("10.0.0.1", f"nosuchuser{i}", config)

    assert len(limiter._failures) > 200

    time.sleep(1.1)

    # A single unrelated request is enough to clear everything that has aged out
    limiter.check("10.0.0.1", "someone_else", config)
    assert limiter._failures == {}


def test_rate_limiter_truncates_long_usernames():
    limiter = LoginRateLimiter()
    config = _config()

    limiter.record_failure("10.0.0.1", "x" * 100_000, config)

    user_keys = [k for k in limiter._failures if isinstance(k, tuple)]
    assert len(user_keys) == 1
    assert len(user_keys[0][1]) == limiter.max_key_username_length


def test_rate_limiter_caps_tracked_keys():
    limiter = LoginRateLimiter()
    limiter.max_tracked_keys = 50
    config = _config(window=60)

    for i in range(500):
        limiter.record_failure("10.0.0.1", f"nosuchuser{i}", config)

    # The cap is applied on sweep, which happens at most once per window
    limiter._sweep(config.login_rate_limit_window, time.time())
    assert len(limiter._failures) == 50


def test_rate_limiter_still_limits_after_truncation():
    # Truncating the key must not lose track of a real (short) username
    limiter = LoginRateLimiter()
    config = _config(window=60, max_attempts=3)

    for _ in range(3):
        limiter.record_failure("10.0.0.1", "admin_user", config)

    with pytest.raises(TooManyRequests):
        limiter.check("10.0.0.1", "admin_user", config)

    # A different username from the same address is unaffected by the per-user counter
    limiter.check("10.0.0.1", "other_user", config)

    # Case is still normalized
    with pytest.raises(TooManyRequests):
        limiter.check("10.0.0.1", "ADMIN_USER", config)


def test_rate_limiter_success_clears_truncated_key():
    limiter = LoginRateLimiter()
    config = _config(window=60, max_attempts=3)
    long_name = "y" * 100_000

    for _ in range(3):
        limiter.record_failure("10.0.0.1", long_name, config)
    with pytest.raises(TooManyRequests):
        limiter.check("10.0.0.1", long_name, config)

    limiter.record_success("10.0.0.1", long_name)
    limiter.check("10.0.0.1", long_name, config)
