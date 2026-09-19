"""
Rate limiting for login attempts

A hostile client can otherwise try passwords as fast as the server can check them. This limits the
number of failed login attempts within a sliding time window, both per (client address, username)
pair and per client address (to blunt spraying many usernames from one source).

The state belongs to a single flask application (see FlaskLoginRateLimiter), not to the process,
so that several apps in one process - as happens throughout the test suite - never share
failed-login counters. Within one app the state is in-process (a plain dict guarded by a lock), so
a multi-process deployment limits per worker rather than globally. That is a deliberate simplicity
trade-off; it still bounds the guess rate. Successful logins clear the per-user counter but not
the per-address one.
"""

from __future__ import annotations

import math
import threading
import time
from typing import TYPE_CHECKING, Dict, List, Tuple, Union
from weakref import WeakKeyDictionary

from flask import Flask, current_app, request
from werkzeug.exceptions import TooManyRequests

if TYPE_CHECKING:
    from ..config import WebAPIConfig

# Key for the per-(address, username) counter, or just the address for the per-address counter
_Key = Union[str, Tuple[str, str]]


class LoginRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._failures: Dict[_Key, List[float]] = {}

    def _recent(self, key: _Key, window: float, now: float) -> List[float]:
        # Caller must hold the lock. Prunes and returns the timestamps still inside the window
        times = [t for t in self._failures.get(key, ()) if t > now - window]
        if times:
            self._failures[key] = times
        else:
            self._failures.pop(key, None)
        return times

    def check(self, address: str, username: str, config: WebAPIConfig) -> None:
        """
        Raises TooManyRequests if this address/username has failed too many logins recently
        """

        if not config.login_rate_limit_enabled:
            return

        window = config.login_rate_limit_window
        now = time.time()

        with self._lock:
            checks = (
                (self._recent((address, username.lower()), window, now), config.login_rate_limit_max_attempts),
                (self._recent(address, window, now), config.login_rate_limit_ip_max_attempts),
            )

            for times, limit in checks:
                if len(times) >= limit:
                    retry_after = max(1, math.ceil(window - (now - min(times))))
                    err = TooManyRequests("Too many failed login attempts. Please wait before trying again.")
                    err.retry_after = retry_after
                    raise err

    def record_failure(self, address: str, username: str, config: WebAPIConfig) -> None:
        if not config.login_rate_limit_enabled:
            return

        now = time.time()
        with self._lock:
            self._failures.setdefault((address, username.lower()), []).append(now)
            self._failures.setdefault(address, []).append(now)

    def record_success(self, address: str, username: str) -> None:
        # Clear the per-user counter. The per-address counter is left in place so a single valid
        # login does not wipe out evidence of spraying from that address
        with self._lock:
            self._failures.pop((address, username.lower()), None)

    def reset(self) -> None:
        with self._lock:
            self._failures.clear()


class FlaskLoginRateLimiter:
    """
    Flask extension owning one LoginRateLimiter per application

    The extension object is a module-level singleton (created in flask_app.py, like jwt and
    app_storage_sockets), but the counters it acts on belong to whichever app is handling the
    current request. Two apps in one process therefore never share counters - without that,
    failed logins against one server could lock out an unrelated one, which is the normal
    situation in the test suite where many snowflakes are built in a single process.

    The client address and configuration are taken from the current request, so callers only
    supply the username.
    """

    _app_limiters: WeakKeyDictionary[Flask, LoginRateLimiter]

    def __init__(self):
        self._app_limiters = WeakKeyDictionary()

    def init_app(self, app: Flask) -> None:
        limiter = LoginRateLimiter()

        if not hasattr(app, "extensions"):
            app.extensions = {}

        app.extensions["login_rate_limiter"] = limiter
        self._app_limiters[app] = limiter

    def _current(self) -> LoginRateLimiter:
        """
        The limiter belonging to the app handling the current request
        """

        try:
            return current_app.extensions["login_rate_limiter"]
        except KeyError:
            raise RuntimeError("Login rate limiter not initialized for this flask app")

    @staticmethod
    def _config() -> WebAPIConfig:
        return current_app.config["QCFRACTAL_CONFIG"].api

    def check(self, username: str) -> None:
        """
        Raises TooManyRequests if this client and username have failed too many logins recently
        """

        self._current().check(request.remote_addr, username, self._config())

    def record_failure(self, username: str) -> None:
        self._current().record_failure(request.remote_addr, username, self._config())

    def record_success(self, username: str) -> None:
        self._current().record_success(request.remote_addr, username)

    def reset_all(self) -> None:
        """
        Clears the counters of every application's limiter

        Only intended for testing, where a single process runs many apps in sequence and must not
        carry lockouts from one test into the next.
        """

        for limiter in list(self._app_limiters.values()):
            limiter.reset()
