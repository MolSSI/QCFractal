"""
Short-lived caching of user verification

Every request re-verifies the authenticated user against the database rather than trusting the
authorization attributes copied into a JWT, so that disabling an account or changing its role or
groups takes effect promptly. A short cache keeps that from being a database round trip on every
single request.

The cache belongs to a single flask application (see FlaskUserVerifier) rather than to the
process. A verification result is keyed by user id, and a user id is only meaningful within one
database, so a process-global cache shared by two apps could hand one server's user to another
server holding the same id - which is exactly the situation throughout the test suite, where many
snowflakes (each with its own database) are built in a single process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

from flask import Flask, current_app

from qcportal.utils import time_based_cache

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.auth import UserInfo


# How long a verification result may be reused. This bounds how long a disabled account or a
# changed role keeps working, so it is deliberately short
VERIFY_CACHE_SECONDS = 5
VERIFY_CACHE_MAXSIZE = 1024


class CachedUserVerifier:
    """
    Verifies users against one storage socket, caching the result briefly

    This class is deliberately free of flask. It holds the socket it verifies against, so a cache
    can never be shared between two databases.
    """

    def __init__(
        self,
        storage_socket: SQLAlchemySocket,
        seconds: int = VERIFY_CACHE_SECONDS,
        maxsize: int = VERIFY_CACHE_MAXSIZE,
    ):
        self._storage_socket = storage_socket

        # The decorator is applied per instance, so every verifier owns its own cache
        self._verify_cached = time_based_cache(seconds=seconds, maxsize=maxsize)(self._verify_uncached)

    def _verify_uncached(self, user_id: int) -> UserInfo:
        return self._storage_socket.auth.verify(user_id=user_id)

    def verify(self, user_id: int) -> UserInfo:
        """
        Returns info about a user, raising if the user does not exist or is disabled
        """

        return self._verify_cached(user_id)

    def cache_clear(self) -> None:
        self._verify_cached.cache_clear()


class FlaskUserVerifier:
    """
    Flask extension owning one CachedUserVerifier per application

    The extension object is a module-level singleton (created in flask_app.py, like jwt and
    app_storage_sockets), but the cache it reads belongs to whichever app is handling the current
    request. A user id only means something within one database, so two apps in one process must
    never share entries.
    """

    _app_verifiers: WeakKeyDictionary[Flask, CachedUserVerifier]

    def __init__(self):
        self._app_verifiers = WeakKeyDictionary()

    def init_app(self, app: Flask) -> None:
        # Must be called after the storage socket has been initialized for this app
        storage_socket = app.extensions["storage_socket"]
        verifier = CachedUserVerifier(storage_socket)

        app.extensions["user_verifier"] = verifier
        self._app_verifiers[app] = verifier

    def _current(self) -> CachedUserVerifier:
        """
        The verifier belonging to the app handling the current request
        """

        try:
            return current_app.extensions["user_verifier"]
        except KeyError:
            raise RuntimeError("User verifier not initialized for this flask app")

    def verify(self, user_id: int) -> UserInfo:
        """
        Returns info about a user, raising if the user does not exist or is disabled
        """

        return self._current().verify(user_id)

    def reset_all(self) -> None:
        """
        Clears every application's cache

        Only intended for testing, where a database may be recreated underneath a long-lived app.
        """

        for verifier in list(self._app_verifiers.values()):
            verifier.cache_clear()


# How long an API token lookup may be reused. Like the user verification cache, this bounds how
# long a revoked or expired token keeps working, so it is deliberately short. Reused here so token
# and user verification behave consistently.
TOKEN_CACHE_SECONDS = VERIFY_CACHE_SECONDS
TOKEN_CACHE_MAXSIZE = 4096


class CachedTokenVerifier:
    """
    Verifies API tokens against one storage socket, caching successful lookups briefly

    An API token is presented on (potentially) every request, so verifying it against the database
    each time would be a per-request round trip - and a second one, on top of user verification.
    This caches the token->user lookup for a short time, mirroring CachedUserVerifier. A revoked or
    expired token therefore keeps working for at most the cache lifetime, consistent with how a
    disabled account or changed role takes effect.

    Only successful lookups are cached. An invalid token raises (and is not cached), so each distinct
    bad token still reaches the database - no worse than before, and it means a token created moments
    ago is never stuck as "invalid".

    Like CachedUserVerifier, this is free of flask and holds the socket it verifies against, so a
    cache is never shared between two databases.
    """

    def __init__(
        self,
        storage_socket: SQLAlchemySocket,
        seconds: int = TOKEN_CACHE_SECONDS,
        maxsize: int = TOKEN_CACHE_MAXSIZE,
    ):
        self._storage_socket = storage_socket
        self._verify_cached = time_based_cache(seconds=seconds, maxsize=maxsize)(self._verify_uncached)

    def _verify_uncached(self, raw_token: str):
        # Returns (user_id, token_id); raises AuthenticationFailure for an invalid token
        return self._storage_socket.auth.verify_api_token(raw_token)

    def verify(self, raw_token: str):
        """
        Returns (user_id, token_id) for a valid token, raising for an invalid one
        """

        return self._verify_cached(raw_token)

    def cache_clear(self) -> None:
        self._verify_cached.cache_clear()


class FlaskTokenVerifier:
    """
    Flask extension owning one CachedTokenVerifier per application (see FlaskUserVerifier)
    """

    _app_verifiers: WeakKeyDictionary[Flask, CachedTokenVerifier]

    def __init__(self):
        self._app_verifiers = WeakKeyDictionary()

    def init_app(self, app: Flask) -> None:
        storage_socket = app.extensions["storage_socket"]
        verifier = CachedTokenVerifier(storage_socket)

        app.extensions["token_verifier"] = verifier
        self._app_verifiers[app] = verifier

    def _current(self) -> CachedTokenVerifier:
        try:
            return current_app.extensions["token_verifier"]
        except KeyError:
            raise RuntimeError("Token verifier not initialized for this flask app")

    def verify(self, raw_token: str):
        """
        Returns (user_id, token_id) for a valid token, raising for an invalid one
        """

        return self._current().verify(raw_token)

    def reset_all(self) -> None:
        """
        Clears every application's cache (only intended for testing)
        """

        for verifier in list(self._app_verifiers.values()):
            verifier.cache_clear()
