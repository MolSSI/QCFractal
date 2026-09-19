"""
Tests for the per-application user verification cache
"""

import time

import pytest
from flask import Flask

from qcfractal.flask_app.user_verify import CachedUserVerifier, FlaskUserVerifier


class _StubAuth:
    def __init__(self, label):
        self.label = label
        self.calls = 0

    def verify(self, user_id: int):
        self.calls += 1
        return f"{self.label}-{user_id}"


class _StubSocket:
    def __init__(self, label):
        self.auth = _StubAuth(label)


def _app(label: str) -> Flask:
    app = Flask(label)
    app.extensions = {"storage_socket": _StubSocket(label)}
    return app


def test_user_verify_caches_repeat_lookups():
    verifier = CachedUserVerifier(_StubSocket("A"))

    assert verifier.verify(1) == "A-1"
    assert verifier.verify(1) == "A-1"
    assert verifier._storage_socket.auth.calls == 1  # second lookup came from the cache

    # A different user is a different key
    assert verifier.verify(2) == "A-2"
    assert verifier._storage_socket.auth.calls == 2

    # Clearing forces a fresh lookup
    verifier.cache_clear()
    assert verifier.verify(1) == "A-1"
    assert verifier._storage_socket.auth.calls == 3


def test_user_verify_expires():
    verifier = CachedUserVerifier(_StubSocket("A"), seconds=1)

    assert verifier.verify(1) == "A-1"
    assert verifier._storage_socket.auth.calls == 1

    time.sleep(1.5)
    assert verifier.verify(1) == "A-1"
    assert verifier._storage_socket.auth.calls == 2


def test_user_verify_caches_are_independent():
    # A user id only means something within one database. Two verifiers must never share entries,
    # or one server's user could be returned for another server's identical id
    a = CachedUserVerifier(_StubSocket("A"))
    b = CachedUserVerifier(_StubSocket("B"))

    assert a.verify(1) == "A-1"
    assert b.verify(1) == "B-1"  # would be "A-1" if the cache were shared
    assert b._storage_socket.auth.calls == 1


def test_user_verify_extension_is_per_app():
    extension = FlaskUserVerifier()
    app_a = _app("A")
    app_b = _app("B")

    extension.init_app(app_a)
    extension.init_app(app_b)

    verifier_a = app_a.extensions["user_verifier"]
    verifier_b = app_b.extensions["user_verifier"]
    assert verifier_a is not verifier_b

    # The same extension object resolves to whichever app is handling the request. A user id
    # only means something within one database, so these must never cross over
    with app_a.app_context():
        assert extension.verify(1) == "A-1"
    with app_b.app_context():
        assert extension.verify(1) == "B-1"

    # Both are now cached
    with app_a.app_context():
        assert extension.verify(1) == "A-1"
    with app_b.app_context():
        assert extension.verify(1) == "B-1"
    assert verifier_a._storage_socket.auth.calls == 1
    assert verifier_b._storage_socket.auth.calls == 1

    # reset_all clears every app's cache, so both look up again
    extension.reset_all()
    with app_a.app_context():
        assert extension.verify(1) == "A-1"
    with app_b.app_context():
        assert extension.verify(1) == "B-1"
    assert verifier_a._storage_socket.auth.calls == 2
    assert verifier_b._storage_socket.auth.calls == 2


def test_user_verify_extension_requires_init():
    extension = FlaskUserVerifier()
    app = Flask("uninitialized")
    app.extensions = {}

    with app.app_context():
        with pytest.raises(RuntimeError, match="not initialized"):
            extension.verify(1)
