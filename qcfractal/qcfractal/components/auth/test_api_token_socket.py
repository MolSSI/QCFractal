from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from qcfractal.components.auth.auth_socket import hash_api_token, _MAX_API_TOKENS_PER_USER
from qcfractal.components.auth.db_models import UserAPITokenORM
from qcportal.auth import API_TOKEN_PREFIX, MAX_API_TOKEN_DESCRIPTION_LENGTH
from qcportal.auth.models import UserInfo
from qcportal.exceptions import AuthenticationFailure, UserManagementError
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def _add_user(storage_socket: SQLAlchemySocket, username: str = "test_user", role: str = "read") -> int:
    uinfo = UserInfo(username=username, role=role, enabled=True)
    storage_socket.users.add(uinfo, password="a_password_123")
    return storage_socket.users.get(username)["id"]


def test_api_token_socket_create_verify(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)

    raw, info = storage_socket.auth.create_api_token(user_id, description="laptop")
    assert raw.startswith(API_TOKEN_PREFIX)
    assert info["user_id"] == user_id
    assert info["description"] == "laptop"
    assert info["expires_at"] is None
    assert info["last_used_at"] is None
    # The stored prefix really is a prefix of the plaintext token
    assert raw.startswith(info["token_prefix"])

    got_user_id, token_id = storage_socket.auth.verify_api_token(raw)
    assert got_user_id == user_id
    assert token_id == info["id"]


def test_api_token_socket_no_secret_in_listing(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    raw, info = storage_socket.auth.create_api_token(user_id)

    listed = storage_socket.auth.list_api_tokens(user_id)
    assert len(listed) == 1
    row = listed[0]
    assert "token_hash" not in row
    assert raw not in row.values()
    assert row["token_prefix"] == info["token_prefix"]

    # And list_all
    all_listed = storage_socket.auth.list_all_api_tokens()
    assert len(all_listed) == 1
    assert "token_hash" not in all_listed[0]


def test_api_token_socket_plaintext_not_stored(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    raw, info = storage_socket.auth.create_api_token(user_id)

    with storage_socket.session_scope(True) as session:
        orm = session.execute(select(UserAPITokenORM).where(UserAPITokenORM.id == info["id"])).scalar_one()
        # Only the hash is stored, and it is the hash of the full presented token
        assert orm.token_hash == hash_api_token(raw)
        assert raw not in orm.token_hash


def test_api_token_socket_uniqueness(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    tokens = {storage_socket.auth.create_api_token(user_id)[0] for _ in range(25)}
    assert len(tokens) == 25


@pytest.mark.parametrize(
    "bad_token",
    [
        "",
        "qcf_",
        "qcf_notarealtoken",
        "notaprefix",
        "Bearer qcf_x",
        API_TOKEN_PREFIX + "x" * 10000,  # too long
        "qcf_abc\x00def",  # NUL
        "qcf_abc def",  # space
    ],
)
def test_api_token_socket_verify_bad(storage_socket: SQLAlchemySocket, bad_token):
    # Every invalid token raises with a single, uniform message
    with pytest.raises(AuthenticationFailure) as exc:
        storage_socket.auth.verify_api_token(bad_token)
    assert str(exc.value) == "API token is not valid"


def test_api_token_socket_verify_flipped(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    raw, _ = storage_socket.auth.create_api_token(user_id)

    # A valid token with one character changed must not verify
    flipped = raw[:-1] + ("A" if raw[-1] != "A" else "B")
    with pytest.raises(AuthenticationFailure, match="API token is not valid"):
        storage_socket.auth.verify_api_token(flipped)


def test_api_token_socket_uniform_message_is_not_token_expired(storage_socket: SQLAlchemySocket):
    # The message must not contain "Token has expired" - the qcportal client matches that substring
    # to trigger a JWT refresh, which a token client has no way to do
    with pytest.raises(AuthenticationFailure) as exc:
        storage_socket.auth.verify_api_token("qcf_nope")
    assert "Token has expired" not in str(exc.value)


def test_api_token_socket_revoked(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    raw, info = storage_socket.auth.create_api_token(user_id)

    # Works before deletion
    storage_socket.auth.verify_api_token(raw)

    storage_socket.auth.delete_api_token(info["id"], user_id)

    with pytest.raises(AuthenticationFailure, match="API token is not valid"):
        storage_socket.auth.verify_api_token(raw)


def test_api_token_socket_expired(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    raw, info = storage_socket.auth.create_api_token(user_id)

    # Backdate the expiration directly
    with storage_socket.session_scope() as session:
        orm = session.execute(select(UserAPITokenORM).where(UserAPITokenORM.id == info["id"])).scalar_one()
        orm.expires_at = now_at_utc() - datetime.timedelta(seconds=1)

    with pytest.raises(AuthenticationFailure, match="API token is not valid"):
        storage_socket.auth.verify_api_token(raw)


def test_api_token_socket_delete_wrong_user(storage_socket: SQLAlchemySocket):
    user_a = _add_user(storage_socket, "user_a")
    user_b = _add_user(storage_socket, "user_b")

    raw, info = storage_socket.auth.create_api_token(user_a)

    # user_b cannot delete user_a's token
    with pytest.raises(UserManagementError, match="not found"):
        storage_socket.auth.delete_api_token(info["id"], user_b)

    # ... and it still works
    got_user_id, _ = storage_socket.auth.verify_api_token(raw)
    assert got_user_id == user_a


def test_api_token_socket_delete_nonexistent(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    with pytest.raises(UserManagementError, match="not found"):
        storage_socket.auth.delete_api_token(999999, user_id)


def test_api_token_socket_user_delete_cascade(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    storage_socket.auth.create_api_token(user_id)
    assert len(storage_socket.auth.list_api_tokens(user_id)) == 1

    storage_socket.users.delete(user_id)

    with storage_socket.session_scope(True) as session:
        remaining = session.execute(select(UserAPITokenORM).where(UserAPITokenORM.user_id == user_id)).scalars().all()
        assert remaining == []


def test_api_token_socket_description_too_long(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    with pytest.raises(UserManagementError, match="description"):
        storage_socket.auth.create_api_token(user_id, description="x" * (MAX_API_TOKEN_DESCRIPTION_LENGTH + 1))


def test_api_token_socket_count_limit(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    for _ in range(_MAX_API_TOKENS_PER_USER):
        storage_socket.auth.create_api_token(user_id)

    with pytest.raises(UserManagementError, match="maximum number"):
        storage_socket.auth.create_api_token(user_id)


def test_api_token_socket_last_used_throttled(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    raw, info = storage_socket.auth.create_api_token(user_id)

    storage_socket.auth.verify_api_token(raw)
    first = storage_socket.auth.list_api_tokens(user_id)[0]["last_used_at"]
    assert first is not None

    # An immediate second use does not move last_used_at (throttled)
    storage_socket.auth.verify_api_token(raw)
    second = storage_socket.auth.list_api_tokens(user_id)[0]["last_used_at"]
    assert second == first

    # Backdate last_used_at beyond the throttle window; the next use updates it
    with storage_socket.session_scope() as session:
        orm = session.execute(select(UserAPITokenORM).where(UserAPITokenORM.id == info["id"])).scalar_one()
        orm.last_used_at = now_at_utc() - datetime.timedelta(hours=1)

    storage_socket.auth.verify_api_token(raw)
    third = storage_socket.auth.list_api_tokens(user_id)[0]["last_used_at"]
    assert third > first


def test_api_token_socket_create_nonexistent_user(storage_socket: SQLAlchemySocket):
    with pytest.raises(UserManagementError, match="does not exist"):
        storage_socket.auth.create_api_token(999999)


def _with_lifetimes(storage_socket, default, maximum):
    """Context-managerish helper: temporarily set the auth socket's token lifetime policy"""

    auth = storage_socket.auth
    saved = (auth._api_token_default_lifetime, auth._api_token_max_lifetime)
    auth._api_token_default_lifetime = default
    auth._api_token_max_lifetime = maximum
    return saved


def _restore_lifetimes(storage_socket, saved):
    storage_socket.auth._api_token_default_lifetime, storage_socket.auth._api_token_max_lifetime = saved


def test_api_token_socket_default_lifetime(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    saved = _with_lifetimes(storage_socket, default=3600, maximum=None)
    try:
        before = now_at_utc()
        _, info = storage_socket.auth.create_api_token(user_id)
        assert info["expires_at"] is not None
        # Roughly now + 1 hour
        delta = info["expires_at"] - before
        assert datetime.timedelta(minutes=59) < delta < datetime.timedelta(minutes=61)
    finally:
        _restore_lifetimes(storage_socket, saved)


def test_api_token_socket_max_lifetime_blocks_unlimited(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    # A finite maximum with no default: an omitted expiration would be unlimited, which must be
    # rejected rather than silently made unlimited (the v1 bypass)
    saved = _with_lifetimes(storage_socket, default=None, maximum=86400)
    try:
        with pytest.raises(UserManagementError, match="api_token_max_lifetime"):
            storage_socket.auth.create_api_token(user_id)
    finally:
        _restore_lifetimes(storage_socket, saved)


def test_api_token_socket_max_lifetime_blocks_too_far(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    saved = _with_lifetimes(storage_socket, default=None, maximum=86400)
    try:
        too_far = now_at_utc() + datetime.timedelta(days=30)
        with pytest.raises(UserManagementError, match="api_token_max_lifetime"):
            storage_socket.auth.create_api_token(user_id, expires_at=too_far)
    finally:
        _restore_lifetimes(storage_socket, saved)


def test_api_token_socket_within_max_lifetime_ok(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    saved = _with_lifetimes(storage_socket, default=None, maximum=86400)
    try:
        ok = now_at_utc() + datetime.timedelta(hours=1)
        _, info = storage_socket.auth.create_api_token(user_id, expires_at=ok)
        assert info["expires_at"] is not None
    finally:
        _restore_lifetimes(storage_socket, saved)


def test_api_token_socket_naive_expiry_rejected(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    naive = datetime.datetime(2030, 1, 1, 0, 0, 0)  # no tzinfo
    with pytest.raises(UserManagementError, match="timezone-aware"):
        storage_socket.auth.create_api_token(user_id, expires_at=naive)


def test_api_token_socket_past_expiry_rejected(storage_socket: SQLAlchemySocket):
    user_id = _add_user(storage_socket)
    past = now_at_utc() - datetime.timedelta(seconds=1)
    with pytest.raises(UserManagementError, match="in the future"):
        storage_socket.auth.create_api_token(user_id, expires_at=past)
