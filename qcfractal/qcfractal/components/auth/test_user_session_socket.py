"""
Tests for the database-side handling of browser (flask) sessions
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from qcfractal.components.auth.db_models import UserSessionORM
from qcfractal.components.internal_jobs.db_models import InternalJobORM
from qcportal.auth import UserInfo
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def _add_user(storage_socket: SQLAlchemySocket, username: str) -> int:
    uinfo = UserInfo(username=username, role="read", enabled=True)
    storage_socket.users.add(uinfo, "a_valid_password_123")
    return storage_socket.users.get(username)["id"]


def test_user_session_socket_create_update_load(storage_socket: SQLAlchemySocket):
    uid = _add_user(storage_socket, "read_user")

    storage_socket.auth.create_user_session(uid, "key1", {"user_id": str(uid), "a": 1})
    loaded = storage_socket.auth.load_user_session("key1")
    assert loaded is not None
    assert loaded[0] == uid
    assert loaded[1] == {"user_id": str(uid), "a": 1}
    first_access = loaded[2]

    # Updating an existing session works and bumps last_accessed
    assert storage_socket.auth.update_user_session(uid, "key1", {"user_id": str(uid), "a": 2}) is True
    loaded = storage_socket.auth.load_user_session("key1")
    assert loaded[1]["a"] == 2
    assert loaded[2] >= first_access

    # Updating never creates, and never touches a session owned by someone else
    assert storage_socket.auth.update_user_session(uid, "missing", {"user_id": str(uid)}) is False
    assert storage_socket.auth.load_user_session("missing") is None
    assert storage_socket.auth.update_user_session(uid + 1, "key1", {"user_id": str(uid + 1)}) is False
    assert storage_socket.auth.load_user_session("key1")[0] == uid

    # Rotation replaces the key in one step
    storage_socket.auth.rotate_user_session("key1", uid, "key2", {"user_id": str(uid)})
    assert storage_socket.auth.load_user_session("key1") is None
    assert storage_socket.auth.load_user_session("key2")[0] == uid

    # Rotation with no previous key is just a create
    storage_socket.auth.rotate_user_session(None, uid, "key3", {"user_id": str(uid)})
    assert storage_socket.auth.load_user_session("key3")[0] == uid


def test_user_session_socket_delete_expired(storage_socket: SQLAlchemySocket):
    uid = _add_user(storage_socket, "read_user")
    max_age = storage_socket.qcf_config.api.user_session_max_age

    storage_socket.auth.create_user_session(uid, "fresh", {"user_id": str(uid)})
    storage_socket.auth.create_user_session(uid, "stale", {"user_id": str(uid)})
    storage_socket.auth.create_user_session(uid, "borderline", {"user_id": str(uid)})

    with storage_socket.session_scope() as session:
        stmt = update(UserSessionORM).where(UserSessionORM.session_key == "stale")
        session.execute(stmt.values(last_accessed=now_at_utc() - timedelta(seconds=max_age + 10)))

        # Not yet expired
        stmt = update(UserSessionORM).where(UserSessionORM.session_key == "borderline")
        session.execute(stmt.values(last_accessed=now_at_utc() - timedelta(seconds=max_age - 30)))

    with storage_socket.session_scope() as session:
        assert storage_socket.auth.delete_expired_user_sessions(session=session) == 1

    assert storage_socket.auth.load_user_session("stale") is None
    assert storage_socket.auth.load_user_session("fresh") is not None
    assert storage_socket.auth.load_user_session("borderline") is not None

    with storage_socket.session_scope() as session:
        assert storage_socket.auth.delete_expired_user_sessions(session=session) == 0


def test_user_session_socket_cleanup_job_registered(storage_socket: SQLAlchemySocket):
    # The cleanup runs as a repeating internal job
    with storage_socket.session_scope() as session:
        stmt = select(InternalJobORM).where(InternalJobORM.name == "delete_expired_user_sessions")
        jobs = session.execute(stmt).scalars().all()
        assert len(jobs) == 1
        assert jobs[0].function == "auth.delete_expired_user_sessions"
        assert jobs[0].repeat_delay == storage_socket.auth._delete_expired_sessions_frequency
        assert 60 <= jobs[0].repeat_delay <= 3600
