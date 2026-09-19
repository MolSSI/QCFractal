from __future__ import annotations

import hashlib
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Tuple, List, Any, Optional, Union

from sqlalchemy import select, delete, update
from sqlalchemy.dialects.postgresql import insert

from qcportal.auth import UserInfo
from qcportal.exceptions import AuthenticationFailure, SecurityNotEnabledError
from qcportal.utils import now_at_utc
from .db_models import UserORM, UserSessionORM
from .permission_evaluation import evaluate_global_permissions
from .role_permissions import AuthorizedEnum

if TYPE_CHECKING:
    import datetime
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


def hash_session_key(user_session_key: str) -> str:
    """
    Hashes a browser session key for storage

    The session key is a bearer credential - presenting it is enough to act as the user - so only
    its hash is ever stored. A read of the user_session table (a database dump, a replica, a
    support query, an injection) then yields nothing that can be replayed.

    Unlike a password, the key is 256 bits of output from secrets.token_urlsafe, so guessing it is
    infeasible and no salt or deliberately-slow KDF is needed. A plain SHA-256 keeps the lookup a
    single indexed comparison.
    """

    return hashlib.sha256(user_session_key.encode("UTF-8")).hexdigest()


class AuthSocket:
    """
    Socket for authenticating and authorizing
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self.security_enabled = self.root_socket.qcf_config.enable_security
        self.allow_unauthenticated_read = self.root_socket.qcf_config.allow_unauthenticated_read

        # Browser sessions that have been idle longer than this are expired. Requests already check
        # this when loading a session, but rows for sessions that are never presented again
        # (closed browsers, deleted cookies) would otherwise accumulate forever
        self._user_session_max_age = self.root_socket.qcf_config.api.user_session_max_age

        # Run the cleanup at least hourly, but never more often than once a minute
        self._delete_expired_sessions_frequency = max(60, min(60 * 60, self._user_session_max_age))

        with self.root_socket.session_scope() as session:
            self.root_socket.internal_jobs.add(
                "delete_expired_user_sessions",
                now_at_utc() + timedelta(seconds=5.0),
                "auth.delete_expired_user_sessions",
                {},
                user_id=None,
                unique_name=True,
                repeat_delay=self._delete_expired_sessions_frequency,
                session=session,
            )

    def verify(self, user_id: int, *, session: Optional[Session] = None) -> UserInfo:
        """
        Verifies that a given user id exists and is enabled, returning info about the user and their role

        This does not check the user's password.

        If the user is not found, or is disabled, an exception is raised.

        Parameters
        ----------
        user_id
            The id of the user to check
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        --------
        :
            All information about the user, and all information about the user's role
        """

        stmt = select(UserORM)
        stmt = stmt.where(UserORM.id == user_id)

        with self.root_socket.optional_session(session, True) as session:
            user_orm: Optional[UserORM] = session.execute(stmt).scalar_one_or_none()

            if user_orm is None:
                raise AuthenticationFailure("User does not exist")

            if not user_orm.enabled:
                raise AuthenticationFailure(f"User {user_id} is disabled.")

            user_info = user_orm.to_model(UserInfo)
            return user_info

    def check_global_permission(
        self, role: Optional[str], resource: str, action: str, require_security: bool
    ) -> AuthorizedEnum:
        # Some endpoints require security to be enabled
        if not self.security_enabled:
            if require_security:
                raise SecurityNotEnabledError(f"Cannot access '{resource}' with security disabled")
            else:
                return AuthorizedEnum.Allow

        # Use anonymous role if no role is given
        if role is None:
            role = "anonymous"

        # Don't allow the anonymous role unless the server is set up to allow it
        if role == "anonymous" and not self.allow_unauthenticated_read:
            raise AuthenticationFailure("Server requires login")

        return evaluate_global_permissions(role, resource, action)

    def allowed_actions(self, subject: Any, resources: Any, actions: Any, policies: Any) -> List[Tuple[str, str]]:
        raise NotImplementedError("TODO")

    def create_user_session(
        self,
        user_id: int,
        user_session_key: str,
        user_session_data: Any,
        *,
        session: Optional[Session] = None,
    ) -> None:
        """
        Creates a new user/flask session in the database

        The session key must not already exist.
        """

        with self.root_socket.optional_session(session, False) as session:
            session_orm = UserSessionORM(
                user_id=user_id,
                session_key_hash=hash_session_key(user_session_key),
                session_data=user_session_data,
            )
            session.add(session_orm)

    def update_user_session(
        self,
        user_id: int,
        user_session_key: str,
        user_session_data: Any,
        *,
        session: Optional[Session] = None,
    ) -> bool:
        """
        Updates the data (and last accessed time) of an existing user/flask session

        This never creates a session. If no session with the given key exists that belongs to the
        given user, then False is returned. This happens if the session was revoked (logout,
        administrative action, expiry cleanup) while a request using it was in flight, and such a
        session must not be resurrected.

        Returns
        -------
        :
            True if the session existed and was updated, False otherwise
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = update(UserSessionORM)
            stmt = stmt.where(
                UserSessionORM.session_key_hash == hash_session_key(user_session_key),
                UserSessionORM.user_id == user_id,
            )
            stmt = stmt.values(session_data=user_session_data, last_accessed=now_at_utc())
            r = session.execute(stmt)
            return r.rowcount > 0

    def rotate_user_session(
        self,
        old_session_key: Optional[str],
        user_id: int,
        new_session_key: str,
        user_session_data: Any,
        *,
        session: Optional[Session] = None,
    ) -> None:
        """
        Replaces a user/flask session with a new one under a different key

        The old session (if given) is deleted and the new one created in a single transaction.
        This is used on login so that a session key that existed before authentication is never
        associated with the newly authenticated user (session fixation).
        """

        with self.root_socket.optional_session(session, False) as session:
            if old_session_key is not None:
                stmt = delete(UserSessionORM).where(
                    UserSessionORM.session_key_hash == hash_session_key(old_session_key)
                )
                session.execute(stmt)

            session_orm = UserSessionORM(
                user_id=user_id,
                session_key_hash=hash_session_key(new_session_key),
                session_data=user_session_data,
            )
            session.add(session_orm)

    def load_user_session(
        self, user_session_key: str, *, session: Optional[Session] = None
    ) -> Optional[Tuple[int, Any, datetime.datetime]]:
        """
        Loads user/flask session data from the database

        Returns
        -------
        :
            A tuple of the owning user id (from the relational column, which is authoritative),
            the session data, and the last accessed time. If the session_key does not exist,
            None is returned.
        """
        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserSessionORM).where(UserSessionORM.session_key_hash == hash_session_key(user_session_key))
            flask_session_orm = session.execute(stmt).scalar_one_or_none()

            if not flask_session_orm:
                return None

            return flask_session_orm.user_id, flask_session_orm.session_data, flask_session_orm.last_accessed

    def delete_user_session(
        self,
        user_session_key: Optional[str] = None,
        user_session_public_id: Optional[int] = None,
        *,
        session: Optional[Session] = None,
    ) -> None:
        """
        Deletes user/flask session data from the database (if it exists)
        """

        if (user_session_key is None) and (user_session_public_id is None):
            raise ValueError("Either user_session_key or user_session_public_id (but not both) must be specified")
        if (user_session_key is not None) and (user_session_public_id is not None):
            raise ValueError("Either user_session_key or user_session_public_id (but not both) must be specified")

        with self.root_socket.optional_session(session, False) as session:
            stmt = delete(UserSessionORM)

            if user_session_key is not None:
                stmt = stmt.where(UserSessionORM.session_key_hash == hash_session_key(user_session_key))
            if user_session_public_id is not None:
                stmt = stmt.where(UserSessionORM.public_id == user_session_public_id)

            session.execute(stmt)

    def delete_expired_user_sessions(self, session: Session) -> int:
        """
        Deletes user/flask sessions that have been idle for longer than the configured maximum age

        The predicate is the same one used when a session is loaded for a request
        (last_accessed + max_age < now), and is evaluated in the DELETE itself so that a session
        refreshed by a concurrent request is not removed.

        Returns
        -------
        :
            The number of sessions deleted
        """

        before = now_at_utc() - timedelta(seconds=self._user_session_max_age)
        stmt = delete(UserSessionORM).where(UserSessionORM.last_accessed < before)
        num_deleted = session.execute(stmt).rowcount

        if num_deleted:
            self._logger.info(f"Deleted {num_deleted} expired user sessions (last accessed before {before})")

        return num_deleted

    def list_all_user_sessions(self, *, session: Optional[Session] = None) -> List[Tuple[int, datetime.datetime]]:
        """
        List all sessions currently in the database
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserSessionORM)
            session_orm = session.execute(stmt).scalars().all()
            return [s.public_dict() for s in session_orm]

    def list_user_sessions(
        self, username_or_id: Union[int, str], *, session: Optional[Session] = None
    ) -> List[Tuple[int, datetime.datetime]]:
        """
        List all sessions currently in the database for a single user

        The user may be given by username or id. A username that does not exist raises, rather than
        the raw value reaching the query (where a non-numeric username would be a database error).
        """

        with self.root_socket.optional_session(session, True) as session:
            user_id = self.root_socket.users.get_optional_user_id(username_or_id, session=session)

            stmt = select(UserSessionORM)
            stmt = stmt.where(UserSessionORM.user_id == user_id)
            session_orm = session.execute(stmt).scalars().all()
            return [s.public_dict() for s in session_orm]

    def clear_user_sessions(self, user_id: int, *, session: Optional[Session] = None):
        """
        Clear all sessions for a single user
        """

        with self.root_socket.optional_session(session) as session:
            stmt = delete(UserSessionORM)
            stmt = stmt.where(UserSessionORM.user_id == user_id)
            session.execute(stmt)
