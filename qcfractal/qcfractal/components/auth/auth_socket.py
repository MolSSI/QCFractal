from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Tuple, List, Any, Optional

from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert

from qcportal.auth import UserInfo
from qcportal.exceptions import AuthenticationFailure, AuthorizationFailure
from qcportal.utils import now_at_utc
from .db_models import UserORM, UserSessionORM
from .permission_evaluation import evaluate_global_permissions

if TYPE_CHECKING:
    import datetime
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


class AuthSocket:
    """
    Socket for authenticating and authorizing
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self.security_enabled = self.root_socket.qcf_config.enable_security
        self.allow_unauthenticated_read = self.root_socket.qcf_config.allow_unauthenticated_read

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

    def assert_global_permission(self, role: Optional[str], resource: str, action: str, require_security: bool):
        # Some endpoints require security to be enabled
        if not self.security_enabled:
            if require_security:
                raise AuthorizationFailure(f"Cannot access '{resource}' with security disabled")
            else:
                return

        # Use anonymous role if no role is given
        if role is None:
            role = "anonymous"

        # Don't allow the anonymous role unless the server is set up to allow it
        if role == "anonymous" and not self.allow_unauthenticated_read:
            raise AuthenticationFailure("Server requires login")

        allowed = evaluate_global_permissions(role, resource, action)

        if not allowed:
            raise AuthorizationFailure(
                f"Role '{role}' is not authorized to use action '{action}' on resource '{resource}'"
            )

    def allowed_actions(self, subject: Any, resources: Any, actions: Any, policies: Any) -> List[Tuple[str, str]]:
        raise NotImplementedError("TODO")

    def save_user_session(
        self,
        user_id: int,
        user_session_key: str,
        user_session_data: Any,
        *,
        session: Optional[Session] = None,
    ) -> None:
        """
        Saves user/flask session data to the database
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = insert(UserSessionORM)
            stmt = stmt.values(user_id=user_id, session_key=user_session_key, session_data=user_session_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=[UserSessionORM.session_key],
                set_={"session_data": user_session_data, "last_accessed": now_at_utc()},
            )
            session.execute(stmt)

    def load_user_session(
        self, user_session_key: str, *, session: Optional[Session] = None
    ) -> Tuple[Any, datetime.datetime]:
        """
        Loads user/flask session data from the database

        Will return None if the session_key does not exist in the database

        Returns a tuple of the session data and the last accessed time
        """
        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserSessionORM).where(UserSessionORM.session_key == user_session_key)
            flask_session_orm = session.execute(stmt).scalar_one_or_none()

            if not flask_session_orm:
                return None, now_at_utc()

            return flask_session_orm.session_data, flask_session_orm.last_accessed

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
                stmt = stmt.where(UserSessionORM.session_key == user_session_key)
            if user_session_public_id is not None:
                stmt = stmt.where(UserSessionORM.public_id == user_session_public_id)

            session.execute(stmt)

    def list_all_user_sessions(self, *, session: Optional[Session] = None) -> List[Tuple[int, datetime.datetime]]:
        """
        List all sessions currently in the database
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserSessionORM)
            session_orm = session.execute(stmt).scalars().all()
            return [s.public_dict() for s in session_orm]

    def list_user_sessions(
        self, user_id: int, *, session: Optional[Session] = None
    ) -> List[Tuple[int, datetime.datetime]]:
        """
        List all sessions currently in the database for a single user
        """

        with self.root_socket.optional_session(session, True) as session:
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
