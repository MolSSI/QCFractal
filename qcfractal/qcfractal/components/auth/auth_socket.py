from __future__ import annotations

import datetime
import hashlib
import secrets
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Tuple, List, Any, Dict, Optional, Union

from sqlalchemy import select, delete, update, func, or_
from sqlalchemy.dialects.postgresql import insert

from sqlalchemy.exc import IntegrityError

from qcportal.auth import (
    UserInfo,
    API_TOKEN_PREFIX,
    MAX_API_TOKEN_NAME_LENGTH,
    looks_like_api_token,
)
from qcportal.exceptions import AuthenticationFailure, SecurityNotEnabledError, UserManagementError
from qcportal.utils import now_at_utc
from .db_models import UserORM, UserSessionORM, UserAPITokenORM
from .permission_evaluation import evaluate_global_permissions
from .role_permissions import AuthorizedEnum

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


# Number of random bytes in an API token. 32 bytes = 256 bits, so guessing a token is infeasible
_API_TOKEN_NBYTES = 32

# A single "qcf_" plus this many characters of the token are stored as a non-secret display prefix
_API_TOKEN_PREFIX_LENGTH = 12

# The maximum number of tokens a single user may have at once. me:* lets any user create tokens for
# themselves, so this bounds how much one account can write to the table
_MAX_API_TOKENS_PER_USER = 100

# last_used_at is only rewritten once it is this stale, to avoid a database write on every request
_API_TOKEN_LAST_USED_THROTTLE = datetime.timedelta(minutes=5)

# A single message for every way a token can fail to authenticate. Deliberately uniform so that a
# caller cannot tell "no such token" from "expired" from "malformed". Must not contain the substring
# "Token has expired", which the qcportal client matches to trigger a JWT refresh
_API_TOKEN_INVALID_MSG = "API token does not exist, is not valid, or is expired"


def hash_api_token(raw_token: str) -> str:
    """
    Hashes an API token for storage or lookup

    Like a browser session key (see hash_session_key), an API token is a high-entropy bearer
    credential, so only its SHA-256 hash is ever stored and a single indexed comparison suffices.
    The entire token as presented (including the "qcf_" prefix) is hashed.
    """

    return hashlib.sha256(raw_token.encode("UTF-8")).hexdigest()


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

        # API token expiration policy (see create_api_token). Both may be None (no default
        # expiration / no maximum lifetime)
        self._api_token_default_lifetime = self.root_socket.qcf_config.api.api_token_default_lifetime
        self._api_token_max_lifetime = self.root_socket.qcf_config.api.api_token_max_lifetime

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

    ############################
    # API token management
    ############################
    def _resolve_api_token_expiration(
        self, expires_at: Optional[datetime.datetime], now: datetime.datetime
    ) -> Optional[datetime.datetime]:
        """
        Applies the server's expiration policy to a requested token expiration

        Returns the expiration to store (which may be None for a non-expiring token). Raises
        UserManagementError if the request is not allowed by the configured default/maximum
        lifetimes.
        """

        # A caller-supplied expiration must be timezone-aware and in the future
        if expires_at is not None:
            if expires_at.tzinfo is None:
                raise UserManagementError("API token expiration must be timezone-aware")
            if expires_at <= now:
                raise UserManagementError("API token expiration must be in the future")
        else:
            # No expiration requested - apply the default lifetime if one is configured
            if self._api_token_default_lifetime is not None:
                expires_at = now + datetime.timedelta(seconds=self._api_token_default_lifetime)

        # Enforce the maximum lifetime. A never-expiring token is only allowed when there is no
        # maximum - otherwise it (and anything beyond the maximum) is rejected, never silently
        # shortened, so the caller is not handed a credential that dies sooner than they asked
        if self._api_token_max_lifetime is not None:
            latest = now + datetime.timedelta(seconds=self._api_token_max_lifetime)
            if expires_at is None or expires_at > latest:
                raise UserManagementError(
                    f"API token expiration may not be more than {self._api_token_max_lifetime} seconds "
                    "in the future (set by the server's api_token_max_lifetime)"
                )

        return expires_at

    def create_api_token(
        self,
        user_id: int,
        name: str,
        expires_at: Optional[datetime.datetime] = None,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Creates a new API token for a user

        The name is required and must be unique among the user's tokens. Returns a tuple of
        (plaintext token, token metadata). The plaintext token is not stored and cannot be recovered
        afterwards - only its hash is kept. The metadata is the public_dict of the new token (never
        including the hash).

        Raises UserManagementError if the name is missing/too long or already in use, if the
        requested expiration violates the server policy, or if the user already has the maximum
        number of tokens.
        """

        if not name:
            raise UserManagementError("An API token name is required")
        if len(name) > MAX_API_TOKEN_NAME_LENGTH:
            raise UserManagementError(f"API token name must be at most {MAX_API_TOKEN_NAME_LENGTH} characters")

        now = now_at_utc()
        stored_expires_at = self._resolve_api_token_expiration(expires_at, now)

        raw_token = API_TOKEN_PREFIX + secrets.token_urlsafe(_API_TOKEN_NBYTES)
        token_hash = hash_api_token(raw_token)
        token_prefix = raw_token[:_API_TOKEN_PREFIX_LENGTH]

        with self.root_socket.optional_session(session) as session:
            # Lock the owning user row so a concurrent create cannot also pass the count check and
            # push the user over the limit. This also confirms the user exists.
            user_exists = session.execute(
                select(UserORM.id).where(UserORM.id == user_id).with_for_update()
            ).scalar_one_or_none()
            if user_exists is None:
                raise UserManagementError(f"User with id {user_id} does not exist")

            count = session.execute(
                select(func.count()).select_from(UserAPITokenORM).where(UserAPITokenORM.user_id == user_id)
            ).scalar_one()
            if count >= _MAX_API_TOKENS_PER_USER:
                raise UserManagementError(
                    f"User already has the maximum number of API tokens ({_MAX_API_TOKENS_PER_USER}). "
                    "Delete an existing token before creating a new one."
                )

            token_orm = UserAPITokenORM(
                user_id=user_id,
                token_hash=token_hash,
                token_prefix=token_prefix,
                name=name,
                created_at=now,
                expires_at=stored_expires_at,
                last_used_at=None,
            )
            session.add(token_orm)
            try:
                session.flush()
            except IntegrityError:
                # The (user_id, name) unique constraint - the user already has a token by this name
                raise UserManagementError(f"An API token named '{name}' already exists for this user")

            return raw_token, token_orm.public_dict()

    def verify_api_token(self, raw_token: str, *, session: Optional[Session] = None) -> Tuple[int, int]:
        """
        Verifies an API token and returns (user_id, token_id)

        This performs *only* the token lookup: it confirms the token exists and has not expired, and
        returns the owning user id (which the caller then verifies with auth.verify, so that a
        disabled account or a changed role takes effect regardless of the token). It deliberately
        does not check whether the user is enabled - that is auth.verify's job.

        This is the authoritative, uncached lookup. The request path calls it through a short-lived
        cache (see CachedTokenVerifier), so in practice revoking a token takes effect within the
        cache lifetime rather than instantly.

        Raises AuthenticationFailure (with a single uniform message) for any invalid token - unknown,
        malformed, or expired - so that a caller cannot distinguish these cases.
        """

        # Reject anything that is not shaped like one of our tokens before hashing. The length cap
        # in particular keeps an oversized Authorization header from being hashed on every request.
        if not looks_like_api_token(raw_token):
            raise AuthenticationFailure(_API_TOKEN_INVALID_MSG)

        token_hash = hash_api_token(raw_token)
        now = now_at_utc()

        # A fresh, writable session owned by this method: the last_used_at update below must commit
        # even if the request later fails, and the expiration check must use the current time rather
        # than a caller transaction's (possibly old) start time.
        with self.root_socket.optional_session(session, False) as session:
            stmt = select(UserAPITokenORM).where(
                UserAPITokenORM.token_hash == token_hash,
                or_(UserAPITokenORM.expires_at.is_(None), UserAPITokenORM.expires_at > now),
            )
            token_orm = session.execute(stmt).scalar_one_or_none()

            if token_orm is None:
                raise AuthenticationFailure(_API_TOKEN_INVALID_MSG)

            user_id = token_orm.user_id
            token_id = token_orm.id

            # Refresh last_used_at, but only if it is stale, to avoid a write on every request. The
            # predicate is repeated in the WHERE clause so concurrent workers do not both write.
            if token_orm.last_used_at is None or (now - token_orm.last_used_at) > _API_TOKEN_LAST_USED_THROTTLE:
                cutoff = now - _API_TOKEN_LAST_USED_THROTTLE
                session.execute(
                    update(UserAPITokenORM)
                    .where(
                        UserAPITokenORM.id == token_id,
                        or_(
                            UserAPITokenORM.last_used_at.is_(None),
                            UserAPITokenORM.last_used_at < cutoff,
                        ),
                    )
                    .values(last_used_at=now)
                )

            return user_id, token_id

    def list_api_tokens(self, user_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Lists all API tokens belonging to a single user (never including the token hash)
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserAPITokenORM).where(UserAPITokenORM.user_id == user_id)
            stmt = stmt.order_by(UserAPITokenORM.id)
            token_orms = session.execute(stmt).scalars().all()
            return [t.public_dict() for t in token_orms]

    def list_all_api_tokens(self, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Lists all API tokens in the database (never including the token hash)
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserAPITokenORM).order_by(UserAPITokenORM.id)
            token_orms = session.execute(stmt).scalars().all()
            return [t.public_dict() for t in token_orms]

    def delete_api_token(self, token_id: int, user_id: int, *, session: Optional[Session] = None) -> None:
        """
        Deletes (revokes) a single API token

        The user_id is required and always constrains the delete, so that a token can only ever be
        revoked by (or on behalf of) its owner - a caller cannot revoke another user's token by
        guessing its id. Raises UserManagementError if no such token exists for that user (the same
        error whether the token does not exist or belongs to someone else).
        """

        with self.root_socket.optional_session(session) as session:
            stmt = delete(UserAPITokenORM).where(
                UserAPITokenORM.id == token_id,
                UserAPITokenORM.user_id == user_id,
            )
            result = session.execute(stmt)

            if result.rowcount == 0:
                raise UserManagementError("API token not found")
