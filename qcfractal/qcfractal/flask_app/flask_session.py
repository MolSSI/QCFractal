from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Optional, Any, Dict

from flask import request as flask_request
from flask.sessions import SessionInterface, SecureCookieSession

from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from flask import Flask, Response, Request


class QCFFlaskSession(SecureCookieSession):
    """
    A user session backed by the user_session database table

    Besides the (dict) session data, this tracks the database key the session was loaded under,
    the user that owns that database row, and any key that must be revoked when the session is saved.

    The SecureCookieSession base class tracks accesses and modifications for us.
    """

    def __init__(self, initial: Optional[Dict[str, Any]] = None):
        super().__init__(initial)

        # Key under which the data was loaded from the database (None if not persisted yet)
        self.session_key: Optional[str] = None

        # Owner of the database row (from the relational user_id column, not the session data)
        self.user_id: Optional[int] = None

        # A key that must be deleted from the database when this session is saved
        self.revoke_key: Optional[str] = None

        # When the database row was last accessed (as of loading it)
        self.last_accessed: Optional[datetime] = None

    def rotate(self) -> None:
        """
        Discards the current session

        All data is cleared, and the current key (if any) will be deleted from the database when
        the session is saved. Any data set afterwards is stored under a freshly-generated key.

        This must be called on login (after successful authentication) so that a key that existed
        before authentication is never associated with the authenticated user (session fixation),
        and on logout.
        """

        if self.session_key is not None:
            self.revoke_key = self.session_key

        self.session_key = None
        self.user_id = None
        self.last_accessed = None
        self.clear()


class QCFFlaskSessionInterface(SessionInterface):
    """
    Interface for a database-backed user session store

    open_session will see if data exists in the database and use that. A check is made for expired data as well.

    save_session persists the session. New sessions (after login) get a random session_key. Existing sessions
    are only ever updated, never re-created - if the row is gone, the session was revoked and stays revoked.

    Sessions expire after being idle for the configured lifetime. To avoid a database write (and a new
    Set-Cookie) on every request, an unmodified session is only "touched" once its last access is older
    than a refresh threshold (a tenth of the lifetime, at most five minutes). The effective idle timeout
    is therefore up to one threshold shorter than configured.
    """

    # Upper bound on how long an unmodified session goes without its last-access time being refreshed
    _max_refresh_threshold = timedelta(minutes=5)

    def __init__(self, app: Flask):
        if not hasattr(app, "extensions") or "storage_socket" not in app.extensions:
            raise RuntimeError("The QCFFlaskSessionInterface requires the storage_socket extension to be initialized")

        self._storage_socket = app.extensions["storage_socket"]

    def _cookie_options(self, app: Flask) -> Dict[str, Any]:
        """
        Cookie attributes, which must be identical when setting and deleting the cookie
        """

        api_config = app.config["QCFRACTAL_CONFIG"].api
        return {
            "path": self.get_cookie_path(app),
            "domain": api_config.user_session_cookie_domain,
            "httponly": api_config.user_session_cookie_httponly,
            "samesite": api_config.user_session_cookie_samesite,
            "secure": api_config.user_session_cookie_secure,
            "partitioned": api_config.user_session_cookie_partitioned,
        }

    def _refresh_threshold(self, app: Flask) -> timedelta:
        return min(app.permanent_session_lifetime / 10, self._max_refresh_threshold)

    @staticmethod
    def _update_client_metadata(session_data: QCFFlaskSession, request: Request) -> None:
        """
        Stores basic information about the client in the session (only if changed, to avoid
        marking the session as modified unnecessarily)
        """

        user_agent = request.headers.get("User-Agent")
        if session_data.get("user_agent") != user_agent:
            session_data["user_agent"] = user_agent

        ip_address = request.remote_addr
        if session_data.get("ip_address") != ip_address:
            session_data["ip_address"] = ip_address

    def open_session(self, app: Flask, request: Request) -> QCFFlaskSession:
        """
        Retrieve the session from the database

        If no session exists, or if the session has expired, an empty one will be created.

        This function will not set the session_key on an empty session. That way, empty data is
        not persisted to the database.
        """
        cookie_name = self.get_cookie_name(app)

        # Get the session_key from the cookie
        session_key = request.cookies.get(cookie_name)

        # No session cookie set - use empty session
        if not session_key:
            return QCFFlaskSession()

        loaded = self._storage_socket.auth.load_user_session(session_key)

        # No session data
        if loaded is None:
            return QCFFlaskSession()  # New session if none exists

        user_id, session_data, last_accessed = loaded

        # IMPORTANT - Has the session expired?
        # Normally, this would be handled by flask with an expired cookie.
        # But the user may forge the expiration time
        # Note that app.permanent_session_lifetime is a timedelta
        expire_time = last_accessed + app.permanent_session_lifetime
        if expire_time < now_at_utc():
            # Consider the session invalid. Remove it and return a new one
            self._storage_socket.auth.delete_user_session(user_session_key=session_key)
            return QCFFlaskSession()

        # The relational user_id column is authoritative. Session data that disagrees is invalid
        if str(session_data.get("user_id")) != str(user_id):
            app.logger.error(f"User session data does not match the owner of the session (user {user_id}). Revoking")
            self._storage_socket.auth.delete_user_session(user_session_key=session_key)
            return QCFFlaskSession()

        ret = QCFFlaskSession(initial=session_data)
        ret.session_key = session_key
        ret.user_id = user_id
        ret.last_accessed = last_accessed

        self._update_client_metadata(ret, request)
        return ret

    def save_session(self, app: Flask, session_data: QCFFlaskSession, response: Response) -> None:
        """
        Save the session to the database

        If no session_key is given on the QCFFlaskSession object, a new one will be generated
        (revoking any key marked for revocation in the same transaction). Otherwise, the existing
        row is updated - but never re-created.

        This function will also request to delete cookies if needed.
        """

        # This code adapted from flask sessions:
        # https://github.com/pallets/flask/blob/f61172b8dd3f962d33f25c50b2f5405e90ceffa5/src/flask/sessions.py#L350

        cookie_name = self.get_cookie_name(app)
        cookie_options = self._cookie_options(app)
        auth_socket = self._storage_socket.auth

        # Responses that depend on the session must not be cached across users
        if session_data.accessed:
            response.vary.add("Cookie")

        if not session_data:
            # Empty session (never logged in, logged out, or rotated without new data).
            # Revoke any keys - both the key it was loaded under and any key explicitly marked for revocation
            for key in (session_data.revoke_key, session_data.session_key):
                if key:
                    auth_socket.delete_user_session(user_session_key=key)

            if session_data.modified or session_data.revoke_key:
                # Session was modified to be empty, so delete the cookie
                # The attributes must match those used when setting the cookie
                response.delete_cookie(cookie_name, **cookie_options)
                response.vary.add("Cookie")

            return

        user_id = int(session_data["user_id"])
        session_key = session_data.session_key

        if session_key is None:
            # Brand new session (typically right after login). Generate a key and insert it,
            # deleting any key marked for revocation in the same transaction
            session_key = secrets.token_urlsafe(32)
            assert len(session_key) > 36  # Paranoid

            # Store the client information right away, so the next request does not need to
            self._update_client_metadata(session_data, flask_request)

            auth_socket.rotate_user_session(session_data.revoke_key, user_id, session_key, dict(session_data))

            # Store for later, so we know to reuse this session
            session_data.session_key = session_key
            session_data.user_id = user_id
            session_data.revoke_key = None
        else:
            # Existing, unmodified session: only refresh the last-access time (and the cookie) once
            # it is older than the threshold. See the class docstring
            if not session_data.modified and session_data.last_accessed is not None:
                if now_at_utc() - session_data.last_accessed < self._refresh_threshold(app):
                    return

            # Existing session. Only update an existing row - never re-create one. If the row is gone,
            # the session was revoked (logout, administrative action, expiry cleanup) while this
            # request was in flight, and it must stay revoked. The cookie is deliberately left
            # untouched so that a late response cannot clobber a cookie issued by a newer login.
            if not auth_socket.update_user_session(session_data.user_id, session_key, dict(session_data)):
                app.logger.info(f"User session for user {user_id} was revoked while a request was in progress")
                return

        # Set the cookie in the response
        # Same name & session id, but extend the lifetime
        response.set_cookie(cookie_name, session_key, max_age=app.permanent_session_lifetime, **cookie_options)
        response.vary.add("Cookie")
