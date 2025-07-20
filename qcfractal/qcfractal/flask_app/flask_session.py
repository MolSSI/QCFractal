from __future__ import annotations

import secrets
from typing import TYPE_CHECKING, Optional

from flask.sessions import SessionInterface, SecureCookieSession

from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from flask import Flask, Response, Request


# The SecureCookieSession tracks accesses and modifications for us
class QCFFlaskSession(SecureCookieSession):
    session_key: Optional[str] = None


class QCFFlaskSessionInterface(SessionInterface):
    """
    Interface for a database-backed user session store

    open_session will see if data exists in the database and use that. A check is made for expired data as well.

    If there is something to store in the database, a random session_key will be generated in save_session.
    """

    def __init__(self, app: Flask):
        if not hasattr(app, "extensions") or "storage_socket" not in app.extensions:
            raise RuntimeError("The QCFFlaskSessionInterface requires the storage_socket extension to be initialized")

        self._storage_socket = app.extensions["storage_socket"]

    def open_session(self, app: Flask, request: Request) -> QCFFlaskSession:
        """
        Retrieve the session from the database

        If no session exists, or if the session has expired, an empty one will be created.

        This function will not set the session_key. That way, empty data is not persisted to the database
        """
        cookie_name = self.get_cookie_name(app)

        # Get the session_key from the cookie
        session_key = request.cookies.get(cookie_name)

        # No session cookie set - use empty session
        if not session_key:
            return QCFFlaskSession()

        session_data, last_accessed = self._storage_socket.auth.load_user_session(session_key)

        # No session data
        if session_data is None:
            return QCFFlaskSession()  # New session if none exists

        ret = QCFFlaskSession(initial=session_data)
        ret.session_key = session_key

        # Set/update basic data
        ret["user_agent"] = request.headers.get("User-Agent")
        ret["ip_address"] = request.remote_addr

        # IMPORTANT - Has the session expired?
        # Normally, this would be handled by flask with an expired cookie.
        # But the user may forge the expiration time
        # Note that app.permanent_session_lifetime is a timedelta
        expire_time = last_accessed + app.permanent_session_lifetime
        if expire_time < now_at_utc():
            return QCFFlaskSession()  # Consider the session invalid and return a new one

        return ret

    def save_session(self, app: Flask, session_data: QCFFlaskSession, response: Response) -> None:
        """
        Save the session to the database

        If no session_key is given on the QCFFlaskSession object, a new one will be generated. Otherwise, existing
        data will be replaced in the database.

        This function will also request to delete cookies if needed.
        """

        # This code adapted from flask sessions:
        # https://github.com/pallets/flask/blob/f61172b8dd3f962d33f25c50b2f5405e90ceffa5/src/flask/sessions.py#L350

        # See if the session_key exists in the data. If so, we should reuse the session
        cookie_name = self.get_cookie_name(app)
        session_key = session_data.session_key  # may be None

        if not session_data:
            if session_data.modified:
                # Session was modified to be empty, so delete the cookie
                response.delete_cookie(cookie_name)
                response.vary.add("Cookie")

            # session_key should only be attached if there is something in the database
            # That is, only if there was data loaded from the database in open_session
            # This should prevent unnecessary database queries.
            if session_key:
                self._storage_socket.auth.delete_user_session(user_session_key=session_key)
        else:
            user_id = int(session_data["user_id"])

            if not session_key:
                # create a unique ID if needed
                session_key = secrets.token_urlsafe(32)
                assert len(session_key) > 36  # Paranoid

                # Store for later, so we know to reuse this session
                session_data.session_key = session_key

            # Update the session data in the database
            self._storage_socket.auth.save_user_session(user_id, session_key, session_data)

            # Set the cookie in the response
            # Same name & session id, but extend the lifetime
            response.set_cookie(
                cookie_name,
                session_key,
                domain=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_domain,
                max_age=app.permanent_session_lifetime,
                httponly=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_httponly,
                samesite=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_samesite,
                secure=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_secure,
                partitioned=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_partitioned,
            )
            response.vary.add("Cookie")
