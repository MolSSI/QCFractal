from __future__ import annotations

import datetime
import secrets
import uuid
from typing import TYPE_CHECKING, Optional

from flask.sessions import SessionInterface, SessionMixin, SecureCookieSession

from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from flask import Flask, Response, Request


# The SecureCookieSession tracks accesses and modifications for us
class QCFFlaskSession(SecureCookieSession):
    session_id: Optional[str] = None


class QCFFlaskSessionInterface(SessionInterface):
    """
    Interface for a database-backed user session store

    open_session will see if data exists in the database and use that. A check is made for expired data as well.

    If there is something to store in the database, a random session_id will be generated in save_session.
    """

    def __init__(self, storage_socket: SQLAlchemySocket):
        self._storage_socket = storage_socket

    def open_session(self, app: Flask, request: Request) -> QCFFlaskSession:
        """
        Retrieve the session from the database

        If no session exists, or if the session has expired, an empty one will be created.

        This function will not set the session_id. That way, empty data is not persisted to the database
        """
        cookie_name = self.get_cookie_name(app)

        # Get the session_id from the cookie
        session_id = request.cookies.get(cookie_name)

        if not session_id:
            return QCFFlaskSession()

        session_data, last_accessed = self._storage_socket.auth.load_user_session(session_id)

        if session_data is None:
            return QCFFlaskSession()  # New session if none exists

        ret = QCFFlaskSession(initial=session_data)
        ret.session_id = session_id

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

        If no session_id is given on the QCFFlaskSession object, a new one will be generated. Otherwise, existing
        data will be replaced in the database.

        This function will also request to delete cookies if needed.
        """

        # This code adapted from flask sessions:
        # https://github.com/pallets/flask/blob/f61172b8dd3f962d33f25c50b2f5405e90ceffa5/src/flask/sessions.py#L350

        # See if the session_id exists in the data. If so, we should reuse the session
        cookie_name = self.get_cookie_name(app)
        session_id = session_data.session_id  # may be None

        if not session_data:
            if session_data.modified:
                # Session was modified to be empty, so delete the cookie
                response.delete_cookie(cookie_name)
                response.vary.add("Cookie")

            # session_id should only be attached if there is something in the database
            # That is, only if there was data loaded from the database in open_session
            # This should prevent unnecessary database queries.
            if session_id:
                self._storage_socket.auth.delete_user_session(session_id)
        else:
            if not session_id:
                # create a unique ID if needed
                session_id = secrets.token_urlsafe(32)
                assert len(session_id) > 36  # Paranoid

                # Store for later, so we know to reuse this session
                session_data.session_id = session_id

            # Update the session data in the database
            self._storage_socket.auth.save_user_session(session_id, session_data)

            # Set the cookie in the response
            # Same name & session id, but extend the lifetime
            response.set_cookie(
                cookie_name,
                session_id,
                domain=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_domain,
                max_age=app.permanent_session_lifetime,
                httponly=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_httponly,
                samesite=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_samesite,
                secure=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_secure,
                partitioned=app.config["QCFRACTAL_CONFIG"].api.user_session_cookie_partitioned,
            )
            response.vary.add("Cookie")
