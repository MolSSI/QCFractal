from __future__ import annotations

from queue import Queue
from typing import Optional
from weakref import WeakKeyDictionary

from flask import Flask

from qcfractal.db_socket import SQLAlchemySocket


class FlaskStorageSocket:

    _app_sockets: WeakKeyDictionary[Flask, SQLAlchemySocket]

    def __init__(self):
        self._app_sockets = WeakKeyDictionary()

    def init_app(self, app: Flask, finished_queue: Optional[Queue] = None):
        socket = SQLAlchemySocket(app.config["QCFRACTAL_CONFIG"])

        if not hasattr(app, "extensions"):
            app.extensions = {}

        app.extensions["storage_socket"] = socket

        if finished_queue:
            socket.set_finished_watch(finished_queue)

        self._app_sockets[app] = socket

    def get_socket(self, app) -> SQLAlchemySocket:
        app_co = app._get_current_object()

        s = self._app_sockets.get(app_co, None)
        if s is None:
            raise RuntimeError("Socket not initialized for this flask app")

        return s
