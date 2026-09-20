from __future__ import annotations

from queue import Queue
from typing import Optional

from flask import Flask

from qcfractal.db_socket import SQLAlchemySocket


class FlaskStorageSocket:

    def init_app(self, app: Flask, finished_queue: Optional[Queue] = None):
        socket = SQLAlchemySocket(app.config["QCFRACTAL_CONFIG"])

        app.extensions["storage_socket"] = socket

        if finished_queue:
            socket.set_finished_watch(finished_queue)

    def get_socket(self, app: Flask) -> SQLAlchemySocket:
        s = app.extensions.get("storage_socket")
        if s is None:
            raise RuntimeError("Socket not initialized for this flask app")

        return s
