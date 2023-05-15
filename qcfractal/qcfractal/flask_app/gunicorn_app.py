from __future__ import annotations

import logging
import logging.handlers
from typing import TYPE_CHECKING

import gunicorn.app.base
from gunicorn.glogging import Logger as GLogger

from .flask_app import create_flask_app, storage_socket

if TYPE_CHECKING:
    import queue
    import threading
    from typing import Optional
    from ..config import FractalConfig


#####################################################
# See https://docs.gunicorn.org/en/stable/custom.html
#####################################################


def post_fork_cleanup(server, worker):
    """
    Do some cleanup after forking inside gunicorn

    We use synchronous workers, which are spawned via fork(). Howver,
    this would cause multiple processes to share the same db connections.
    We must dispose of them (from the global storage_socket object).

    https://docs.sqlalchemy.org/en/14/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
    """

    storage_socket.post_fork_cleanup()


class FractalGunicornLogger(GLogger):
    def setup(self, cfg):
        # The base constructor messes with logging. So undo the important parts
        # of those changes. That way, all logging will be unified
        self.error_log = logging.getLogger("gunicorn.error")
        self.access_log = logging.getLogger("gunicorn.access")
        self.error_log.handlers = []
        self.access_log.handlers = []
        self.error_log.propagate = True
        self.access_log.propagate = True


class FractalGunicornApp(gunicorn.app.base.BaseApplication):
    def __init__(
        self,
        qcfractal_config: FractalConfig,
        finished_queue: Optional[queue.Queue] = None,
        started_event: Optional[threading.Event] = None,
    ):
        self.qcfractal_config = qcfractal_config
        self.application = create_flask_app(qcfractal_config, finished_queue=finished_queue)
        self.started_event = started_event

        super().__init__()

    def _server_ready(self, server):
        if self.started_event is not None:
            self.started_event.set()

    def load(self):
        return self.application

    def load_config(self):
        # This must be provided. It is called from the superclass, and we need to
        # populate self.cfg
        bind = self.qcfractal_config.api.host
        port = self.qcfractal_config.api.port

        # Use the sync worker class, however if threads > 1, then the gthread worker class will
        # be automatically used instead (according to gunicorn docs)
        # https://docs.gunicorn.org/en/stable/settings.html#threads
        config = {
            "worker_class": "sync",
            "bind": f"{bind}:{port}",
            "workers": self.qcfractal_config.api.num_workers,
            "threads": self.qcfractal_config.api.num_threads_per_worker,
            "timeout": self.qcfractal_config.api.worker_timeout,
            "loglevel": self.qcfractal_config.loglevel,
            "logger_class": FractalGunicornLogger,
            "post_fork": post_fork_cleanup,
            "when_ready": self._server_ready,
        }

        if self.qcfractal_config.api.extra_gunicorn_options:
            config.update(**self.qcfractal_config.api.extra_gunicorn_options)

        for key, value in config.items():
            self.cfg.set(key.lower(), value)
