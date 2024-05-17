from __future__ import annotations

import logging
import logging.handlers
from typing import TYPE_CHECKING

from .flask_app import create_flask_app

if TYPE_CHECKING:
    import queue
    from typing import Optional
    from ..config import FractalConfig


class FractalWaitressApp:
    def __init__(
        self,
        qcfractal_config: FractalConfig,
        finished_queue: Optional[queue.Queue] = None,
        logging_queue: Optional[queue.Queue] = None,
    ):
        self.qcfractal_config = qcfractal_config
        self.application = create_flask_app(qcfractal_config, finished_queue=finished_queue)
        self.logging_queue = logging_queue

    def run(self):
        from waitress import serve

        if self.logging_queue:
            # Replace the root handler with one that just sends data to the logging queue
            log_handler = logging.handlers.QueueHandler(self.logging_queue)
            root_logger = logging.getLogger()
            root_logger.handlers.clear()
            root_logger.addHandler(log_handler)

        waitress_opts = self.qcfractal_config.api.extra_waitress_options
        if waitress_opts is None:
            waitress_opts = {}

        serve(
            self.application,
            host=self.qcfractal_config.api.host,
            port=self.qcfractal_config.api.port,
            threads=self.qcfractal_config.api.num_threads_per_worker,
            **waitress_opts,
        )
