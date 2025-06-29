from __future__ import annotations

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
    ):
        self.qcfractal_config = qcfractal_config
        self.application = create_flask_app(qcfractal_config, finished_queue=finished_queue)

    def run(self):
        from waitress import serve

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
