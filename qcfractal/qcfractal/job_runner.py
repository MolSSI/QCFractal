"""
Classes for handling periodic tasks required by QCFractal

QCFractal requires some periodic maintenance. These include updating services, pruning
dead managers, and updating statistics.
"""

from __future__ import annotations

import multiprocessing
import threading
from typing import TYPE_CHECKING, Optional

from .db_socket.socket import SQLAlchemySocket

if TYPE_CHECKING:
    from .config import FractalConfig


class FractalJobRunner:
    """
    Runs internal QCFractal jobs

    QCFractal requires some jobs to be run periodically or otherwise in the background. This class will run these jobs.
    """

    def __init__(
        self,
        qcf_config: FractalConfig,
        finished_queue: Optional[multiprocessing.Queue] = None,
    ):
        """
        Parameters
        ----------
        qcf_config: FractalConfig
            Configuration for the QCFractal server
        """

        self.storage_socket = SQLAlchemySocket(qcf_config)
        self.storage_socket.set_finished_watch(finished_queue)
        self._end_event = threading.Event()

    def start(self) -> None:
        """
        Start running the periodic tasks in the foreground

        This function will block until interrupted
        """

        self.storage_socket.internal_jobs.run_loop(self._end_event)

    def stop(self) -> None:
        """
        Stop running the periodic tasks
        """

        self._end_event.set()
