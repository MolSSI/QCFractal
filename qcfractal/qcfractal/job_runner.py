"""
Classes for handling periodic tasks required by QCFractal

QCFractal requires some periodic maintenance. These include updating services, pruning
dead managers, and updating statistics.
"""

from __future__ import annotations

import logging
import multiprocessing
from typing import TYPE_CHECKING, Optional

from .db_socket.socket import SQLAlchemySocket
from .process_runner import ProcessBase

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
        end_event,
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

        self.logger = logging.getLogger("qcfractal_internal_jobs")
        self._end_event = end_event

    def start(self) -> None:
        """
        Start running the periodic tasks in the foreground

        This function will block until interrupted
        """

        self.storage_socket.internal_jobs.run_processes(self._end_event)


class FractalJobRunnerProcess(ProcessBase):
    """
    Enable running periodics in a separate process

    This is used with :class:`process_runner.ProcessRunner` to run all periodic tasks in a separate process
    """

    def __init__(
        self,
        qcf_config: FractalConfig,
        finished_queue: Optional[multiprocessing.Queue] = None,
    ):
        ProcessBase.__init__(self)
        self._qcf_config = qcf_config
        self._end_event = multiprocessing.Event()
        self._finished_queue = finished_queue

        # ---------------------------------------------------------------
        # We should not instantiate the FractalJobRunner class here.
        # The setup and run functions will be run in a separate process
        # and so instantiation should happen there
        # ---------------------------------------------------------------

    def setup(self) -> None:
        self._runner = FractalJobRunner(self._qcf_config, self._end_event, self._finished_queue)

    def run(self) -> None:
        self._runner.start()

    def interrupt(self) -> None:
        self._end_event.set()
