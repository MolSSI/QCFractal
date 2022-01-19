"""
Classes for handling periodic tasks required by QCFractal

QCFractal requires some periodic maintenance. These include updating services, pruning
dead managers, and updating statistics.
"""

from __future__ import annotations

import logging
import sched
import time
import weakref
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from .db_socket.socket import SQLAlchemySocket
from .process_runner import ProcessBase, InterruptableSleep, SleepInterrupted

if TYPE_CHECKING:
    import multiprocessing
    from typing import Optional
    from .config import FractalConfig


class FractalPeriodics:
    """
    Periodic tasks required by QCFractal

    QCFractal requires some tasks to be run periodically. This class will run these tasks
    in the background.
    """

    def __init__(self, qcf_config: FractalConfig, completed_queue: Optional[multiprocessing.Queue] = None):
        """
        Parameters
        ----------
        qcf_config: FractalConfig
            Configuration for the QCFractal server

        completed_queue: multiprocessing.Queue, optional
            A multiprocessing queue. If passed to this function, information about completed computations
            will be passed into this queue. See :func:`SQLAlchemySocket.set_completed_watch`
        """

        self.storage_socket = SQLAlchemySocket(qcf_config)
        self._int_sleep = InterruptableSleep()
        self.scheduler = sched.scheduler(time.time, self._int_sleep)

        self._completed_queue = completed_queue
        self.storage_socket.set_completed_watch(self._completed_queue)

        self.logger = logging.getLogger("qcfractal_periodics")

        # Frequencies/Intervals at which to run various tasks
        self.server_stats_frequency = qcf_config.statistics_frequency
        self.manager_heartbeat_frequency = qcf_config.heartbeat_frequency
        self.manager_max_missed_heartbeats = qcf_config.heartbeat_max_missed

        self.service_frequency = qcf_config.service_frequency
        self.max_active_services = qcf_config.max_active_services

        self.logger.info("Periodics Information:")
        self.logger.info(f"    Server stats update frequency: {self.server_stats_frequency} seconds")
        self.logger.info(f"      Manager heartbeat frequency: {self.manager_heartbeat_frequency} seconds")
        self.logger.info(f"    Manager max missed heartbeats: {self.manager_max_missed_heartbeats}")
        self.logger.info(f"         Service update frequency: {self.service_frequency} seconds")
        self.logger.info(f"              Max active services: {self.max_active_services}")

        # Now set up the finalizer so that the background stuff always shuts down correctly
        self._finalizer = weakref.finalize(self, self._stop, self.logger, self._int_sleep)

        self.logger.info("Initializing QCFractal Periodics")

        # Set up the typical periodics
        # We set the delay to zero so it will run immediately. Inside the various functions
        # we will set up the next runs
        # The second argument (1,2,3) is the priority

        # 1.) Updating the overall server information (counts, etc)
        #     This is stored in a public info class
        self.scheduler.enter(0, 1, self._update_server_stats)

        # 2.) Manager heartbeats
        self.scheduler.enter(0, 2, self._check_manager_heartbeats)

        # 3.) Service updates
        self.scheduler.enter(0, 3, self._update_services)

    def _update_server_stats(self) -> None:
        """
        Updates various server statistics (number of results, etc)
        """
        self.logger.info("Updating server stats in the database")
        self.storage_socket.serverinfo.update_server_stats()

        # Set up the next run of this function
        self.scheduler.enter(self.server_stats_frequency, 1, self._update_server_stats)

    def _check_manager_heartbeats(self) -> None:
        """
        Checks for manager heartbeats

        If a manager has not been heard from in a while, it is set to inactivate and its tasks
        reset to a waiting state. The amount of time to wait for a manager is controlled by the config
        options manager_max_missed_heartbeats and manager_heartbeat_frequency.
        """
        self.logger.info("Checking manager heartbeats")
        manager_window = self.manager_max_missed_heartbeats * self.manager_heartbeat_frequency
        dt = datetime.utcnow() - timedelta(seconds=manager_window)

        dead_managers = self.storage_socket.managers.deactivate(modified_before=dt, reason="missing heartbeat")

        if dead_managers:
            self.logger.info(f"Deactivated {len(dead_managers)} managers due to missing heartbeats")

        # Set up the next run of this function
        self.scheduler.enter(self.manager_heartbeat_frequency, 2, self._check_manager_heartbeats)

    def _update_services(self) -> None:
        """Runs through all active services and examines their current status

        This will check all services to see if they require another iteration, and if so, perform that iteration.
        It will also check for errors.

        If new services are waiting, they will also be started as long as the total number of services
        are under the limit given by max_active_services

        The current number of running services is returned. While this is not used when running under the scheduler,
        it is used in testing where this function is run manually
        """

        self.logger.debug(f"Updating/iterating services...")
        time_0 = time.time()
        self.storage_socket.services.iterate_services()
        time_1 = time.time()
        self.logger.info(f"Services iterated. Took {((time_1-time_0)*1000):.1f} ms")

        # Set up the next run of this function
        self.scheduler.enter(self.service_frequency, 3, self._update_services)

    def start(self) -> None:
        """
        Start running the periodic tasks in the foreground

        This function will block until interrupted
        """

        try:
            self.scheduler.run()
        except SleepInterrupted:
            self.logger.info("Scheduler interrupted and is now shut down")

    @classmethod
    def _stop(cls, logger, int_sleep) -> None:
        ####################################################################################
        # This is written as a class method so that it can be called by a weakref finalizer
        ####################################################################################

        logger.info("Shutting down periodics (currently running tasks will finish)")
        int_sleep.interrupt()

    def stop(self) -> None:
        """
        Stop running the periodic tasks

        This will stop the tasks that are running in the background. Currently running tasks will
        be allowed to finish.
        """

        self._stop(self.logger, self._int_sleep)


class PeriodicsProcess(ProcessBase):
    """
    Enable running periodics in a separate process

    This is used with :class:`process_runner.ProcessRunner` to run all periodic tasks in a separate process
    """

    def __init__(self, qcf_config: FractalConfig, completed_queue: Optional[multiprocessing.Queue] = None):
        ProcessBase.__init__(self)
        self._qcf_config = qcf_config
        self._completed_queue = completed_queue

        # ---------------------------------------------------------------
        # We should not instantiate the FractalPeriodics class here.
        # The setup and run functions will be run in a separate process
        # and so instantiation should happen there
        # ---------------------------------------------------------------

    def setup(self) -> None:
        self._periodics = FractalPeriodics(self._qcf_config, self._completed_queue)

    def run(self) -> None:
        self._periodics.start()

    def interrupt(self) -> None:
        self._periodics.stop()
