"""
Classes for handling periodic tasks required by QCFractal

QCFractal requires some periodic maintenance. These include updating services, pruning
dead managers, and updating statistics.
"""

from __future__ import annotations
import traceback
import logging
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_STOPPED
from qcfractal.interface.models import ManagerStatusEnum, TaskStatusEnum, ComputeError
from .storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from .process_runner import ProcessBase

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import multiprocessing
    from typing import Optional
    from .config import FractalConfig


from .services import construct_service


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
        self.scheduler = BackgroundScheduler()

        self._completed_queue = completed_queue
        self.storage_socket.set_completed_watch(self._completed_queue)

        self.logger = logging.getLogger("qcfractal_periodics")

        # Frequencies/Intervals at which to run various tasks
        self.server_stats_frequency = 60
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

        # Set up the typical periodics
        # 1.) Updating the overall server information (counts, etc)
        #     This is stored in a public info class
        self.logger.info("Initializing QCFractal Periodics")
        self.scheduler.add_job(self._update_server_stats, "interval", seconds=self.server_stats_frequency)

        # 2.) Manager heartbeats
        self.scheduler.add_job(self._check_manager_heartbeats, "interval", seconds=self.manager_heartbeat_frequency)

        # 3.) Service updates
        self.scheduler.add_job(self._update_services, "interval", seconds=self.service_frequency)

    def _update_server_stats(self) -> None:
        """
        Updates various server statistics (number of results, etc)
        """
        self.logger.info("Updating server stats in the database")
        self.storage_socket.log_server_stats()

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
        ret = self.storage_socket.get_managers(status=ManagerStatusEnum.active, modified_before=dt)

        for manager in ret["data"]:
            name = manager["name"]
            # For each manager, reset any orphaned tasks that belong to that manager
            # These are stored as 'returned' in the manager info table

            n_incomplete = self.storage_socket.queue_reset_status(manager=name, reset_running=True)
            self.storage_socket.manager_update(name, returned=n_incomplete, status=ManagerStatusEnum.inactive)

            self.logger.info("Hearbeat missing from {}. Recycling {} incomplete tasks.".format(name, n_incomplete))

    def _update_services(self) -> int:
        """Runs through all active services and examines their current status

        This will check all services to see if they require another iteration, and if so, perform that iteration.
        It will also check for errors.

        If new services are waiting, they will also be started as long as the total number of services
        are under the limit given by max_active_services
        """

        # Grab current services
        current_services = self.storage_socket.get_services(status=TaskStatusEnum.running)["data"]

        # Grab new services if we have open slots
        open_slots = max(0, self.max_active_services - len(current_services))
        if open_slots > 0:
            new_services = self.storage_socket.get_services(status=TaskStatusEnum.waiting, limit=open_slots)["data"]
            current_services.extend(new_services)
            if len(new_services):
                self.logger.info(f"Starting {len(new_services)} new services.")

        self.logger.debug(f"Updating {len(current_services)} services.")

        # Loop over the services and iterate
        running_services = 0
        completed_services = []
        for data in current_services:

            # TODO HACK: remove task_id from 'output'. This is contained in services
            # created in previous versions. Doing this now, but should do a db migration
            # at some point
            if "output" in data:
                data["output"].pop("task_id", None)

            # Attempt to iteration and get message
            service = None
            try:
                service = construct_service(self.storage_socket, data)
            except Exception:
                error_message = "Error constructing service. This is pretty bad!:\n{}".format(traceback.format_exc())
                self.logger.critical(error_message)
                continue

            try:
                finished = service.iterate()
            except Exception:
                error_message = "Error iterating service with id={}:\n{}".format(service.id, traceback.format_exc())
                self.logger.error(error_message)
                service.status = "ERROR"
                service.error = ComputeError(error_type="iteration_error", error_message=error_message)
                self.storage_socket.update_service_status("ERROR", id=service.id)
                finished = False

            self.storage_socket.update_services([service])

            if finished is not False:
                # Add results to procedures, remove complete_ids
                completed_services.append(service)
            else:
                running_services += 1

        if len(completed_services):
            self.logger.info(f"Completed {len(completed_services)} services.")

        self.logger.debug(f"Done updating services.")

        # Add new procedures and services
        self.storage_socket.services_completed(completed_services)

        return running_services

    def start(self) -> None:
        """
        Start running the periodic tasks in the background

        This function will immediately return, with the tasks now running periodically in the background
        """

        self.scheduler.start()

    def stop(self) -> None:
        """
        Stop running the periodic tasks

        This will stop the tasks that are running in the background. Currently running tasks will
        be allowed to finish.
        """
        self.logger.info("Shutting down periodics")
        if self.scheduler.state != STATE_STOPPED:
            self.scheduler.shutdown(wait=True)

    def __del__(self):
        self.stop()


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
        # The .run() function will be run in a separate process
        # and so instantiation should happen there
        # ---------------------------------------------------------------

    def run(self) -> None:
        self.periodics = FractalPeriodics(self._qcf_config, self._completed_queue)
        self.periodics.start()

        # Periodics are now running in the background. But we need to keep this process alive
        while True:
            time.sleep(3600)

    def finalize(self) -> None:
        if self.periodics:
            self.periodics.stop()
