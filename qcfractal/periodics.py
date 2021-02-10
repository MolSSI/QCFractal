from __future__ import annotations
import traceback
import logging
import logging.handlers
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_STOPPED
from qcfractal.interface.models import ManagerStatusEnum, TaskStatusEnum, ComputeError
from .storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from .fractal_proc import FractalProcessBase

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .config import FractalConfig


from .services import construct_service

class FractalPeriodics:
    def __init__(self, qcf_cfg: FractalConfig):
        self.storage_socket = SQLAlchemySocket()
        self.storage_socket.init(qcf_cfg)
        self.scheduler = BackgroundScheduler()
        self.logger = logging.getLogger("qcfractal_periodics")

        # Frequencies/Intervals at which to run various tasks
        self.server_stats_frequency = 60
        self.manager_heartbeat_frequency = qcf_cfg.heartbeat_frequency
        self.service_frequency = qcf_cfg.service_frequency
        self.max_active_services = qcf_cfg.max_active_services

        self.logger.info("Periodics Information:")
        self.logger.info(f"    Server stats update frequency: {self.server_stats_frequency} seconds")
        self.logger.info(f"      Manager heartbeat frequency: {self.manager_heartbeat_frequency} seconds")
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


    def _update_server_stats(self):
        self.logger.info("Updating server stats in the database")
        self.storage_socket.log_server_stats()

    def _check_manager_heartbeats(self):
        self.logger.info("Checking manager heartbeats")
        dt = datetime.utcnow() - timedelta(seconds=(5*self.manager_heartbeat_frequency))
        ret = self.storage_socket.get_managers(status=ManagerStatusEnum.active, modified_before=dt)

        for manager in ret["data"]:
            name = manager["name"]
            # For each manager, reset any orphaned tasks that belong to that manager
            # These are stored as 'returned' in the manager info table

            n_incomplete = self.storage_socket.queue_reset_status(manager=name, reset_running=True)
            self.storage_socket.manager_update(name, returned=n_incomplete, status=ManagerStatusEnum.inactive)

            self.logger.info(
                "Hearbeat missing from {}. Recycling {} incomplete tasks.".format(
                    name, n_incomplete
                )
            )

    def _update_services(self) -> int:
        """Runs through all active services and examines their current status."""

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
            try:
                service = construct_service(self.storage_socket, data)
                finished = service.iterate()
            except Exception:
                error_message = "FractalServer Service Build and Iterate Error:\n{}".format(traceback.format_exc())
                self.logger.error(error_message)
                service.status = "ERROR"
                service.error = ComputeError(error_type="iteration_error", error_message=error_message)
                finished = False

            self.storage_socket.update_services([service])

            # Mark procedure and service as error
            if service.status == "ERROR":
                self.storage_socket.update_service_status("ERROR", id=service.id)

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

    def start(self):
        self.scheduler.start()

    def stop(self):
        self.logger.info("Shutting down periodics")
        if self.scheduler.state != STATE_STOPPED:
            self.scheduler.shutdown(wait=True)

    def __del__(self):
        self.stop()



class FractalPeriodicsProcess(FractalProcessBase):
    """
    Runs periodics in a separate process
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
    ):
        FractalProcessBase.__init__(self)
        self.config = qcf_config

        # We cannot instantiate this here. The .run() function will be run in a separate process
        # and so instantiation must happen there
        self.periodics = None

    def run(self) -> None:
        self.periodics = FractalPeriodics(self.config)
        self.periodics.start()

        # Periodics are now running in the background. But we need to keep this process alive
        while True:
            time.sleep(3600)

    def finalize(self) -> None:
        if self.periodics:
            self.periodics.stop()
