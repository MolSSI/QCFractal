from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, or_
from sqlalchemy.dialects.postgresql import array_agg
from sqlalchemy.orm import contains_eager, make_transient, aliased

from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.components.records.db_models import BaseRecordORM, RecordComputeHistoryORM
from qcfractal.db_socket.helpers import (
    get_count,
)
from qcportal.compression import CompressionEnum
from qcportal.outputstore import OutputStore, OutputTypeEnum
from qcportal.records.models import RecordStatusEnum
from .db_models import ServiceQueueORM, ServiceDependencyORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional


class ServiceSocket:
    """
    Socket for managing services
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._max_active_services = root_socket.qcf_config.max_active_services

    def mark_service_complete(self, session: Session, service_orm: ServiceQueueORM):
        # If the service has successfully completed, delete the entry from the Service Queue
        self._logger.info(f"Record {service_orm.record_id} (service {service_orm.id}) has successfully completed!")
        service_orm.record.compute_history[-1].status = RecordStatusEnum.complete
        service_orm.record.compute_history[-1].modified_on = datetime.utcnow()
        service_orm.record.status = RecordStatusEnum.complete
        service_orm.record.modified_on = datetime.utcnow()
        session.delete(service_orm)

        session.commit()
        self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.complete)

    def iterate_services(self, *, session: Optional[Session] = None) -> int:
        """
        Check for services that have their dependencies finished, and iterate or mark them as errored

        This function will search for services that have all their dependencies finished. If any of the
        dependencies is errored, it will mark the service as errored. Otherwise, it will call the
        record-dependent iterate_service function.

        If that function returns 0, indicating that the service has successfully completed, this function
        will then handle the cleanup.

        After that, this function will start new services if needed.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------

        """

        self._logger.info("Iterating on services")
        # A CTE that contains just service id and all the statuses as an array

        # We alias BaseRecord so we can join twice to it
        # once for aggregating the status of the dependencies
        a_br_svc_deps = aliased(BaseRecordORM)

        # And once for filtering by the status of the record corresponding to the service itself
        a_br_svc = aliased(BaseRecordORM)
        status_cte = (
            select(ServiceDependencyORM.service_id, array_agg(a_br_svc_deps.status).label("task_statuses"))
            .join(ServiceQueueORM, ServiceQueueORM.id == ServiceDependencyORM.service_id)
            .join(a_br_svc_deps, a_br_svc_deps.id == ServiceDependencyORM.record_id)
            .join(a_br_svc, a_br_svc.id == ServiceQueueORM.record_id)
            .where(a_br_svc.status == RecordStatusEnum.running)
            .group_by(ServiceDependencyORM.service_id)
            .cte()
        )

        with self.root_socket.optional_session(session) as session:

            # Services where a task has errored
            # Find only those that all tasks are completed or errored, but only those
            # with at least one error
            stmt = (
                select(ServiceQueueORM)
                .join(status_cte, status_cte.c.service_id == ServiceQueueORM.id)
                .join(ServiceQueueORM.record)
                .where(status_cte.c.task_statuses.contained_by(["complete", "error"]))
                .where(status_cte.c.task_statuses.contains(["error"]))
            )

            err_services = session.execute(stmt).scalars().all()

            # Services whose tasks this iteration are all successfully completed
            stmt = (
                select(ServiceQueueORM)
                .join(status_cte, status_cte.c.service_id == ServiceQueueORM.id)
                .join(ServiceQueueORM.record)
                .where(or_(status_cte.c.task_statuses.contained_by(["complete"]), status_cte.c.task_statuses == []))
            )
            completed_services = session.execute(stmt).scalars().all()

            if len(err_services) > 0:
                self._logger.info(f"Found {len(err_services)} running services with task failures")
            if len(completed_services) > 0:
                self._logger.info(f"Found {len(completed_services)} running services with completed tasks")

            # Services with an errored task
            for service_orm in err_services:
                error = {
                    "error_type": "service_iteration_error",
                    "error_message": "Some task(s) did not complete successfully",
                }

                self._logger.info(
                    f"Record {service_orm.record_id} (service {service_orm.id}) has task failures. Marking as errored"
                )

                self.root_socket.records.update_failed_service(session, service_orm.record, error)
                session.commit()

                self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.error)

            # Completed, successful service dependencies. Service is ready for the next iteration
            for service_orm in completed_services:
                try:
                    self._logger.debug(
                        f"Record {service_orm.record_id} (service {service_orm.id}) has all tasks completed. Iterating..."
                    )
                    completed = self.root_socket.records.iterate_service(session, service_orm)
                    service_orm.record.modified_on = datetime.utcnow()
                except Exception as err:
                    session.rollback()

                    error = {
                        "error_type": "service_iteration_error",
                        "error_message": "Error iterating service: " + str(err),
                    }

                    self.root_socket.records.update_failed_service(session, service_orm.record, error)
                    session.commit()
                    self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.error)
                    continue

                if completed:
                    self.mark_service_complete(session, service_orm)

            # Should we start more?
            stmt = select(BaseRecordORM).where(
                BaseRecordORM.status == RecordStatusEnum.running, BaseRecordORM.is_service.is_(True)
            )

            running_count = get_count(session, stmt)

            self._logger.info(
                f"After iteration, now {running_count} running services. Max is {self._max_active_services}"
            )

            new_service_count = self._max_active_services - running_count

            if new_service_count > 0:
                stmt = (
                    select(ServiceQueueORM)
                    .join(ServiceQueueORM.record)
                    .options(contains_eager(ServiceQueueORM.record))
                    .filter(BaseRecordORM.status == RecordStatusEnum.waiting)
                    .order_by(ServiceQueueORM.priority.desc(), ServiceQueueORM.created_on)
                    .limit(new_service_count)
                )

                new_services = session.execute(stmt).scalars().all()

                running_count += len(new_services)

                if len(new_services) > 0:
                    self._logger.info(f"Attempting to start {len(new_services)} services")

                for service_orm in new_services:

                    now = datetime.utcnow()
                    service_orm.record.modified_on = now
                    service_orm.record.status = RecordStatusEnum.running

                    # Has this service been started before? ie, the service was restarted or something
                    fresh_start = len(service_orm.dependencies) == 0 and (
                        service_orm.service_state == {} or service_orm.service_state is None
                    )

                    existing_history = service_orm.record.compute_history
                    if len(existing_history) == 0 or existing_history[-1].status != RecordStatusEnum.running:

                        # Add a compute history entry. The iterate functions expect that at least one
                        # history entry exists
                        # But only add if this wasn't a restart of a running service

                        hist = RecordComputeHistoryORM()
                        hist.status = RecordStatusEnum.running
                        hist.modified_on = now

                        if len(existing_history) == 0:
                            stdout = OutputStore.compress(
                                OutputTypeEnum.stdout,
                                f"Starting service: {service_orm.record.record_type} at {now}",
                                CompressionEnum.lzma,
                                1,
                            )
                            hist.outputs[OutputTypeEnum.stdout] = OutputStoreORM.from_model(stdout)

                        else:  # this was a restart of a not-running (ie, errored) service
                            stdout_orm = service_orm.record.compute_history[-1].get_output(OutputTypeEnum.stdout)
                            make_transient(stdout_orm)
                            stdout_orm.id = None
                            stdout_orm.history_id = None
                            stdout_orm.append(f"\nRestarting service: {service_orm.record.record_type} at {now}")
                            hist.outputs[OutputTypeEnum.stdout] = stdout_orm

                        service_orm.record.compute_history.append(hist)

                    session.commit()

                    try:
                        if fresh_start:
                            self.root_socket.records.initialize_service(session, service_orm)
                            completed = self.root_socket.records.iterate_service(session, service_orm)

                            # Completed on first iteration? Possible if everything was already computed
                            if completed:
                                self.mark_service_complete(session, service_orm)

                    except Exception as err:
                        session.rollback()

                        error = {
                            "error_type": "service_initialization_error",
                            "error_message": "Error in initialization/iteration of service: " + str(err),
                        }

                        self.root_socket.records.update_failed_service(session, service_orm.record, error)
                        session.commit()
                        self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.error)

        return running_count
