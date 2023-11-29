from __future__ import annotations

import logging
import traceback
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, or_
from sqlalchemy.dialects.postgresql import array_agg
from sqlalchemy.orm import contains_eager, aliased, defer, selectinload, joinedload

from qcfractal.components.record_db_models import BaseRecordORM, RecordComputeHistoryORM
from qcfractal.db_socket.helpers import (
    get_count,
)
from qcportal.generic_result import GenericTaskResult
from qcportal.metadata_models import InsertMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.utils import now_at_utc
from .db_models import ServiceQueueORM, ServiceDependencyORM, ServiceSubtaskRecordORM
from ..record_socket import BaseRecordSocket

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.components.internal_jobs.status import JobProgress
    from typing import List, Dict, Tuple, Optional, Any, Union


class ServiceSocket:
    """
    Socket for managing services
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._max_active_services = root_socket.qcf_config.max_active_services
        self._service_frequency = root_socket.qcf_config.service_frequency

        # Add the initial job for iterating the service
        self.add_internal_job_iterate_services(0.0)

    def add_internal_job_iterate_services(self, delay: float, *, session: Optional[Session] = None):
        """
        Adds an internal job to check/update the services

        Parameters
        ----------
        delay
            Schedule for this many seconds in the future
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """
        with self.root_socket.optional_session(session) as session:
            self.root_socket.internal_jobs.add(
                "iterate_services",
                now_at_utc() + timedelta(seconds=delay),
                "services.iterate_services",
                {},
                user_id=None,
                unique_name=True,
                after_function="services.add_internal_job_iterate_services",
                after_function_kwargs={"delay": self._service_frequency},
                session=session,
            )

    def mark_service_complete(self, session: Session, service_orm: ServiceQueueORM):
        # If the service has successfully completed, delete the entry from the Service Queue
        self._logger.info(f"Record {service_orm.record_id} (service {service_orm.id}) has successfully completed!")
        service_orm.record.compute_history[-1].status = RecordStatusEnum.complete
        service_orm.record.compute_history[-1].modified_on = now_at_utc()
        service_orm.record.status = RecordStatusEnum.complete
        service_orm.record.modified_on = now_at_utc()
        session.delete(service_orm)

        session.commit()
        self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.complete)

    def _iterate_service(self, session: Session, job_progress: JobProgress, service_id: int) -> bool:
        """
        Iterate a single service given its service id

        Parameters
        -----------
        session
            An existing SQLAlchemy session to use. This session will be committed at the end of this function
        job_progress
            An object used to report the current job progress and status
        service_id
            ID of the service to iterate (not the record ID)

        Returns
        -------
        :
            True if the service has completely finished, false if there are still more iterations
        """

        stmt = select(ServiceQueueORM)
        stmt = stmt.options(selectinload(ServiceQueueORM.record))
        stmt = stmt.options(selectinload(ServiceQueueORM.dependencies))
        stmt = stmt.where(ServiceQueueORM.id == service_id)
        stmt = stmt.with_for_update()

        service_orm = session.execute(stmt).scalar_one_or_none()

        if service_orm is None:
            self._logger.warning(f"Service {service_id} does not exist anymore!")
            return True

        # All tasks successfully completed?
        # Since this is done asynchronously, then something could have happened
        # between the creation of the internal job and this function call (invalidated, etc)
        all_status = {x.record.status for x in service_orm.dependencies}
        if all_status != {RecordStatusEnum.complete} and all_status != set():
            self._logger.info(
                f"Record {service_orm.record_id} (service {service_orm.id}) does NOT have all tasks completed. Ignoring..."
            )
            return False

        # Call record-dependent iterate service
        # If that function returns 0, indicating that the service has successfully completed
        # Handle cleanup if there is an error
        try:
            self._logger.debug(
                f"Record {service_orm.record_id} (service {service_orm.id}) has all tasks completed. Iterating..."
            )
            completed = self.root_socket.records.iterate_service(session, service_orm)
            service_orm.record.modified_on = now_at_utc()
        except Exception as err:
            session.rollback()

            error = {
                "error_type": "service_iteration_error",
                "error_message": "Error iterating service: " + str(err) + "\n" + traceback.format_exc(),
            }

            self.root_socket.records.update_failed_service(session, service_orm.record, error)
            session.commit()
            self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.error)

            return True

        if completed:
            # Will commit inside this function
            self.mark_service_complete(session, service_orm)
            return True
        else:
            # Commit the changes
            session.commit()
            return False

    def iterate_services(self, session: Session, job_progress: JobProgress) -> int:
        """
        Check for services that have their dependencies finished, and then either queue them for iteration
        or mark them as errored

        This function will search for services that have all their dependencies finished. If any of the
        dependencies is errored, it will mark the service as errored. Otherwise, it will submit a job
        to the internal job queue to iterate the service.

        After that, this function will start new services if needed.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. This session will be periodically committed
        job_progress
            An object used to report the current job progress and status

        Returns
        -------
        :
            Number of services currently running after this function is done
        """

        self._logger.info("Iterating on services")

        #
        # A CTE that contains just service id and all the statuses as an array
        #

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

        #########################
        # First, errored services
        #########################

        # Find only those that all tasks are completed or errored, but only those
        # with at least one error
        stmt = (
            select(ServiceQueueORM)
            .options(defer(ServiceQueueORM.service_state))  # could be large, but is not needed
            .options(joinedload(ServiceQueueORM.record))
            .join(status_cte, status_cte.c.service_id == ServiceQueueORM.id)
            .where(status_cte.c.task_statuses.contained_by(["complete", "error"]))
            .where(status_cte.c.task_statuses.contains(["error"]))
        )

        err_services = session.execute(stmt).scalars().all()

        if len(err_services) > 0:
            self._logger.info(f"Found {len(err_services)} running services with task failures")

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

        ###########################
        # Now successful services
        ###########################

        # Services whose tasks this iteration are all successfully completed
        # Service is ready for the next iteration. We only need the ID
        stmt = (
            select(ServiceQueueORM.id)
            .join(status_cte, status_cte.c.service_id == ServiceQueueORM.id)
            .where(or_(status_cte.c.task_statuses.contained_by(["complete"]), status_cte.c.task_statuses == []))
        )

        service_ids = session.execute(stmt).scalars().all()

        # Add an internal job for each completed service, calling the internal function
        for service_id in service_ids:
            jobname = f"iterate_service_{service_id}"
            job_id = self.root_socket.internal_jobs.add(
                name=jobname,
                scheduled_date=now_at_utc(),
                unique_name=True,
                function="services._iterate_service",
                kwargs={"service_id": service_id},
                user_id=None,
                session=session,
            )
            self._logger.debug(f"Internal job {job_id} for service {service_id} queued")

            # Commit after each one to allow it to be picked up by an internal job worker
            session.commit()

        #
        # How many services are currently running
        #
        stmt = select(BaseRecordORM).where(
            BaseRecordORM.status == RecordStatusEnum.running, BaseRecordORM.is_service.is_(True)
        )
        running_count = get_count(session, stmt)

        self._logger.info(f"After iteration, now {running_count} running services. Max is {self._max_active_services}")

        while True:
            # Start up to 10 services at a time until we are full
            new_service_count = min(10, self._max_active_services - running_count)

            # we could possibly have a negative number here if the max active services was lowered or
            # something weird was done manually, services restarted, etc
            if new_service_count <= 0:
                break

            stmt = (
                select(ServiceQueueORM)
                .join(ServiceQueueORM.record)
                .options(contains_eager(ServiceQueueORM.record))
                .filter(BaseRecordORM.status == RecordStatusEnum.waiting)
                .order_by(ServiceQueueORM.priority.desc(), BaseRecordORM.created_on)
                .limit(new_service_count)
            )

            new_services = session.execute(stmt).scalars().all()

            # No more services available to be started
            if len(new_services) == 0:
                break

            running_count += len(new_services)

            for service_orm in new_services:
                now = now_at_utc()
                service_orm.record.modified_on = now
                service_orm.record.status = RecordStatusEnum.running

                # Has this service been started before? ie, the service was restarted or something
                fresh_start = len(service_orm.dependencies) == 0 and (
                    service_orm.service_state == {} or service_orm.service_state is None
                )

                existing_history = service_orm.record.compute_history
                if len(existing_history) == 0:
                    # Add a compute history entry.
                    # The iterate functions expect that at least one history entry exists
                    # But only add if this wasn't a restart of a running service

                    hist = RecordComputeHistoryORM()
                    hist.status = RecordStatusEnum.running
                    hist.modified_on = now

                    stdout_str = f"Starting service: {service_orm.record.record_type} at {now}"
                    stdout = self.root_socket.records.create_output_orm(session, OutputTypeEnum.stdout, stdout_str)
                    hist.outputs[OutputTypeEnum.stdout] = stdout

                    service_orm.record.compute_history.append(hist)

                else:  # this was (probably) a restart
                    self.root_socket.records.append_output(
                        session,
                        service_orm.record,
                        OutputTypeEnum.stdout,
                        f"\nRestarting service: {service_orm.record.record_type} at {now}",
                    )

                session.commit()

                try:
                    if fresh_start:
                        self.root_socket.records.initialize_service(session, service_orm)

                        jobname = f"iterate_service_{service_orm.id}"
                        job_id = self.root_socket.internal_jobs.add(
                            name=jobname,
                            scheduled_date=now_at_utc(),
                            unique_name=True,
                            function="services._iterate_service",
                            kwargs={"service_id": service_orm.id},
                            user_id=None,
                            session=session,
                        )
                        self._logger.debug(
                            f"Internal job {job_id} for service {service_orm.id} queued - first iteration"
                        )
                        session.commit()

                except Exception as err:
                    session.rollback()

                    import traceback

                    traceback.print_exc()
                    error = {
                        "error_type": "service_initialization_error",
                        "error_message": "Error in initialization/iteration of service: " + str(err),
                    }

                    self.root_socket.records.update_failed_service(session, service_orm.record, error)
                    session.commit()
                    self.root_socket.notify_finished_watch(service_orm.record_id, RecordStatusEnum.error)

        return running_count


class ServiceSubtaskRecordSocket(BaseRecordSocket):
    """
    Socket for handling distributed service generic subtasks
    """

    # Used by the base class
    record_orm = ServiceSubtaskRecordORM

    def __init__(self, root_socket: SQLAlchemySocket):
        BaseRecordSocket.__init__(self, root_socket)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def get_children_select() -> List[Any]:
        return []

    def generate_task_specification(self, record_orm: ServiceSubtaskRecordORM) -> Dict[str, Any]:
        # Normally, this function is a little more complicated (ie, for others the spec is
        # generated from data in the record). However, this record type is a pretty
        # transparent passthrough. The function and kwargs stored in the record, so just return them
        return {"function": record_orm.function, "function_kwargs": record_orm.function_kwargs}

    def update_completed_task(
        self, session: Session, record_orm: ServiceSubtaskRecordORM, result: GenericTaskResult, manager_name: str
    ) -> None:
        record_orm.results = result.results

    def add(
        self,
        required_programs: Dict[str, Any],
        function: str,
        function_kwargs: List[Dict[str, Any]],
        tag: str,
        priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertMetadata, List[Optional[int]]]:
        """
        Adds new service iteration records

        No duplicate checking is done, so records will always be added

        Parameters
        ----------
        required_programs
            Programs & versions required on the worker
        function
            The compute function to run on the worker
        function_kwargs
            Keyword arguments passed to the compute function. One record will be added for
            each dictionary in the list.
        tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        priority
            The priority for the computation
        owner_user
            Name or ID of the user who owns the record
        owner_group
            Group with additional permission for these records
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and a list of record ids. The ids will be in the
            order of the function_kwargs list
        """

        with self.root_socket.optional_session(session, False) as session:
            owner_user_id, owner_group_id = self.root_socket.users.get_owner_ids(
                owner_user, owner_group, session=session
            )
            self.root_socket.users.assert_group_member(owner_user_id, owner_group_id, session=session)

            all_orm = []

            for kw in function_kwargs:
                rec_orm = ServiceSubtaskRecordORM(
                    is_service=False,
                    function=function,
                    function_kwargs=kw,
                    required_programs=required_programs,
                    status=RecordStatusEnum.waiting,
                    owner_user_id=owner_user_id,
                    owner_group_id=owner_group_id,
                )

                self.create_task(rec_orm, tag, priority)

                all_orm.append(rec_orm)
                session.add(rec_orm)

            session.flush()

            ids = [r.id for r in all_orm]
            meta = InsertMetadata(inserted_idx=list(range(len(function_kwargs))), existing_idx=[])

            return meta, ids
