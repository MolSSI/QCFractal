from __future__ import annotations

import logging

from sqlalchemy.orm import selectinload, contains_eager
from qcfractal.interface.models import (
    ObjectId,
    AllServiceSpecifications,
    InsertMetadata,
    RecordStatusEnum,
)
from qcfractal.storage_sockets.models import BaseResultORM, ServiceQueueORM

from typing import TYPE_CHECKING

from .services import BaseServiceHandler, TorsionDriveHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Optional, Sequence, Tuple


class ServiceSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._max_active_services = core_socket.qcf_config.max_active_services

        self.torsiondrive = TorsionDriveHandler(core_socket)

        self.handler_map: Dict[str, BaseServiceHandler] = {
            "torsiondrive": self.torsiondrive,
        }

    def create(
        self, specifications: Sequence[AllServiceSpecifications], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[Optional[ObjectId]]]:

        # TODO - error handling
        inserted_idx = []
        existing_idx = []
        errors = []
        ids = []

        with self._core_socket.optional_session(session) as session:
            for idx, specification in enumerate(specifications):
                service_handler = self.handler_map[specification.procedure]

                # Verify the service input
                # TODO - error handling
                service_handler.verify_input(specification)

                # Right now we are only giving this one at a time. But
                # I am leaving open the possibility of doing multiple at some point
                # hence a bit of awkwardness with lists that should have a length of one
                meta, new_ids = service_handler.create_records(session, specification)
                ids.extend(new_ids)

                if not meta.success:
                    errors.append((idx, meta.errors[0][1]))
                if meta.n_existing > 0:
                    existing_idx.append(idx)
                else:
                    inserted_idx.append(idx)

                    new_orm = (
                        session.query(BaseResultORM)
                        .filter(BaseResultORM.id.in_(new_ids))
                        .options(selectinload("*"))
                        .all()
                    )

                    assert all(x.procedure == specification.procedure for x in new_orm)
                    service_meta, _ = service_handler.create_services(
                        session, new_orm, specification.tag, specification.priority
                    )

                    if not service_meta.success:
                        # Should always succeed. This is a programmer error
                        # This will rollback the session
                        raise RuntimeError(f"Error adding services: {service_meta.error_string}")

                session.commit()

        return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors), ids  # type: ignore

    @staticmethod
    def tasks_done(service_orm: ServiceQueueORM) -> Tuple[bool, bool]:
        """
        Check if requested tasks are complete and successful.

        Parameters
        ----------
        service_orm
            Service to check to see if tasks are finished

        Returns
        -------
        :
            The first bool indicates whether all tasks have ended, the second
            indicates whether they were all successful. If the first is False, then
            the second will always be False
        """

        tasks = service_orm.tasks_obj

        if len(tasks) == 0:
            return True, True

        status_values = set(x.procedure_obj.status for x in tasks)

        # Waiting or running = not finished
        if RecordStatusEnum.waiting in status_values:
            return False, False
        elif RecordStatusEnum.running in status_values:
            return False, False
        else:
            # If all are complete, then all are successful
            if all(x == RecordStatusEnum.complete for x in status_values):
                return True, True
            else:
                # Something is not "complete" (successful)
                return True, False

    def iterate_services(self, *, session: Optional[Session] = None) -> int:

        with self._core_socket.optional_session(session) as session:

            # We do a plain .join() because we are querying, and then also supplying contains_eager() so that
            # the ServiceQueueORM.procedure_obj gets populated
            # See https://docs-sqlalchemy.readthedocs.io/ko/latest/orm/loading_relationships.html#routing-explicit-joins-statements-into-eagerly-loaded-collections
            services = (
                session.query(ServiceQueueORM)
                .join(ServiceQueueORM.procedure_obj)  # Joins a BaseResultORM
                .options(contains_eager(ServiceQueueORM.procedure_obj))
                .filter(BaseResultORM.status == RecordStatusEnum.running)
                .all()
            )

            if len(services) > 0:
                self._logger.info(f"Iterating on {len(services)} running services...")
            else:
                self._logger.info(f"Found {len(services)} running services.")

            for service_orm in services:
                service_type = service_orm.procedure_obj.procedure

                finished, successful = self.tasks_done(service_orm)

                if not finished:
                    continue

                if successful:
                    completed = self.handler_map[service_type].iterate(session, service_orm)

                    # If the service has successfully completed, delete the entry from the Service Queue
                    if completed:
                        session.delete(service_orm)
                else:
                    # At least one of the tasks was not successful. Therefore, mark the service as an error
                    service_orm.procedure_obj.status = RecordStatusEnum.error

                    error = {
                        "error_type": "service_iteration_error",
                        "error_message": "Some task(s) did not complete successfully",
                    }
                    service_orm.procedure_obj.error = self._core_socket.output_store.replace(
                        service_orm.procedure_obj.error, error, session=session
                    )

                    self._logger.info(
                        f"Service {service_orm.id} marked as errored. Some tasks did not complete successfully"
                    )

                session.commit()

            # Should we start more?
            running_count = (
                session.query(ServiceQueueORM)
                .join(ServiceQueueORM.procedure_obj)  # Joins a BaseResultORM
                .filter(BaseResultORM.status == RecordStatusEnum.running)
                .count()
            )

            self._logger.info(
                f"After iteration, now {running_count} running services. Max is {self._max_active_services}"
            )
            new_service_count = self._max_active_services - running_count

            if new_service_count > 0:
                new_services = (
                    session.query(ServiceQueueORM)
                    .join(ServiceQueueORM.procedure_obj)  # Joins a BaseResultORM
                    .options(contains_eager(ServiceQueueORM.procedure_obj))
                    .filter(BaseResultORM.status == RecordStatusEnum.waiting)
                    .order_by(ServiceQueueORM.priority.desc(), ServiceQueueORM.created_on)
                    .limit(new_service_count)
                    .all()
                )

                running_count += len(new_services)

                if len(new_services) > 0:
                    self._logger.info(f"Attempting to start {len(new_services)} services")

                    for service_orm in new_services:
                        service_type = service_orm.procedure_obj.procedure
                        self.handler_map[service_type].iterate(session, service_orm)

        return running_count
