from __future__ import annotations

import logging

from sqlalchemy import and_
from sqlalchemy.orm import selectinload, load_only, contains_eager
from qcfractal.interface.models import (
    ObjectId,
    AllServiceSpecifications,
    InsertMetadata,
    RecordStatusEnum,
    QueryMetadata,
)
from qcfractal.storage_sockets.models import BaseResultORM, ServiceQueueORM
from qcfractal.storage_sockets.sqlalchemy_socket import calculate_limit

from qcfractal.storage_sockets.sqlalchemy_common import (
    insert_general,
    get_query_proj_columns,
    get_count,
)

from typing import TYPE_CHECKING

from .services import BaseServiceHandler, TorsionDriveHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Optional, Sequence, Tuple, Any, Sequence, Iterable

    ServiceQueueDict = Dict[str, Any]


class ServiceSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._user_service_limit = core_socket.qcf_config.response_limits.service_queue
        self._max_active_services = core_socket.qcf_config.max_active_services

        self.torsiondrive = TorsionDriveHandler(core_socket)

        self.handler_map: Dict[str, BaseServiceHandler] = {
            "torsiondrive": self.torsiondrive,
        }

    def add_task_orm(
        self, services: List[ServiceQueueORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds ServiceQueueORM to the database, taking into account duplicates

        If a service should not be added because the corresponding procedure is already marked
        complete, then that will raise an exception.

        The session is flushed at the end of this function.

        Parameters
        ----------
        services
            ORM objects to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata showing what was added, and a list of returned task ids. These will be in the
            same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
        """

        # Check for incompatible statuses
        base_result_ids = [x.procedure_id for x in services]
        statuses = self._core_socket.record.get(base_result_ids, include=["status"], session=session)

        # TODO - logic will need to be adjusted with new statuses
        # This is an error. These should have been checked before calling this function
        if any(x["status"] == RecordStatusEnum.complete for x in statuses):
            raise RuntimeError(
                "Cannot add ServiceQueueORM for a procedure that is already complete. This is a programmer error"
            )

        with self._core_socket.optional_session(session) as session:
            meta, ids = insert_general(session, services, (ServiceQueueORM.procedure_id,), (ServiceQueueORM.id,))

            return meta, [x[0] for x in ids]

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
                    service_meta, _ = service_handler.create_tasks(
                        session, new_orm, specification.tag, specification.priority
                    )

                    if not service_meta.success:
                        # Should always succeed. This is a programmer error
                        # This will rollback the session
                        raise RuntimeError(f"Error adding services: {service_meta.error_string}")

                session.commit()

        return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors), ids  # type: ignore

    @staticmethod
    def subtasks_done(service_orm: ServiceQueueORM) -> Tuple[bool, bool]:
        """
        Check if the tasks associated with a service are completed and successful

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

                finished, successful = self.subtasks_done(service_orm)

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

    def get_tasks(
        self,
        id: Sequence[str],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ServiceQueueDict]]:
        """Get service task entries by their IDs

        The returned service information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of services will be None.

        Parameters
        ----------
        id
            List of the service Ids in the DB
        include
            Which fields of the task to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing tasks will be tolerated, and the returned list of
           tasks will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            List of the found tasks
        """

        with self._core_socket.optional_session(session, True) as session:
            if len(id) > self._user_service_limit:
                raise RuntimeError(f"Request for {len(id)} services is over the limit of {self._user_service_limit}")

            # TODO - int id
            int_id = [int(x) for x in id]
            unique_ids = list(set(int_id))

            load_cols, _ = get_query_proj_columns(ServiceQueueORM, include, exclude)

            results = (
                session.query(ServiceQueueORM)
                .filter(ServiceQueueORM.id.in_(unique_ids))
                .options(load_only(*load_cols))
                .yield_per(250)
            )
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested task records")

            return ret

    def query_tasks(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        procedure_id: Optional[Iterable[ObjectId]] = None,
        program: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        tag: Optional[Iterable[str]] = None,
        manager: Optional[Iterable[str]] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ServiceQueueDict]]:
        """
        General query of tasks in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        id
            Ids of the task (not result!) to search for
        procedure_id
            The base result ID of the task
        program
            Programs to search for
        status
            The status of the task: 'running', 'waiting', 'error'
        tag
            Tags of the task to search for
        manager
            Search for tasks assigned to given managers
        include
            Which fields of the molecule to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results from the total list of matches. The limit will apply after skipping,
            allowing for pagination.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Dict with keys: data, meta. Data is the objects found
        """

        limit = calculate_limit(self._user_service_limit, limit)

        load_cols, _ = get_query_proj_columns(ServiceQueueORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(ServiceQueueORM.id.in_(id))
        if procedure_id is not None:
            and_query.append(ServiceQueueORM.procedure_id.in_(procedure_id))
        if status is not None:
            and_query.append(BaseResultORM.status.in_(status))
        if tag is not None:
            and_query.append(ServiceQueueORM.tag.in_(tag))
        if manager is not None:
            and_query.append(BaseResultORM.manager_name.in_(manager))
        if program:
            and_query.append(ServiceQueueORM.required_programs.has_any(program))

        with self._core_socket.optional_session(session, True) as session:
            query = (
                session.query(ServiceQueueORM)
                .join(ServiceQueueORM.procedure_obj)
                .filter(and_(*and_query))
                .options(load_only(*load_cols))
            )
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
