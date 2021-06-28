from __future__ import annotations

import logging
from sqlalchemy import and_
from sqlalchemy.orm import load_only
from qcfractal.interface.models import ObjectId, InsertMetadata, QueryMetadata, RecordStatusEnum
from qcfractal.storage_sockets.models import BaseResultORM, ServiceQueueORM
from qcfractal.storage_sockets.sqlalchemy_socket import calculate_limit

from qcfractal.storage_sockets.sqlalchemy_common import (
    insert_general,
    get_query_proj_columns,
    get_count,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Optional, Sequence, Tuple, Iterable, Any

    ServiceQueueDict = Dict[str, Any]


class ServiceQueueSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._user_limit = core_socket.qcf_config.response_limits.service_queue

    def add_orm(
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

    def get(
        self,
        id: Sequence[str],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ServiceQueueDict]]:
        """Get service queue entries by their IDs

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
            if len(id) > self._user_limit:
                raise RuntimeError(f"Request for {len(id)} services is over the limit of {self._user_limit}")

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

    def query(
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
        base_result_id
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

        limit = calculate_limit(self._user_limit, limit)

        load_cols, _ = get_query_proj_columns(ServiceQueueORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(ServiceQueueORM.id.in_(id))
        if procedure_id is not None:
            and_query.append(ServiceQueueORM.base_result_id.in_(procedure_id))
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
                .join(BaseResultORM, ServiceQueueORM.base_result_id == BaseResultORM.id)
                .filter(and_(*and_query))
                .options(load_only(*load_cols))
            )
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
