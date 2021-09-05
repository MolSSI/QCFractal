from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import or_, and_, update
from sqlalchemy.orm import load_only, selectinload
from qcfractal.interface.models import ObjectId, ManagerStatusEnum, QueryMetadata
from qcfractal.components.managers.db_models import QueueManagerLogORM, QueueManagerORM
from qcfractal.storage_sockets.sqlalchemy_socket import calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import get_query_proj_columns, get_count

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Iterable, Optional, Sequence, Sequence, Dict, Any, Tuple

    ManagerDict = Dict[str, Any]
    ManagerLogDict = Dict[str, Any]


class ManagerSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._manager_limit = core_socket.qcf_config.response_limits.manager
        self._manager_log_limit = core_socket.qcf_config.response_limits.manager_log

    def update(self, name: str, *, session: Optional[Session] = None, **kwargs):
        """
        Updates information for a queue manager in the database

        TODO - needs a bit of work when we get to task and queue management polishing: It's too wishy washy

        Parameters
        ----------
        name
            The name of the manager to update
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        do_log = kwargs.pop("log", False)

        inc_count = {
            # Increment relevant data
            "submitted": QueueManagerORM.submitted + kwargs.pop("submitted", 0),
            "completed": QueueManagerORM.completed + kwargs.pop("completed", 0),
            "returned": QueueManagerORM.returned + kwargs.pop("returned", 0),
            "failures": QueueManagerORM.failures + kwargs.pop("failures", 0),
        }

        upd = {key: kwargs[key] for key in QueueManagerORM.__dict__.keys() if key in kwargs}

        with self._core_socket.optional_session(session) as session:
            manager_query = session.query(QueueManagerORM).filter_by(name=name)
            if manager_query.count() > 0:  # existing
                upd.update(inc_count, modified_on=datetime.utcnow())
                num_updated = manager_query.update(upd)
            else:
                # Manager did not exist, so create it
                manager = QueueManagerORM(name=name, **upd)  # type: ignore
                session.add(manager)
                session.flush()
                num_updated = 1

            if do_log:
                # Pull again in case it was updated
                manager = session.query(QueueManagerORM).filter_by(name=name).first()

                manager_log = QueueManagerLogORM(
                    manager_id=manager.id,
                    completed=manager.completed,
                    submitted=manager.submitted,
                    failures=manager.failures,
                    total_worker_walltime=manager.total_worker_walltime,
                    total_task_walltime=manager.total_task_walltime,
                    active_tasks=manager.active_tasks,
                    active_cores=manager.active_cores,
                    active_memory=manager.active_memory,
                )

                session.add(manager_log)

        return num_updated == 1

    def deactivate(
        self,
        name: Optional[Iterable[str]] = None,
        modified_before: Optional[datetime] = None,
        *,
        reason: str = "(none given)",
        session: Optional[Session] = None,
    ) -> List[str]:
        """Marks managers as inactive

        Parameters
        ----------
        name
            Names of managers to mark as inactive
        modified_before
            Mark all managers that were last modified before this date as inactive
        reason
            A descriptive reason given for deactivation
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            A list of manager names that were marked as inactive

        """
        if not name and not modified_before:
            return []

        now = datetime.now()
        query_or = []
        if name:
            query_or.append(QueueManagerORM.name.in_(name))
        if modified_before:
            query_or.append(QueueManagerORM.modified_on < modified_before)

        stmt = (
            update(QueueManagerORM)
            .where((QueueManagerORM.status == ManagerStatusEnum.active) & (or_(*query_or)))
            .values(status=ManagerStatusEnum.inactive, modified_on=now)
            .returning(QueueManagerORM.name)
        )

        with self._core_socket.optional_session(session) as session:
            deactivated_names = session.execute(stmt).fetchall()
            deactivated_names = [x[0] for x in deactivated_names]

            # For the manager, also reset any orphaned tasks that belong to that manager
            # (could also do this in one call to reset_status, but without logging n_incomplete)
            for dead_name in deactivated_names:
                n_incomplete = self._core_socket.procedure.reset_tasks(
                    manager=[dead_name], reset_running=True, session=session
                )
                self._logger.info(
                    f"Deactivated manager {dead_name}. Reason: {reason}. Recycling {n_incomplete} incomplete tasks."
                )

        return deactivated_names

    def get(
        self,
        name: Sequence[str],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ManagerDict]]:
        """
        Obtain manager information from specified names frm an existing session

        Names for managers are unique, since they include a UUID.

        The returned molecule ORMs will be in order of the given names

        If missing_ok is False, then any manager names that are missing in the database will raise an exception.
        Otherwise, the corresponding entry in the returned list of manager info will be None.

        Parameters
        ----------
        name
            A list or other sequence of manager names
        include
            Which fields of the manager info to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing managers will be tolerated, and the returned list of
           managers will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Manager information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the manager was missing.
        """

        if len(name) > self._manager_limit:
            raise RuntimeError(f"Request for {len(name)} managers is over the limit of {self._manager_limit}")

        unique_names = list(set(name))
        load_cols, load_rels = get_query_proj_columns(QueueManagerORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = (
                session.query(QueueManagerORM)
                .filter(QueueManagerORM.name.in_(unique_names))
                .options(load_only(*load_cols))
            )

            for r in load_rels:
                query = query.options(selectinload(r))

            results = query.yield_per(500)
            result_map = {r.name: r.dict() for r in results}

        # Put into the requested order
        ret = [result_map.get(x, None) for x in name]

        if missing_ok is False and None in ret:
            raise RuntimeError("Could not find all requested manager records")

        return ret

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        name: Optional[Iterable[str]] = None,
        hostname: Optional[Iterable[str]] = None,
        cluster: Optional[Iterable[str]] = None,
        status: Optional[Iterable[ManagerStatusEnum]] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ManagerDict]]:
        """
        General query of managers in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        id
            Query for managers its ID
        name
            Query for managers based on manager name
        hostname
            Query for managers based on hostname
        cluster
            Query for managers based on cluster name
        status
            Query for managers based on status (active, inactive)
        modified_before
            Query for managers that were last modified before a specific time
        modified_after
            Query for managers that were last modified before a specific time
        include
            Which fields of the manager to return. Default is to return all fields.
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
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        limit = calculate_limit(self._manager_limit, limit)

        load_cols, _ = get_query_proj_columns(QueueManagerORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(QueueManagerORM.id.in_(id))
        if name is not None:
            and_query.append(QueueManagerORM.name.in_(name))
        if hostname is not None:
            and_query.append(QueueManagerORM.hostname.in_(hostname))
        if cluster is not None:
            and_query.append(QueueManagerORM.cluster.in_(cluster))
        if status is not None:
            and_query.append(QueueManagerORM.status.in_(status))
        if modified_before is not None:
            and_query.append(QueueManagerORM.modified_on < modified_before)
        if modified_after is not None:
            and_query.append(QueueManagerORM.modified_on > modified_after)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(QueueManagerORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def query_logs(
        self,
        manager_id: Iterable[ObjectId],
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit=None,
        skip=0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ManagerLogDict]]:
        """
        General query of managers in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        manager_id
            Query for log entries with these manager IDs
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        include
            Which fields of the manager log to return. Default is to return all fields.
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
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        limit = calculate_limit(self._manager_log_limit, limit)

        and_query = [QueueManagerLogORM.manager_id.in_(manager_id)]
        if before is not None:
            and_query.append(QueueManagerLogORM.timestamp < before)
        if after is not None:
            and_query.append(QueueManagerLogORM.timestamp > after)

        load_cols, _ = get_query_proj_columns(QueueManagerLogORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(QueueManagerLogORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
