from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import and_, update, select

from qcfractal.db_socket.helpers import get_query_proj_options, get_count, get_general
from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerStatusEnum, ManagerName, ManagerQueryFilters
from qcportal.utils import now_at_utc
from .db_models import ComputeManagerORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Iterable, Optional, Sequence, Sequence, Dict, Any


class ManagerSocket:
    """
    Socket for managing/querying compute managers
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self._manager_heartbeat_frequency = root_socket.qcf_config.heartbeat_frequency
        self._manager_heartbeat_frequency_jitter = root_socket.qcf_config.heartbeat_frequency_jitter
        self._manager_max_missed_heartbeats = root_socket.qcf_config.heartbeat_max_missed

        with self.root_socket.session_scope() as session:
            self.root_socket.internal_jobs.add(
                "check_manager_heartbeats",
                now_at_utc(),
                "managers.check_manager_heartbeats",
                {},
                user_id=None,
                unique_name=True,
                repeat_delay=self._manager_heartbeat_frequency,
                session=session,
            )

    def activate(
        self,
        name_data: ManagerName,
        manager_version: str,
        username: Optional[str],
        programs: Dict[str, List[str]],
        compute_tags: List[str],
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Activates a new manager on the server
        """

        # Strip out empty tags and programs
        compute_tags = [x.lower() for x in compute_tags if len(x) > 0]

        # Some version strings can contain uppercase characters
        programs = {k.lower(): [v.lower() for v in vlst] for k, vlst in programs.items() if len(k) > 0}

        if len(compute_tags) == 0:
            raise ComputeManagerError("Manager does not have any tags assigned. Use '*' to match all tags")
        if len(programs) == 0:
            raise ComputeManagerError("Manager does not have any programs available")

        compute_tags = list(dict.fromkeys(compute_tags))  # remove duplicates, maintaining order (in python 3.6+)

        manager_orm = ComputeManagerORM(
            name=name_data.fullname,
            cluster=name_data.cluster,
            hostname=name_data.hostname,
            username=username,
            compute_tags=compute_tags,
            status=ManagerStatusEnum.active,
            manager_version=manager_version,
            programs=programs,
        )

        with self.root_socket.optional_session(session) as session:
            stmt = select(ComputeManagerORM).where(ComputeManagerORM.name == name_data.fullname)
            count = get_count(session, stmt)

            if count > 0:
                self._logger.warning(f"Cannot activate duplicate manager: {name_data.fullname}")
                raise ComputeManagerError("A manager with this name already exists")

            session.add(manager_orm)
            session.flush()
            return manager_orm.id

    def update_resource_stats(
        self,
        name: str,
        active_tasks: int,
        active_cores: int,
        active_memory: float,
        total_cpu_hours: float,
        *,
        session: Optional[Session] = None,
    ):
        """
        Updates the resources available/in use by a manager
        """

        with self.root_socket.optional_session(session) as session:
            stmt = select(ComputeManagerORM).where(ComputeManagerORM.name == name).with_for_update(skip_locked=False)
            manager: Optional[ComputeManagerORM] = session.execute(stmt).scalar_one_or_none()

            if manager is None:
                raise ComputeManagerError(f"Cannot update resource stats for manager {name} - does not exist")
            if manager.status != ManagerStatusEnum.active:
                raise ComputeManagerError(f"Cannot update resource stats for manager {name} - is not active")

            manager.active_tasks = active_tasks
            manager.active_cores = active_cores
            manager.active_memory = active_memory
            manager.total_cpu_hours = total_cpu_hours
            manager.modified_on = now_at_utc()

    def deactivate(
        self,
        name: Optional[Iterable[str]] = None,
        modified_before: Optional[datetime] = None,
        *,
        reason: str = "(none given)",
        session: Optional[Session] = None,
    ) -> List[str]:
        """Marks managers as inactive

        If both name and modified_before are specified, managers that match both conditions will be deactivated.

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
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of manager names that were marked as inactive

        """
        if not name and not modified_before:
            return []

        now = now_at_utc()
        query_and = []
        if name:
            query_and.append(ComputeManagerORM.name.in_(name))
        if modified_before:
            query_and.append(ComputeManagerORM.modified_on < modified_before)

        stmt = (
            update(ComputeManagerORM)
            .where(and_(ComputeManagerORM.status == ManagerStatusEnum.active, and_(True, *query_and)))
            .values(status=ManagerStatusEnum.inactive, modified_on=now)
            .returning(ComputeManagerORM.name)
        )

        with self.root_socket.optional_session(session) as session:
            deactivated_names = session.execute(stmt).fetchall()
            deactivated_names = [x[0] for x in deactivated_names]

            # For the manager, also reset any now-orphaned tasks that belonged to that manager
            for dead_name in deactivated_names:
                incomplete_ids = self.root_socket.records.reset_assigned(manager_name=[dead_name], session=session)
                self._logger.info(
                    f"Deactivated manager {dead_name}. Reason: {reason}. Recycling {len(incomplete_ids)} incomplete tasks."
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
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Obtain manager information with specified names from the database

        Names for managers are unique, since they include a UUID.

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
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            List of manager data (as dictionaries) in the same order as the given names.
            If missing_ok is True, then this list will contain None where the manager was missing.
        """

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, ComputeManagerORM, ComputeManagerORM.name, name, include, exclude, missing_ok)

    def query(
        self,
        query_data: ManagerQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        General query of managers in the database

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        query_data
            Fields/filters to query for
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of manager info (as dictionaries) that were found in the database.
        """

        proj_options = get_query_proj_options(ComputeManagerORM, query_data.include, query_data.exclude)

        and_query = []
        if query_data.manager_id is not None:
            and_query.append(ComputeManagerORM.id.in_(query_data.manager_id))
        if query_data.name is not None:
            and_query.append(ComputeManagerORM.name.in_(query_data.name))
        if query_data.hostname is not None:
            and_query.append(ComputeManagerORM.hostname.in_(query_data.hostname))
        if query_data.cluster is not None:
            and_query.append(ComputeManagerORM.cluster.in_(query_data.cluster))
        if query_data.status is not None:
            and_query.append(ComputeManagerORM.status.in_(query_data.status))
        if query_data.modified_before is not None:
            and_query.append(ComputeManagerORM.modified_on < query_data.modified_before)
        if query_data.modified_after is not None:
            and_query.append(ComputeManagerORM.modified_on > query_data.modified_after)

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(ComputeManagerORM).filter(and_(True, *and_query))
            stmt = stmt.options(*proj_options)

            if query_data.cursor is not None:
                stmt = stmt.where(ComputeManagerORM.id < query_data.cursor)

            stmt = stmt.order_by(ComputeManagerORM.id.desc())
            stmt = stmt.limit(query_data.limit)
            stmt = stmt.distinct(ComputeManagerORM.id)

            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in results]

        return result_dicts

    def check_manager_heartbeats(self, session: Session) -> None:
        """
        Checks for manager heartbeats

        If a manager has not been heard from in a while, it is set to inactivate and its tasks
        reset to a waiting state. The amount of time to wait for a manager is controlled by the config
        options manager_max_missed_heartbeats and manager_heartbeat_frequency.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use.
        """
        self._logger.debug("Checking manager heartbeats")

        # Take into account the maximum jitter allowed
        manager_window = self._manager_max_missed_heartbeats * (
            self._manager_heartbeat_frequency * (1 + self._manager_heartbeat_frequency_jitter)
        )
        dt = now_at_utc() - timedelta(seconds=manager_window)

        dead_managers = self.deactivate(modified_before=dt, reason="missing heartbeat", session=session)

        if dead_managers:
            self._logger.info(f"Deactivated {len(dead_managers)} managers due to missing heartbeats")
