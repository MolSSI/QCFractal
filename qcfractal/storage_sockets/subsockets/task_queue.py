from __future__ import annotations

import logging
from sqlalchemy import and_
from sqlalchemy.orm import load_only, contains_eager
from datetime import datetime as dt
from qcfractal.storage_sockets.models import BaseResultORM, TaskQueueORM
from qcfractal.storage_sockets.sqlalchemy_socket import calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import (
    insert_general,
    get_query_proj_columns,
    get_count,
)
from qcfractal.interface.models import RecordStatusEnum, ManagerStatusEnum, ObjectId
from qcfractal.interface.models.query_meta import InsertMetadata, QueryMetadata

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import List, Dict, Union, Optional, Tuple, Sequence, Iterable, Any

    TaskDict = Dict[str, Any]


class TaskQueueSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._user_limit = core_socket.qcf_config.response_limits.task_queue
        self._manager_limit = core_socket.qcf_config.response_limits.manager_task

    def add_orm(
        self, tasks: List[TaskQueueORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds TaskQueueORM to the database, taking into account duplicates

        If a task should not be added because the corresponding procedure is already marked
        complete, then that will raise an exception.

        The session is flushed at the end of this function.

        Parameters
        ----------
        tasks
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
        base_result_ids = [x.base_result_id for x in tasks]
        statuses = self._core_socket.procedure.get(base_result_ids, include=["status"], session=session)

        # This is an error. These should have been checked before calling this function
        if any(x["status"] == RecordStatusEnum.complete for x in statuses):
            raise RuntimeError(
                "Cannot add TaskQueueORM for a procedure that is already complete. This is a programmer error"
            )

        with self._core_socket.optional_session(session) as session:
            meta, ids = insert_general(session, tasks, (TaskQueueORM.base_result_id,), (TaskQueueORM.id,))
            return meta, [x[0] for x in ids]

    def get(
        self,
        id: Sequence[str],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[TaskDict]]:
        """Get tasks by their IDs

        The returned task information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of tasks will be None.

        Parameters
        ----------
        id
            List of the task Ids in the DB
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
                raise RuntimeError(f"Request for {len(id)} tasks is over the limit of {self._user_limit}")

            # TODO - int id
            int_id = [int(x) for x in id]
            unique_ids = list(set(int_id))

            load_cols, _ = get_query_proj_columns(TaskQueueORM, include, exclude)

            results = (
                session.query(TaskQueueORM)
                .filter(TaskQueueORM.id.in_(unique_ids))
                .options(load_only(*load_cols))
                .yield_per(250)
            )
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested task records")

            return ret

    def claim(
        self,
        manager_name: str,
        available_programs: Dict[str, Optional[str]],
        limit: Optional[int] = None,
        tag: Sequence[str] = None,
        *,
        session: Optional[Session] = None,
    ) -> List[TaskDict]:
        """Claim/assign tasks for a manager

        Given tags and available programs/procedures on the manager, obtain
        waiting tasks to run.

        Parameters
        ----------
        manager_name
            Name of the manager requesting tasks
        available_programs
            Dictionary of program name -> version. This includes all programs and procedures available.
        limit
            Maximum number of tasks that the manager can claim
        tag
            List or other sequence of tags (with earlier tags taking preference over later tags)
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        limit = calculate_limit(self._manager_limit, limit)

        tag_queries = []
        if tag is not None:
            for t in tag:
                tag_queries.append((TaskQueueORM.tag == t,))
        else:
            tag_queries = [()]

        with self._core_socket.optional_session(session) as session:
            manager = self._core_socket.manager.get(
                [manager_name], include=["status"], missing_ok=True, session=session
            )
            if manager[0] is None:
                self._logger.warning(f"Manager {manager_name} does not exist! Will not give it tasks")
                return []
            elif manager[0]["status"] != ManagerStatusEnum.active:
                self._logger.warning(f"Manager {manager_name} exists but is not active!")
                return []

            found = []

            for q in tag_queries:

                new_limit = limit - len(found)

                # Have we found all we needed to find
                # (should always be >= 0, but can never be too careful. If it wasn't this could be an infinite loop)
                if new_limit <= 0:
                    break

                # Find tasks/base result:
                #   1. Status is waiting
                #   2. Whose required programs I am able to match
                #   3. Whose tags I am able to compute
                # with_for_update locks the rows. skip_locked=True makes it skip already-locked rows
                # (possibly from another process)
                # Also, load the base_result object so we can update stuff there (status)
                # TODO - we only test for the presence of the available_programs in the requirements. Eventually
                #        we want to then verify the versions

                # We do a plain .join() because we are querying, and then also supplying contains_eager() so that
                # the TaskQueueORM.base_result_obj gets populated
                # See https://docs-sqlalchemy.readthedocs.io/ko/latest/orm/loading_relationships.html#routing-explicit-joins-statements-into-eagerly-loaded-collections
                query = (
                    session.query(TaskQueueORM)
                    .join(TaskQueueORM.base_result_obj)  # Joins a BaseResultORM
                    .options(contains_eager(TaskQueueORM.base_result_obj))
                    .filter(BaseResultORM.status == RecordStatusEnum.waiting)
                    .filter(TaskQueueORM.required_programs.contained_by(available_programs))
                    .filter(*q)
                    .order_by(TaskQueueORM.priority.desc(), TaskQueueORM.created_on)
                    .limit(new_limit)
                    .with_for_update(skip_locked=True)
                )

                new_items = query.all()

                # Update all the task records to reflect this manager claiming them
                for task_orm in new_items:
                    task_orm.base_result_obj.status = RecordStatusEnum.running
                    task_orm.base_result_obj.manager_name = manager_name
                    task_orm.base_result_obj.modified_on = dt.utcnow()

                session.flush()

                # Store in dict form for returning
                found.extend([task_orm.dict() for task_orm in new_items])

        return found

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        base_result_id: Optional[Iterable[ObjectId]] = None,
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
    ) -> Tuple[QueryMetadata, List[TaskDict]]:
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

        load_cols, _ = get_query_proj_columns(TaskQueueORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(TaskQueueORM.id.in_(id))
        if base_result_id is not None:
            and_query.append(TaskQueueORM.base_result_id.in_(base_result_id))
        if status is not None:
            and_query.append(BaseResultORM.status.in_(status))
        if tag is not None:
            and_query.append(TaskQueueORM.tag.in_(tag))
        if manager is not None:
            and_query.append(BaseResultORM.manager_name.in_(manager))
        if program:
            and_query.append(TaskQueueORM.required_programs.has_any(program))

        with self._core_socket.optional_session(session, True) as session:
            query = (
                session.query(TaskQueueORM)
                .join(BaseResultORM, TaskQueueORM.base_result_id == BaseResultORM.id)
                .filter(and_(*and_query))
                .options(load_only(*load_cols))
            )
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def reset_status(
        self,
        id: Optional[List[str]] = None,
        base_result: Optional[List[str]] = None,
        manager: Optional[List[str]] = None,
        reset_running: bool = False,
        reset_error: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Reset the status of tasks to waiting

        Parameters
        ----------
        id : Optional[Union[str, List[str]]], optional
            The id of the task to modify
        base_result : Optional[Union[str, List[str]]], optional
            The id of the base result to modify
        manager : Optional[str], optional
            The manager name to reset the status of
        reset_running : bool, optional
            If True, reset running tasks to be waiting
        reset_error : bool, optional
            If True, also reset errored tasks to be waiting,

        Returns
        -------
        int
            Updated count
        """

        if not (reset_running or reset_error):
            # nothing to do
            return 0

        if sum(x is not None for x in [id, base_result, manager]) == 0:
            raise ValueError("All query fields are None, reset_status must specify queries.")

        status = []
        if reset_running:
            status.append(RecordStatusEnum.running)
        if reset_error:
            status.append(RecordStatusEnum.error)

        query = []
        if id:
            query.append(TaskQueueORM.id.in_(id))
        if status:
            query.append(BaseResultORM.status.in_(status))
        if base_result:
            query.append(BaseResultORM.id.in_(base_result))
        if manager:
            query.append(BaseResultORM.manager_name.in_(manager))

        # Must have status + something, checking above as well (being paranoid)
        if len(query) < 2:
            raise ValueError("All query fields are None, reset_status must specify queries.")

        with self._core_socket.optional_session(session) as session:
            results = (
                session.query(BaseResultORM)
                .join(TaskQueueORM, TaskQueueORM.base_result_id == BaseResultORM.id)
                .filter(*query)
                .with_for_update()
                .all()
            )

            for r in results:
                r.status = RecordStatusEnum.waiting
                r.modified_on = dt.utcnow()

            return len(results)

    def modify(
        self,
        id: Optional[List[str]] = None,
        base_result: Optional[List[str]] = None,
        new_tag: Optional[str] = None,
        new_priority: Optional[int] = None,
    ):
        """
        Modifies the tag and priority of tasks.

        This will only modify if the status is not running

        Parameters
        ----------
        id : Optional[Union[str, List[str]]], optional
            The id of the task to modify
        base_result : Optional[Union[str, List[str]]], optional
            The id of the base result to modify
        new_tag : Optional[str], optional
            New tag to assign to the given tasks
        new_priority: int, optional
            New priority to assign to the given tasks

        Returns
        -------
        int
            Updated count
        """

        if new_tag is None and new_priority is None:
            # nothing to do
            return 0

        if sum(x is not None for x in [id, base_result]) == 0:
            raise ValueError("All query fields are None, modify_task must specify queries.")

        and_query = []
        if id is not None:
            and_query.append(TaskQueueORM.id.in_(id))
        if base_result is not None:
            and_query.append(TaskQueueORM.base_result_id.in_(base_result))

        with self._core_socket.session_scope() as session:
            to_update = (
                session.query(TaskQueueORM)
                .join(TaskQueueORM.base_result_obj)
                .filter(and_(*and_query))
                .filter(BaseResultORM.status != RecordStatusEnum.running)
                .all()
            )

            for r in to_update:
                r.modified_on = dt.utcnow()
                if new_tag is not None:
                    r.tag = new_tag
                if new_priority is not None:
                    r.priority = new_priority

            return len(to_update)
