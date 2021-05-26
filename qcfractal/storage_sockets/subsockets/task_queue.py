from __future__ import annotations

import logging
from sqlalchemy import or_, and_
from sqlalchemy.orm import load_only
from datetime import datetime as dt
from qcfractal.storage_sockets.models import BaseResultORM, TaskQueueORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import (
    insert_general,
    get_query_proj_columns,
    get_count,
)
from qcfractal.interface.models import TaskRecord, RecordStatusEnum, TaskStatusEnum, ManagerStatusEnum, ObjectId
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
        self._user_limit = core_socket.qcf_config.response_limits.task
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

        # TODO - logic will need to be adjusted with new statuses
        # This is an error. These should have been checked before calling this function
        if any(x["status"] == RecordStatusEnum.complete for x in statuses):
            raise RuntimeError(
                "Cannot add TaskQueueORM for a procedure that is already complete. This is a programmer error"
            )

        with self._core_socket.optional_session(session) as session:
            meta, ids = insert_general(session, tasks, (TaskQueueORM.base_result_id,), (TaskQueueORM.id,))

            return meta, [x[0] for x in ids]

    def add(self, data: List[TaskRecord]):
        """Submit a list of tasks to the queue.
        Tasks are unique by their base_result, which should be inserted into
        the DB first before submitting it's corresponding task to the queue
        (with result.status='incomplete' as the default)
        The default task.status is 'WAITING'

        Parameters
        ----------
        data : List[TaskRecord]
            A task is a dict, with the following fields:
            - hash_index: idx, not used anymore
            - spec: dynamic field (dict-like), can have any structure
            - tag: str
            - base_results: tuple (required), first value is the class type
             of the result, {'results' or 'procedure'). The second value is
             the ID of the result in the DB. Example:
             "base_result": ('results', result_id)

        Returns
        -------
        Dict[str, Any]
            Dictionary with keys data and meta.
            'data' is a list of the IDs of the tasks IN ORDER, including
            duplicates. An errored task has 'None' in its ID
            meta['duplicates'] has the duplicate tasks
        """

        self._logger.warning("REMOVE ME")
        meta = add_metadata_template()

        results = ["placeholder"] * len(data)

        with self._core_socket.session_scope() as session:
            # preserving all the base results for later check
            all_base_results = [record.base_result for record in data]
            query_res = (
                session.query(TaskQueueORM.id, TaskQueueORM.base_result_id)
                .filter(TaskQueueORM.base_result_id.in_(all_base_results))
                .all()
            )

            # constructing a dict of found tasks and their ids
            found_dict = {str(base_result_id): str(task_id) for task_id, base_result_id in query_res}
            new_tasks, new_idx = [], []
            duplicate_idx = []
            for task_num, record in enumerate(data):

                if found_dict.get(record.base_result):
                    # if found, get id from found_dict
                    # Note: found_dict may return a task object because the duplicate id is of an object in the input.
                    results[task_num] = found_dict.get(record.base_result)
                    # add index of duplicates
                    duplicate_idx.append(task_num)
                    meta["duplicates"].append(task_num)

                else:
                    task_dict = record.dict(exclude={"id"})
                    task = TaskQueueORM(**task_dict)
                    new_idx.append(task_num)
                    task.priority = task.priority.value
                    # append all the new tasks that should be added
                    new_tasks.append(task)
                    # add the (yet to be) inserted object id to dictionary
                    found_dict[record.base_result] = task

            session.add_all(new_tasks)
            session.commit()

            meta["n_inserted"] += len(new_tasks)
            # setting the id for new inserted objects, cannot be done before commiting as new objects do not have ids
            for i, task_idx in enumerate(new_idx):
                results[task_idx] = str(new_tasks[i].id)

            # finding the duplicate items in input, for which ids are found only after insertion
            for i in duplicate_idx:
                if not isinstance(results[i], str):
                    results[i] = str(results[i].id)

        meta["success"] = True

        ret = {"data": results, "meta": meta}
        return ret

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
        available_programs: Iterable[str],
        available_procedures: Iterable[str],
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
            List or other iterable of programs available on that manager
        available_procedures
            List or other iterable of procedures available on that manager
        limit
            Maximum number of tasks that the manager can claim
        tag
            List or other sequence of tags (with earlier tags taking preference over later tags)
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        # TODO - rewrite to avoid format_query

        limit = calculate_limit(self._manager_limit, limit)
        proc_filt = TaskQueueORM.procedure.in_([p.lower() for p in available_procedures])
        none_filt = TaskQueueORM.procedure == None  # lgtm [py/test-equals-none]

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

            order_by = []
            if tag is not None:
                if isinstance(tag, str):
                    tag = [tag]

            order_by.extend([TaskQueueORM.priority.desc(), TaskQueueORM.created_on])
            queries = []
            if tag is not None:
                for t in tag:
                    query = format_query(TaskQueueORM, status=TaskStatusEnum.waiting, program=available_programs, tag=t)
                    query.append(or_(proc_filt, none_filt))
                    queries.append(query)
            else:
                query = format_query(TaskQueueORM, status=TaskStatusEnum.waiting, program=available_programs)
                query.append((or_(proc_filt, none_filt)))
                queries.append(query)

            new_limit = limit
            found = []

            for q in queries:

                # Have we found all we needed to find
                if new_limit == 0:
                    break

                # with_for_update locks the rows. skip_locked=True makes it skip already-locked rows
                # (possibly from another process)
                query = (
                    session.query(TaskQueueORM)
                    .filter(*q)
                    .order_by(*order_by)
                    .limit(new_limit)
                    .with_for_update(skip_locked=True)
                )

                new_items = query.all()

                # Update all the task records to reflect this manager claiming them
                for task in new_items:
                    task.status = TaskStatusEnum.running
                    task.modified_on = dt.utcnow()
                    task.manager = manager_name

                session.flush()

                # How many more do we have to query
                new_limit = limit - len(new_items)

                # I would assume this is always true. If it isn't,
                # that would be really bad, and lead to an infinite loop
                assert new_limit >= 0

                # Store in dict form for returning
                found.extend([task.dict() for task in new_items])

        return found

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        base_result_id: Optional[Iterable[ObjectId]] = None,
        program: Optional[Iterable[ObjectId]] = None,
        status: Optional[Iterable[TaskStatusEnum]] = None,
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
            The status of the task: 'complete', 'running', 'waiting', 'error'
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
        if program is not None:
            and_query.append(TaskQueueORM.program.in_(program))
        if status is not None:
            and_query.append(TaskQueueORM.status.in_(status))
        if tag is not None:
            and_query.append(TaskQueueORM.tag.in_(tag))
        if manager is not None:
            and_query.append(TaskQueueORM.manager.in_(manager))

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(TaskQueueORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def reset_status(
        self,
        id: Union[str, List[str]] = None,
        base_result: Union[str, List[str]] = None,
        manager: Optional[str] = None,
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
            also update results/proc to be 'incomplete'

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
            status.append(TaskStatusEnum.running)
        if reset_error:
            status.append(TaskStatusEnum.error)

        query = format_query(TaskQueueORM, id=id, base_result_id=base_result, manager=manager, status=status)

        # Must have status + something, checking above as well(being paranoid)
        if len(query) < 2:
            raise ValueError("All query fields are None, reset_status must specify queries.")

        with self._core_socket.optional_session(session) as session:
            # Update results and procedures if reset_error
            task_ids = session.query(TaskQueueORM.id).filter(*query)
            session.query(BaseResultORM).filter(TaskQueueORM.base_result_id == BaseResultORM.id).filter(
                TaskQueueORM.id.in_(task_ids)
            ).update(dict(status=RecordStatusEnum.incomplete, modified_on=dt.utcnow()), synchronize_session=False)

            updated = (
                session.query(TaskQueueORM)
                .filter(TaskQueueORM.id.in_(task_ids))
                .update(dict(status=TaskStatusEnum.waiting, modified_on=dt.utcnow()), synchronize_session=False)
            )

        return updated

    def reset_base_result_status(self, id: Union[str, List[str]] = None, *, session: Optional[Session] = None) -> int:
        """
        Reset the status of a base result to "incomplete". Will only work if the
        status is not complete.

        This should be rarely called. Handle with care!

        Parameters
        ----------
        id : Optional[Union[str, List[str]]], optional
            The id of the base result to modify

        Returns
        -------
        int
            Number of base results modified
        """

        query = format_query(BaseResultORM, id=id)
        update_dict = {"status": RecordStatusEnum.incomplete, "modified_on": dt.utcnow()}

        with self._core_socket.optional_session(session) as session:
            updated = (
                session.query(BaseResultORM)
                .filter(*query)
                .filter(BaseResultORM.status != RecordStatusEnum.complete)
                .update(update_dict, synchronize_session=False)
            )

        return updated

    def modify(
        self,
        id: Union[str, List[str]] = None,
        base_result: Union[str, List[str]] = None,
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

        query = format_query(TaskQueueORM, id=id, base_result_id=base_result)

        update_dict = {}
        if new_tag is not None:
            update_dict["tag"] = new_tag
        if new_priority is not None:
            update_dict["priority"] = new_priority

        update_dict["modified_on"] = dt.utcnow()

        with self._core_socket.session_scope() as session:
            updated = (
                session.query(TaskQueueORM)
                .filter(*query)
                .filter(TaskQueueORM.status != TaskStatusEnum.running)
                .update(update_dict, synchronize_session=False)
            )

        return updated
