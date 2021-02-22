from __future__ import annotations

import json
from sqlalchemy import or_
from datetime import datetime as dt
from qcfractal.storage_sockets.models import BaseResultORM, TaskQueueORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import format_query, get_count_fast, get_procedure_class
from qcfractal.interface.models import TaskRecord, RecordStatusEnum, TaskStatusEnum

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Dict, Union, Optional


class TaskSocket:
    def __init__(self, core_socket):
        self._core_socket = core_socket

    def add(self, data: List[TaskRecord]):
        """Submit a list of tasks to the queue.
        Tasks are unique by their base_result, which should be inserted into
        the DB first before submitting it's corresponding task to the queue
        (with result.status='INCOMPLETE' as the default)
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

    def get_next(
            self, manager, available_programs, available_procedures, limit=100, tag=None
    ) -> List[TaskRecord]:
        """Obtain tasks for a manager

        Given tags and available programs/procedures on the manager, obtain
        waiting tasks to run.
        """

        proc_filt = TaskQueueORM.procedure.in_([p.lower() for p in available_procedures])
        none_filt = TaskQueueORM.procedure == None  # lgtm [py/test-equals-none]

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
        update_count = 0

        update_fields = {"status": TaskStatusEnum.running, "modified_on": dt.utcnow(), "manager": manager}
        with self._core_socket.session_scope() as session:
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
                new_ids = [x.id for x in new_items]

                # Update all the task records to reflect this manager claiming them
                update_count += (
                    session.query(TaskQueueORM)
                        .filter(TaskQueueORM.id.in_(new_ids))
                        .update(update_fields, synchronize_session=False)
                )

                # After commiting, the row locks are released
                session.commit()

                # How many more do we have to query
                new_limit = limit - len(new_items)

                # I would assume this is always true. If it isn't,
                # that would be really bad, and lead to an infinite loop
                assert new_limit >= 0

                # Store in dict form for returning. We will add the updated fields later
                found.extend([task.to_dict(exclude=update_fields.keys()) for task in new_items])

            # avoid another trip to the DB to get the updated values, set them here
            found = [TaskRecord(**task, **update_fields) for task in found]

        if update_count != len(found):
            self._core_socket.logger.warning("QUEUE: Number of found tasks does not match the number of updated tasks.")

        return found

    def get(
            self,
            id=None,
            hash_index=None,
            program=None,
            status: str = None,
            base_result: str = None,
            tag=None,
            manager=None,
            include=None,
            exclude=None,
            limit: int = None,
            skip: int = 0,
            return_json=False,
            with_ids=True,
    ):
        """
        TODO: check what query keys are needs
        Parameters
        ----------
        id : Optional[List[str]], optional
            Ids of the tasks
        Hash_index: Optional[List[str]], optional,
            hash_index of service, not used
        program, list of str or str, optional
        status : Optional[bool], optional (find all)
            The status of the task: 'COMPLETE', 'RUNNING', 'WAITING', or 'ERROR'
        base_result: Optional[str], optional
            base_result id
        include : Optional[List[str]], optional
            The fields to return, default to return all
        exclude : Optional[List[str]], optional
            The fields to not return, default to return all
        limit : Optional[int], optional
            maximum number of results to return
            if 'limit' is greater than the global setting self._max_limit,
            the self._max_limit will be returned instead
            (This is to avoid overloading the server)
        skip : int, optional
            skip the first 'skip' results. Used to paginate, default is 0
        return_json : bool, optional
            Return the results as a list of json inseated of objects, deafult is True
        with_ids : bool, optional
            Include the ids in the returned objects/dicts, default is True

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data, meta. Data is the objects found
        """

        meta = get_metadata_template()
        query = format_query(
            TaskQueueORM,
            program=program,
            id=id,
            hash_index=hash_index,
            status=status,
            base_result_id=base_result,
            tag=tag,
            manager=manager,
        )

        data = []
        try:
            data, meta["n_found"] = self._core_socket.get_query_projection(
                TaskQueueORM, query, limit=limit, skip=skip, include=include, exclude=exclude
            )
            meta["success"] = True
        except Exception as err:
            meta["error_description"] = str(err)

        data = [TaskRecord(**task) for task in data]

        return {"data": data, "meta": meta}

    def get_by_id(self, id: List[str], limit: int = None, skip: int = 0, as_json: bool = True):
        """Get tasks by their IDs

        Parameters
        ----------
        id : List[str]
            List of the task Ids in the DB
        limit : Optional[int], optional
            max number of returned tasks. If limit > max_limit, max_limit
            will be returned instead (safe query)
        skip : int, optional
            skip the first 'skip' results. Used to paginate, default is 0
        as_json : bool, optioanl
            Return tasks as JSON, default is True

        Returns
        -------
        List[TaskRecord]
            List of the found tasks
        """

        limit = self._core_socket.get_limit('task_queue', limit)
        with self._core_socket.session_scope() as session:
            found = (
                session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(id)).limit(limit).offset(skip)
            )

            if as_json:
                found = [TaskRecord(**task.to_dict()) for task in found]

        return found

    def mark_complete(self, task_ids: List[str]) -> int:
        """Update the given tasks as complete
        Note that each task is already pointing to its result location
        Mark the corresponding result/procedure as complete

        Parameters
        ----------
        task_ids : List[str]
            IDs of the tasks to mark as COMPLETE

        Returns
        -------
        int
            number of TaskRecord objects marked as COMPLETE, and deleted from the database consequtively.
        """

        if not task_ids:
            return 0

        with self._core_socket.session_scope() as session:
            # delete completed tasks
            tasks_c = (
                session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).delete(synchronize_session=False)
            )

        return tasks_c

    def mark_error(self, task_ids: List[str]) -> int:
        """
        update the given tasks as errored

        Parameters
        ----------
        task_ids : List[str]
            IDs of the tasks to mark as ERROR

        Returns
        -------
        int
            Number of tasks updated as errored.
        """

        if not task_ids:
            return 0

        updated_ids = []
        with self._core_socket.session_scope() as session:
            task_objects = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).all()

            for task_obj in task_objects:
                task_obj.status = TaskStatusEnum.error
                task_obj.modified_on = dt.utcnow()

            session.commit()

        return len(task_ids)

    def reset_status(
            self,
            id: Union[str, List[str]] = None,
            base_result: Union[str, List[str]] = None,
            manager: Optional[str] = None,
            reset_running: bool = False,
            reset_error: bool = False,
    ) -> int:
        """
        Reset the status of the tasks that a manager owns from Running to Waiting
        If reset_error is True, then also reset errored tasks AND its results/proc

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
            also update results/proc to be INCOMPLETE

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

        with self._core_socket.session_scope() as session:
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

    def reset_base_result_status(
            self,
            id: Union[str, List[str]] = None,
    ) -> int:
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

        with self._core_socket.session_scope() as session:
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

    def delete(self, id: Union[str, list]):
        """
        Delete a task from the queue. Use with cautious

        Parameters
        ----------
        id : str or List
            Ids of the tasks to delete
        Returns
        -------
        int
            Number of tasks deleted
        """

        task_ids = [id] if isinstance(id, (int, str)) else id
        with self._core_socket.session_scope() as session:
            count = session.query(TaskQueueORM).filter(TaskQueueORM.id.in_(task_ids)).delete(synchronize_session=False)

        return count