from __future__ import annotations

from datetime import datetime
import traceback
import logging

from sqlalchemy import and_, select
from sqlalchemy.orm import joinedload, selectinload, contains_eager, load_only

from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.components.records.db_models import BaseResultORM
from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.exceptions import ComputeManagerError

from qcfractal.db_socket.helpers import (
    insert_general,
    get_query_proj_options,
    get_count,
)

from qcfractal.db_socket.helpers import calculate_limit

from qcfractal.interface.models import (
    PriorityEnum,
    FailedOperation,
    RecordStatusEnum,
    AllProcedureSpecifications,
)
from qcfractal.portal.components.managers import ManagerStatusEnum

from qcfractal.portal.metadata_models import InsertMetadata, QueryMetadata, TaskReturnMetadata
from typing import TYPE_CHECKING

from qcfractal.components.records.base_handlers import BaseProcedureHandler
from qcfractal.components.records.failure import FailedOperationHandler
from qcfractal.components.records.singlepoint.handlers import SingleResultHandler
from qcfractal.components.records.optimization.handlers import OptimizationHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.interface.models import ObjectId, AllResultTypes, Molecule
    from qcfractal.portal.components.outputstore import OutputStore
    from typing import List, Dict, Tuple, Optional, Any, Iterable, Sequence, Union

    TaskDict = Dict[str, Any]


class TaskSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self._user_task_limit = root_socket.qcf_config.response_limits.task_queue
        self._manager_task_limit = root_socket.qcf_config.response_limits.manager_task

        # Subsubsockets/handlers
        self.single = SingleResultHandler(root_socket)
        self.optimization = OptimizationHandler(root_socket)
        self.failure = FailedOperationHandler(root_socket)

        self.handler_map: Dict[str, BaseProcedureHandler] = {
            "single": self.single,
            "optimization": self.optimization,
            "failure": self.failure,
        }

    # def add_task_orm(
    #    self, tasks: List[TaskQueueORM], *, session: Optional[Session] = None
    # ) -> Tuple[InsertMetadata, List[ObjectId]]:
    #    """
    #    Adds TaskQueueORM to the database, taking into account duplicates

    #    If a task should not be added because the corresponding procedure is already marked
    #    complete, then that will raise an exception.

    #    The session is flushed at the end of this function.

    #    Parameters
    #    ----------
    #    tasks
    #        ORM objects to add to the database
    #    session
    #        An existing SQLAlchemy session to use. If None, one will be created. If an existing session
    #        is used, it will be flushed before returning from this function.

    #    Returns
    #    -------
    #    :
    #        Metadata showing what was added, and a list of returned task ids. These will be in the
    #        same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
    #    """

    #    # Check for incompatible statuses
    #    base_result_ids = [x.base_result_id for x in tasks]
    #    statuses = self.root_socket.records.get(base_result_ids, include=["status"], session=session)

    #    # This is an error. These should have been checked before calling this function
    #    if any(x["status"] == RecordStatusEnum.complete for x in statuses):
    #        raise RuntimeError(
    #            "Cannot add TaskQueueORM for a procedure that is already complete. This is a programmer error"
    #        )

    #    with self.root_socket.optional_session(session) as session:
    #        meta, ids = insert_general(session, tasks, (TaskQueueORM.base_result_id,), (TaskQueueORM.id,))
    #        return meta, [x[0] for x in ids]

    # def update_outputs(
    #    self,
    #    session: Session,
    #    base_record_orm: BaseResultORM,
    #    *,
    #    stdout: Optional[Union[Dict, str, OutputStore]] = None,
    #    stderr: Optional[Union[Dict, str, OutputStore]] = None,
    #    error: Optional[Union[Dict, str, OutputStore]] = None,
    # ):
    #    """
    #    Add outputs (stdout, stderr, error) to a base record, and delete the old one if it exists
    #    """

    #    to_delete = []
    #    if base_record_orm.stdout is not None:
    #        to_delete.append(base_record_orm.stdout)
    #        base_record_orm.stdout = None
    #    if base_record_orm.stderr is not None:
    #        to_delete.append(base_record_orm.stderr)
    #        base_record_orm.stderr = None
    #    if base_record_orm.error is not None:
    #        to_delete.append(base_record_orm.error)
    #        base_record_orm.error = None

    #    if stdout is not None:
    #        base_record_orm.stdout = self.root_socket.outputstore.add([stdout], session=session)[0]
    #    if stderr is not None:
    #        base_record_orm.stderr = self.root_socket.outputstore.add([stderr], session=session)[0]
    #    if error is not None:
    #        base_record_orm.error = self.root_socket.outputstore.add([error], session=session)[0]

    #    # self.root_socket.outputstore.delete(to_delete, session=session)

    # def create(
    #    self, molecules: List[Molecule], specification: AllProcedureSpecifications, *, session: Optional[Session] = None
    # ) -> Tuple[InsertMetadata, List[Optional[ObjectId]]]:
    #    """
    #    Create procedures as well as tasks

    #    Parameters
    #    ----------
    #    molecules
    #    specification

    #    Returns
    #    -------

    #    """

    #    # The procedure should have been checked by the pydantic model
    #    procedure_handler = self.handler_map[specification.procedure]

    #    # Verify the procedure
    #    procedure_handler.validate_input(specification)

    #    # Add all the molecules stored in the 'data' member
    #    # This should apply to all procedures
    #    molecule_meta, molecule_ids = self.root_socket.molecules.add_mixed(molecules)

    #    # Only do valid molecule ids (ie, not None in the returned list)
    #    # These would correspond to errors
    #    # TODO - INT ID
    #    valid_molecule_ids = [int(x) for x in molecule_ids if x is not None]
    #    valid_molecule_idx = [idx for idx, x in enumerate(molecule_ids) if x is not None]
    #    invalid_molecule_idx = [idx for idx, x in enumerate(molecule_ids) if x is None]

    #    # Create procedures and tasks in the same session
    #    # This will be committed only at the end
    #    with self.root_socket.optional_session(session) as session:
    #        meta, ids = procedure_handler.create_records(session, valid_molecule_ids, specification)

    #        if not meta.success:
    #            # The above should always succeed if the model was validated. If not, that is
    #            # an internal error
    #            # This will rollback the session
    #            raise RuntimeError(f"Error adding procedures: {meta.error_string}")

    #        # only create tasks for new procedures
    #        new_ids = [ids[i] for i in meta.inserted_idx]

    #        new_orm = (
    #            session.query(BaseResultORM).filter(BaseResultORM.id.in_(new_ids)).options(selectinload("*")).all()
    #        )

    #        # These should only correspond to the same procedure we added
    #        assert all(x.procedure == specification.procedure for x in new_orm)

    #        # Create tasks for everything
    #        task_meta, _ = procedure_handler.create_tasks(session, new_orm, specification.tag, specification.priority)

    #        if not task_meta.success:
    #            # In general, create_tasks should always succeed
    #            # If not, that is usually a programmer error
    #            # This will rollback the session
    #            raise RuntimeError(f"Error adding tasks: {task_meta.error_string}")

    #        # session will commit when closed at the end of this 'with' block

    #    # Place None in the ids list where molecules were None
    #    for idx in invalid_molecule_idx:
    #        ids.insert(idx, None)

    #    # Now adjust the index lists in the metadata to correspond to the original molecule order
    #    inserted_idx = [valid_molecule_idx[x] for x in meta.inserted_idx]
    #    existing_idx = [valid_molecule_idx[x] for x in meta.existing_idx]
    #    errors = [(valid_molecule_idx[x], msg) for x, msg in meta.errors] + molecule_meta.errors

    #    return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors), ids  # type: ignore

    def update_completed(self, manager_name: str, results: Dict[int, AllResultTypes]) -> TaskReturnMetadata:
        """
        Insert data from completed calculations into the database

        Parameters
        ----------
        manager_name
            The name of the manager submitting the results
        results
            Results (in QCSchema format), with the task_id as the key
        """

        all_task_ids = list(results.keys())

        self._logger.info("Received completed tasks from {}.".format(manager_name))
        self._logger.info("    Task ids: " + " ".join(str(x) for x in all_task_ids))

        tasks_success: List[int] = []
        tasks_failures: List[int] = []
        tasks_rejected: List[Tuple[int, str]] = []

        with self.root_socket.session_scope() as session:

            stmt = select(ComputeManagerORM).where(ComputeManagerORM.name == manager_name)
            stmt = stmt.with_for_update(skip_locked=False)
            manager: Optional[ComputeManagerORM] = session.execute(stmt).scalar_one_or_none()

            if manager is None:
                self._logger.warning(f"Manager {manager_name} does not exist, but is trying to return tasks. Ignoring.")
                raise ComputeManagerError(f"Manager {manager_name} does not exist", True)

            if manager.status != ManagerStatusEnum.active:
                self._logger.warning(f"Manager {manager_name} is not active. Ignoring...")
                raise ComputeManagerError(f"Manager {manager_name} is not active", True)

            all_notifications: List[Tuple[int, RecordStatusEnum]] = []

            for task_id, result in results.items():

                # We load one at a time. This works well with 'with_for_update'
                # which will do row locking. This lock is released on commit or rollback
                stmt = select(TaskQueueORM).filter(TaskQueueORM.id == task_id)
                stmt = stmt.options(joinedload(TaskQueueORM.record, innerjoin=True))
                stmt = stmt.with_for_update(skip_locked=False)

                task_orm: Optional[TaskQueueORM] = session.execute(stmt).scalar_one_or_none()

                # Does the task exist?
                if task_orm is None:
                    self._logger.warning(f"Task id {task_id} does not exist in the task queue")
                    tasks_rejected.append((task_id, "Task does not exist in the task queue"))
                    continue

                record_orm: BaseResultORM = task_orm.record
                record_id = record_orm.id

                # Start a nested transaction, so that we can rollback if there is an issue with
                # an individual result without releasing the lock on the manager
                nested_session = session.begin_nested()

                notify_status = None

                try:
                    #################################################################
                    # Perform some checks for consistency
                    #################################################################
                    # Is the task in the running state
                    # If so, do not attempt to modify the task queue. Just move on
                    if record_orm.status != RecordStatusEnum.running:
                        self._logger.warning(f"Record {record_id} (task {task_id}) is not in a running state")
                        tasks_rejected.append((task_id, "Task is not in a running state"))

                    # Was the manager that sent the data the one that was assigned?
                    # If so, do not attempt to modify the task queue. Just move on
                    elif record_orm.manager_name != manager_name:
                        self._logger.warning(
                            f"Record {record_id} (task {task_id}) claimed by {record_orm.manager_name}, not {manager_name}"
                        )
                        tasks_rejected.append((task_id, "Task is claimed by another manager"))

                    # Failed task returning FailedOperation
                    elif result.success is False and isinstance(result, FailedOperation):
                        self.root_socket.records.update_failure(session, record_orm, result, manager_name)
                        notify_status = RecordStatusEnum.error
                        tasks_failures.append(task_id)

                    elif result.success is not True:
                        # QCEngine should always return either FailedOperation, or some result with success == True
                        msg = f"Unexpected return from manager for task {task_id}/base result {record_id}: Returned success != True, but not a FailedOperation"
                        error = {"error_type": "internal_fractal_error", "error_message": msg}
                        failed_op = FailedOperation(error=error, success=False)

                        self.root_socket.records.update_failure(session, record_orm, failed_op, manager_name)
                        notify_status = RecordStatusEnum.error

                        self._logger.error(msg)
                        tasks_rejected.append((task_id, "Returned success=False, but not a FailedOperation"))

                    # Manager returned a full, successful result
                    else:
                        self.root_socket.records.update_completed(session, record_orm, result, manager_name)

                        # Delete the task from the task queue since it is completed
                        session.delete(task_orm)

                        notify_status = RecordStatusEnum.complete
                        tasks_success.append(task_id)

                except Exception:
                    # We have no idea what was added or is pending for removal
                    # So rollback the transaction to the most recent commit
                    nested_session.rollback()

                    msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                    error = {"error_type": "internal_fractal_error", "error_message": msg}
                    failed_op = FailedOperation(error=error, success=False)

                    self.root_socket.records.update_failure(session, record_orm, failed_op, manager_name)
                    notify_status = RecordStatusEnum.error

                    self._logger.error(msg)
                    tasks_rejected.append((task_id, "Internal server error"))

                finally:
                    # releases the SAVEPOINT, does not actually commit to the db
                    # (see SAVEPOINTS in postgres docs)
                    nested_session.commit()

                    if notify_status is not None:
                        all_notifications.append((record_id, notify_status))

            # Update the stats for the manager
            manager.successes += len(tasks_success)
            manager.failures += len(tasks_failures)
            manager.rejected += len(tasks_rejected)

        # Send notifications that tasks were completed
        for record_id, notify_status in all_notifications:
            self.root_socket.notify_completed_watch(record_id, notify_status)

        self._logger.info(
            "Processed {} returned tasks ({} successful, {} failed, {} rejected).".format(
                len(results), len(tasks_success), len(tasks_failures), len(tasks_rejected)
            )
        )

        return TaskReturnMetadata(rejected_info=tasks_rejected, accepted_ids=tasks_success)

    # def get_tasks(
    #    self,
    #    id: Sequence[str],
    #    include: Optional[Sequence[str]] = None,
    #    exclude: Optional[Sequence[str]] = None,
    #    missing_ok: bool = False,
    #    *,
    #    session: Optional[Session] = None,
    # ) -> List[Optional[TaskDict]]:
    #    """Get tasks by their IDs

    #    The returned task information will be in order of the given ids

    #    If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
    #    the corresponding entry in the returned list of tasks will be None.

    #    Parameters
    #    ----------
    #    id
    #        List of the task Ids in the DB
    #    include
    #        Which fields of the task to return. Default is to return all fields.
    #    exclude
    #        Remove these fields from the return. Default is to return all fields.
    #    missing_ok
    #       If set to True, then missing tasks will be tolerated, and the returned list of
    #       tasks will contain None for the corresponding IDs that were not found.
    #    session
    #        An existing SQLAlchemy session to use. If None, one will be created

    #    Returns
    #    -------
    #    :
    #        List of the found tasks
    #    """

    #    with self.root_socket.optional_session(session, True) as session:
    #        if len(id) > self._user_task_limit:
    #            raise RuntimeError(f"Request for {len(id)} tasks is over the limit of {self._user_task_limit}")

    #        # TODO - int id
    #        int_id = [int(x) for x in id]
    #        unique_ids = list(set(int_id))

    #        load_cols, _ = get_query_proj_columns(TaskQueueORM, include, exclude)

    #        results = (
    #            session.query(TaskQueueORM)
    #            .filter(TaskQueueORM.id.in_(unique_ids))
    #            .options(load_only(*load_cols))
    #            .yield_per(250)
    #        )
    #        result_map = {r.id: r.dict() for r in results}

    #        # Put into the requested order
    #        ret = [result_map.get(x, None) for x in int_id]

    #        if missing_ok is False and None in ret:
    #            raise RuntimeError("Could not find all requested task records")

    #        return ret

    # def regenerate_tasks(
    #    self,
    #    id: Iterable[ObjectId],
    #    tag: Optional[str] = None,
    #    priority: PriorityEnum = PriorityEnum.normal,
    #    *,
    #    session: Optional[Session] = None,
    # ) -> List[Optional[ObjectId]]:
    #    """
    #    Regenerates tasks for the given procedures

    #    Parameters
    #    ----------
    #    session
    #        An existing SQLAlchemy session to use. If None, one will be created. If an existing session
    #        is used, it will be flushed before returning from this function.

    #    Returns
    #    -------
    #    :
    #        List of new task ids, in the order of the input ids. If a base result does not exist, or is marked completed,
    #        or already has a task associated with it, then the corresponding entry will be None
    #    """

    #    task_ids: List[Optional[ObjectId]] = []

    #    with self.root_socket.optional_session(session) as session:
    #        # Slow, but this shouldn't be called too often
    #        for record_id in id:
    #            record = (
    #                session.query(BaseResultORM)
    #                .filter(BaseResultORM.id == record_id)
    #                .options(joinedload(BaseResultORM.task_obj))
    #                .with_for_update()
    #                .one_or_none()
    #            )

    #            # Base result doesn't exist
    #            if record is None:
    #                task_ids.append(None)
    #                continue

    #            # Task entry already exists
    #            if record.task_obj is not None:
    #                task_ids.append(None)
    #                continue

    #            # Result is already complete
    #            if record.status == RecordStatusEnum.complete:
    #                task_ids.append(None)
    #                continue

    #            # Now actually create the task
    #            procedure_handler = self.handler_map[record.procedure]
    #            task_meta, new_task_ids = procedure_handler.create_tasks(session, [record], tag, priority)

    #            if not task_meta.success:
    #                # In general, create_tasks should always succeed
    #                # If not, that is usually a programmer error
    #                # This will rollback the session
    #                raise RuntimeError(f"Error adding tasks: {task_meta.error_string}")

    #            record.status = RecordStatusEnum.waiting
    #            record.modified_on = datetime.utcnow()
    #            task_ids.extend(new_task_ids)

    #    return task_ids

    def claim_tasks(
        self,
        manager_name: str,
        limit: Optional[int] = None,
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
        limit
            Maximum number of tasks that the manager can claim
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        limit = calculate_limit(self._manager_task_limit, limit)

        with self.root_socket.optional_session(session) as session:
            stmt = select(ComputeManagerORM).where(ComputeManagerORM.name == manager_name)
            stmt = stmt.with_for_update(skip_locked=False)
            manager: Optional[ComputeManagerORM] = session.execute(stmt).scalar_one_or_none()

            if manager is None:
                self._logger.warning(f"Manager {manager_name} does not exist! Will not give it tasks")
                raise ComputeManagerError("Manager does not exist!", shutdown=True)
            elif manager.status != ManagerStatusEnum.active:
                self._logger.warning(f"Manager {manager_name} exists but is not active! Will not give it tasks")
                raise ComputeManagerError("Manager is not active!", shutdown=True)

            found = []

            for tag in manager.tags:

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
                stmt = select(TaskQueueORM).join(TaskQueueORM.record).options(contains_eager(TaskQueueORM.record))
                stmt = stmt.filter(BaseResultORM.status == RecordStatusEnum.waiting)
                stmt = stmt.filter(TaskQueueORM.required_programs.contained_by(manager.programs))
                stmt = stmt.order_by(TaskQueueORM.priority.desc(), TaskQueueORM.created_on)

                # If tag is "*", then the manager will pull anything
                if tag != "*":
                    stmt = stmt.filter(TaskQueueORM.tag == tag)

                # Skip locked rows - They may be in the process of being claimed by someone else
                stmt = stmt.limit(new_limit).with_for_update(skip_locked=True)

                new_items = session.execute(stmt).scalars().all()

                # Update all the task records to reflect this manager claiming them
                for task_orm in new_items:
                    task_orm.record.status = RecordStatusEnum.running
                    task_orm.record.manager_name = manager_name
                    task_orm.record.modified_on = datetime.utcnow()

                session.flush()

                # Store in dict form for returning,
                # but no need to store the info from the base record
                found.extend([task_orm.dict(exclude=["record"]) for task_orm in new_items])

            manager.claimed += len(found)

        return found

    # def query_tasks(
    #    self,
    #    id: Optional[Iterable[ObjectId]] = None,
    #    base_result_id: Optional[Iterable[ObjectId]] = None,
    #    program: Optional[Iterable[str]] = None,
    #    status: Optional[Iterable[RecordStatusEnum]] = None,
    #    tag: Optional[Iterable[str]] = None,
    #    manager: Optional[Iterable[str]] = None,
    #    include: Optional[Iterable[str]] = None,
    #    exclude: Optional[Iterable[str]] = None,
    #    limit: Optional[int] = None,
    #    skip: int = 0,
    #    *,
    #    session: Optional[Session] = None,
    # ) -> Tuple[QueryMetadata, List[TaskDict]]:
    #    """
    #    General query of tasks in the database

    #    All search criteria are merged via 'and'. Therefore, records will only
    #    be found that match all the criteria.

    #    Parameters
    #    ----------
    #    id
    #        Ids of the task (not result!) to search for
    #    base_result_id
    #        The base result ID of the task
    #    program
    #        Programs to search for
    #    status
    #        The status of the task: 'running', 'waiting', 'error'
    #    tag
    #        Tags of the task to search for
    #    manager
    #        Search for tasks assigned to given managers
    #    include
    #        Which fields of the molecule to return. Default is to return all fields.
    #    exclude
    #        Remove these fields from the return. Default is to return all fields.
    #    limit
    #        Limit the number of results. If None, the server limit will be used.
    #        This limit will not be respected if greater than the configured limit of the server.
    #    skip
    #        Skip this many results from the total list of matches. The limit will apply after skipping,
    #        allowing for pagination.
    #    session
    #        An existing SQLAlchemy session to use. If None, one will be created

    #    Returns
    #    -------
    #    :
    #        Dict with keys: data, meta. Data is the objects found
    #    """

    #    limit = calculate_limit(self._user_task_limit, limit)

    #    load_cols, _ = get_query_proj_columns(TaskQueueORM, include, exclude)

    #    and_query = []
    #    if id is not None:
    #        and_query.append(TaskQueueORM.id.in_(id))
    #    if base_result_id is not None:
    #        and_query.append(TaskQueueORM.base_result_id.in_(base_result_id))
    #    if status is not None:
    #        and_query.append(BaseResultORM.status.in_(status))
    #    if tag is not None:
    #        and_query.append(TaskQueueORM.tag.in_(tag))
    #    if manager is not None:
    #        and_query.append(BaseResultORM.manager_name.in_(manager))
    #    if program:
    #        and_query.append(TaskQueueORM.required_programs.has_any(program))

    #    with self.root_socket.optional_session(session, True) as session:
    #        query = (
    #            session.query(TaskQueueORM)
    #            .join(TaskQueueORM.base_result_obj)
    #            .filter(and_(*and_query))
    #            .options(load_only(*load_cols))
    #        )
    #        n_found = get_count(query)
    #        results = query.limit(limit).offset(skip).all()
    #        result_dicts = [x.dict() for x in results]

    #    meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
    #    return meta, result_dicts

    def reset_tasks(
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

        if all(x is None for x in [id, base_result, manager]):
            raise ValueError("All query fields are None, reset_stasks must specify queries.")

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
            raise ValueError("All query fields are None, reset_tasks must specify queries.")

        with self.root_socket.optional_session(session) as session:
            results = session.query(BaseResultORM).join(BaseResultORM.task).filter(*query).with_for_update().all()

            for r in results:
                r.status = RecordStatusEnum.waiting
                r.modified_on = datetime.utcnow()
                r.manager_name = None

            return len(results)

    # def modify_tasks(
    #    self,
    #    id: Optional[List[str]] = None,
    #    base_result: Optional[List[str]] = None,
    #    new_tag: Optional[str] = None,
    #    new_priority: Optional[int] = None,
    # ):
    #    """
    #    Modifies the tag and priority of tasks.

    #    This will only modify if the status is not running

    #    Parameters
    #    ----------
    #    id : Optional[Union[str, List[str]]], optional
    #        The id of the task to modify
    #    base_result : Optional[Union[str, List[str]]], optional
    #        The id of the base result to modify
    #    new_tag : Optional[str], optional
    #        New tag to assign to the given tasks
    #    new_priority: int, optional
    #        New priority to assign to the given tasks

    #    Returns
    #    -------
    #    int
    #        Updated count
    #    """

    #    if new_tag is None and new_priority is None:
    #        # nothing to do
    #        return 0

    #    if sum(x is not None for x in [id, base_result]) == 0:
    #        raise ValueError("All query fields are None, modify_task must specify queries.")

    #    and_query = []
    #    if id is not None:
    #        and_query.append(TaskQueueORM.id.in_(id))
    #    if base_result is not None:
    #        and_query.append(TaskQueueORM.base_result_id.in_(base_result))

    #    with self.root_socket.session_scope() as session:
    #        to_update = (
    #            session.query(TaskQueueORM)
    #            .join(TaskQueueORM.record)
    #            .filter(and_(*and_query))
    #            .filter(BaseResultORM.status != RecordStatusEnum.running)
    #            .all()
    #        )

    #        for r in to_update:
    #            r.modified_on = datetime.utcnow()
    #            if new_tag is not None:
    #                r.tag = new_tag
    #            if new_priority is not None:
    #                r.priority = new_priority

    #        return len(to_update)
