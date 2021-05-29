from __future__ import annotations

from datetime import datetime
import traceback
import logging
from qcfractal.storage_sockets.models import (
    BaseResultORM,
    TaskQueueORM,
)
from sqlalchemy import and_
from sqlalchemy.orm import joinedload, selectinload, load_only
from qcfractal.interface.models import (
    TaskStatusEnum,
    FailedOperation,
    RecordStatusEnum,
    InsertMetadata,
    AllProcedureSpecifications,
    QueryMetadata,
)
from ..sqlalchemy_common import get_query_proj_columns, get_count
from qcfractal.storage_sockets.sqlalchemy_socket import (
    calculate_limit,
)

from typing import TYPE_CHECKING

from .procedures import BaseProcedureHandler, FailedOperationHandler, SingleResultHandler, OptimizationHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from qcfractal.interface.models import ObjectId, AllResultTypes, Molecule
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Iterable

    ProcedureDict = Dict[str, Any]


class ProcedureSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.result

        # Subsubsockets/handlers
        self.single = SingleResultHandler(core_socket)
        self.optimization = OptimizationHandler(core_socket)
        self.failure = FailedOperationHandler(core_socket)

        self.handler_map: Dict[str, BaseProcedureHandler] = {
            "single": self.single,
            "optimization": self.optimization,
            "failure": self.failure,
        }

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        procedure: Optional[Iterable[str]] = None,
        manager: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: int = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ProcedureDict]]:
        """

        Parameters
        ----------
        id
            Query for procedures based on its ID
        procedure
            Query based on procedure type
        status
            The status of the procedure
        created_before
            Query for records created before this date
        created_after
            Query for records created after this date
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
            Metadata about the results of the query, and a list of procedure data (as dictionaries)
        """

        limit = calculate_limit(self._limit, limit)

        load_cols, _ = get_query_proj_columns(BaseResultORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(BaseResultORM.id.in_(id))
        if procedure is not None:
            and_query.append(BaseResultORM.procedure.in_(procedure))
        if manager is not None:
            and_query.append(BaseResultORM.manager_name.in_(manager))
        if status is not None:
            and_query.append(BaseResultORM.status.in_(status))
        if created_before is not None:
            and_query.append(BaseResultORM.created_on < created_before)
        if created_after is not None:
            and_query.append(BaseResultORM.created_on > created_after)
        if modified_before is not None:
            and_query.append(BaseResultORM.modified_on < modified_before)
        if modified_after is not None:
            and_query.append(BaseResultORM.modified_on > modified_after)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(BaseResultORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)

            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ProcedureDict]]:
        """
        Obtain results of single computations from with specified IDs from an existing session

        The returned information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        id
            A list or other sequence of result IDs
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing results will be tolerated, and the returned list of
           Molecules will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Single result information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} single results is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        load_cols, load_rels = get_query_proj_columns(BaseResultORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(BaseResultORM).filter(BaseResultORM.id.in_(unique_ids)).options(load_only(*load_cols))

            for r in load_rels:
                query = query.options(selectinload(r))

            results = query.yield_per(100)
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested single result records")

            return ret

    def create(
        self, molecules: List[Molecule], specification: AllProcedureSpecifications
    ) -> Tuple[InsertMetadata, List[Optional[ObjectId]]]:
        """
        Create procedures as well as tasks

        Parameters
        ----------
        molecules
        specification

        Returns
        -------

        """

        # The procedure should have been checked by the pydantic model
        procedure_handler = self.handler_map[specification.procedure]

        # Verify the procedure
        procedure_handler.validate_input(specification)

        # Add all the molecules stored in the 'data' member
        # This should apply to all procedures
        molecule_meta, molecule_ids = self._core_socket.molecule.add_mixed(molecules)

        # Only do valid molecule ids (ie, not None in the returned list)
        # These would correspond to errors
        # TODO - INT ID
        valid_molecule_ids = [int(x) for x in molecule_ids if x is not None]
        valid_molecule_idx = [idx for idx, x in enumerate(molecule_ids) if x is not None]
        invalid_molecule_idx = [idx for idx, x in enumerate(molecule_ids) if x is None]

        # Create procedures and tasks in the same session
        # This will be committed only at the end
        with self._core_socket.session_scope() as session:
            meta, ids = procedure_handler.create_procedures(session, valid_molecule_ids, specification)

            if not meta.success:
                # The above should always succeed if the model was validated. If not, that is
                # an internal error
                # This will rollback the session
                raise RuntimeError(f"Error adding procedures: {meta.error_string}")

            # only create tasks for new procedures
            new_ids = [ids[i] for i in meta.inserted_idx]

            new_orm = (
                session.query(BaseResultORM).filter(BaseResultORM.id.in_(new_ids)).options(selectinload("*")).all()
            )

            # These should only correspond to the same procedure we added
            assert all(x.procedure == specification.procedure for x in new_orm)

            # Create tasks for everything
            task_meta, _ = procedure_handler.create_tasks(session, new_orm, specification.tag, specification.priority)

            if not task_meta.success:
                # In general, create_tasks should always succeed
                # If not, that is usually a programmer error
                # This will rollback the session
                raise RuntimeError(f"Error adding tasks: {task_meta.error_string}")

            # session will commit when closed at the end of this 'with' block

        # Place None in the ids list where molecules were None
        for idx in invalid_molecule_idx:
            ids.insert(idx, None)

        # Now adjust the index lists in the metadata to correspond to the original molecule order
        inserted_idx = [valid_molecule_idx[x] for x in meta.inserted_idx]
        existing_idx = [valid_molecule_idx[x] for x in meta.existing_idx]
        errors = [(valid_molecule_idx[x], msg) for x, msg in meta.errors] + molecule_meta.errors

        return InsertMetadata(inserted_idx=inserted_idx, existing_idx=existing_idx, errors=errors), ids  # type: ignore

    def update_completed(self, manager_name: str, results: Dict[ObjectId, AllResultTypes]):
        """
        Insert data from completed calculations into the database

        Parameters
        ----------
        manager_name
            The name of the manager submitting the results
        results
            Results (in QCSchema format), with the task_id as the key
        """

        all_task_ids = list(int(x) for x in results.keys())

        self._logger.info("Task Queue: Received completed tasks from {}.".format(manager_name))
        self._logger.info("            Task ids: " + " ".join(str(x) for x in all_task_ids))

        # Obtain all ORM for the task queue at once
        # Can be expensive, but probably faster than one-by-one
        with self._core_socket.session_scope() as session:
            task_success = 0
            task_failures = 0
            task_totals = len(results.items())

            for task_id, result in results.items():

                # We load one at a time. This works well with 'with_for_update'
                # which will do row locking. This lock is released on commit or rollback
                task_orm: Optional[TaskQueueORM] = (
                    session.query(TaskQueueORM)
                    .filter(TaskQueueORM.id == task_id)
                    .options(joinedload(TaskQueueORM.base_result_obj))
                    .with_for_update()
                    .one_or_none()
                )

                # Does the task exist?
                if task_orm is None:
                    self._logger.warning(f"Task id {task_id} does not exist in the task queue.")
                    task_failures += 1
                    continue

                base_result_id = task_orm.base_result_id

                try:
                    #################################################################
                    # Perform some checks for consistency
                    #################################################################
                    # Is the task in the running state
                    # If so, do not attempt to modify the task queue. Just move on
                    if task_orm.status != TaskStatusEnum.running:
                        self._logger.warning(f"Task id {task_id} is not in the running state.")
                        task_failures += 1

                    # Is the base result already marked complete? if so, this is a problem
                    # This should never happen, so log at level of "error"
                    if task_orm.base_result_obj.status == RecordStatusEnum.complete:
                        self._logger.error(f"Base result {base_result_id} (task id {task_id}) is already complete!")

                        # Go ahead and delete the task
                        session.delete(task_orm)
                        session.commit()
                        task_failures += 1

                    # Was the manager that sent the data the one that was assigned?
                    # If so, do not attempt to modify the task queue. Just move on
                    elif task_orm.manager != manager_name:
                        self._logger.warning(
                            f"Task id {task_id} belongs to {task_orm.manager}, not manager {manager_name}"
                        )
                        task_failures += 1

                    # Failed task returning FailedOperation
                    elif result.success is False and isinstance(result, FailedOperation):
                        self.failure.update_completed(session, task_orm, manager_name, result)

                        # Update the task object
                        task_orm.status = TaskStatusEnum.error
                        task_orm.modified_on = task_orm.base_result_obj.modified_on
                        session.commit()
                        self._core_socket.notify_completed_watch(base_result_id, RecordStatusEnum.error)

                        task_failures += 1

                    elif result.success is not True:
                        # QCEngine should always return either FailedOperation, or some result with success == True
                        msg = f"Unexpected return from manager for task {task_id} base result {base_result_id}: Returned success != True, but not a FailedOperation"
                        error = {"error_type": "internal_fractal_error", "error_message": msg}
                        failed_op = FailedOperation(error=error, success=False)

                        self.failure.update_completed(session, task_orm, manager_name, failed_op)
                        task_orm.status = TaskStatusEnum.error
                        task_orm.modified_on = datetime.utcnow()
                        session.commit()
                        self._core_socket.notify_completed_watch(base_result_id, RecordStatusEnum.error)

                        self._logger.error(msg)
                        task_failures += 1

                    # Manager returned a full, successful result
                    else:
                        parser = self.handler_map[task_orm.base_result_obj.procedure]
                        parser.update_completed(session, task_orm, manager_name, result)

                        # Delete the task from the task queue since it is completed
                        session.delete(task_orm)
                        session.commit()
                        self._core_socket.notify_completed_watch(base_result_id, RecordStatusEnum.complete)

                        task_success += 1

                except Exception:
                    # We have no idea what was added or is pending for removal
                    # So rollback the transaction to the most recent commit
                    session.rollback()

                    msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                    error = {"error_type": "internal_fractal_error", "error_message": msg}
                    failed_op = FailedOperation(error=error, success=False)

                    self.failure.update_completed(session, task_orm, manager_name, failed_op)
                    session.commit()
                    self._core_socket.notify_completed_watch(base_result_id, RecordStatusEnum.error)

                    self._logger.error(msg)
                    task_failures += 1

        self._logger.info(
            "Task Queue: Processed {} complete tasks ({} successful, {} failed).".format(
                task_totals, task_success, task_failures
            )
        )

        # Update manager logs
        self._core_socket.manager.update(manager_name, completed=task_totals, failures=task_failures)
