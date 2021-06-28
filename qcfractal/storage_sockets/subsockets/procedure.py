from __future__ import annotations

from datetime import datetime
import traceback
import logging
from qcfractal.storage_sockets.models import (
    BaseResultORM,
    TaskQueueORM,
)
from sqlalchemy.orm import joinedload, selectinload
from qcfractal.interface.models import (
    PriorityEnum,
    FailedOperation,
    RecordStatusEnum,
    InsertMetadata,
    AllProcedureSpecifications,
)

from typing import TYPE_CHECKING

from .procedures import BaseProcedureHandler, FailedOperationHandler, SingleResultHandler, OptimizationHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from qcfractal.interface.models import ObjectId, AllResultTypes, Molecule
    from typing import List, Dict, Tuple, Optional, Any, Iterable

    ProcedureDict = Dict[str, Any]


class ProcedureSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)

        # Subsubsockets/handlers
        self.single = SingleResultHandler(core_socket)
        self.optimization = OptimizationHandler(core_socket)
        self.failure = FailedOperationHandler(core_socket)

        self.handler_map: Dict[str, BaseProcedureHandler] = {
            "single": self.single,
            "optimization": self.optimization,
            "failure": self.failure,
        }

    def create(
        self, molecules: List[Molecule], specification: AllProcedureSpecifications, *, session: Optional[Session] = None
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
        with self._core_socket.optional_session(session) as session:
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

    def regenerate_tasks(
        self,
        id: Iterable[ObjectId],
        tag: Optional[str] = None,
        priority: PriorityEnum = PriorityEnum.normal,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ObjectId]]:
        """
        Regenerates tasks for the given procedures

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            List of new task ids, in the order of the input ids. If a base result does not exist, or is marked completed,
            or already has a task associated with it, then the corresponding entry will be None
        """

        task_ids: List[Optional[ObjectId]] = []

        with self._core_socket.optional_session(session) as session:
            # Slow, but this shouldn't be called too often
            for record_id in id:
                record = (
                    session.query(BaseResultORM)
                    .filter(BaseResultORM.id == record_id)
                    .options(joinedload(BaseResultORM.task_obj))
                    .one_or_none()
                )

                # Base result doesn't exist
                if record is None:
                    task_ids.append(None)
                    continue

                # Task entry already exists
                if record.task_obj is not None:
                    task_ids.append(None)
                    continue

                # Result is already complete
                if record.status == RecordStatusEnum.complete:
                    task_ids.append(None)
                    continue

                # Now actually create the task
                procedure_handler = self.handler_map[record.procedure]
                task_meta, new_task_ids = procedure_handler.create_tasks(session, [record], tag, priority)

                if not task_meta.success:
                    # In general, create_tasks should always succeed
                    # If not, that is usually a programmer error
                    # This will rollback the session
                    raise RuntimeError(f"Error adding tasks: {task_meta.error_string}")

                record.status = RecordStatusEnum.waiting
                record.modified_on = datetime.utcnow()
                task_ids.extend(new_task_ids)

        return task_ids

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

                base_result: BaseResultORM = task_orm.base_result_obj
                base_result_id = base_result.id

                try:
                    #################################################################
                    # Perform some checks for consistency
                    #################################################################
                    # Is the task in the running state
                    # If so, do not attempt to modify the task queue. Just move on
                    if task_orm.base_result_obj.status != RecordStatusEnum.running:
                        print("*" * 100)
                        print(task_orm.base_result_obj.status)
                        self._logger.warning(
                            f"Task {task_id}/base result {base_result_id} is not in the running state."
                        )
                        task_failures += 1

                    # Was the manager that sent the data the one that was assigned?
                    # If so, do not attempt to modify the task queue. Just move on
                    elif base_result.manager_name != manager_name:
                        self._logger.warning(
                            f"Task {task_id}/base result {base_result_id} belongs to {base_result.manager_name}, not manager {manager_name}"
                        )
                        task_failures += 1

                    # Failed task returning FailedOperation
                    elif result.success is False and isinstance(result, FailedOperation):
                        self.failure.update_completed(session, task_orm, manager_name, result)
                        session.commit()
                        self._core_socket.notify_completed_watch(base_result_id, RecordStatusEnum.error)

                        task_failures += 1

                    elif result.success is not True:
                        # QCEngine should always return either FailedOperation, or some result with success == True
                        msg = f"Unexpected return from manager for task {task_id}/base result {base_result_id}: Returned success != True, but not a FailedOperation"
                        error = {"error_type": "internal_fractal_error", "error_message": msg}
                        failed_op = FailedOperation(error=error, success=False)

                        self.failure.update_completed(session, task_orm, manager_name, failed_op)
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
