from __future__ import annotations

from datetime import datetime as dt
import traceback
import logging
from qcfractal.storage_sockets.models import (
    BaseResultORM,
    TaskQueueORM,
    OptimizationProcedureORM,
    TorsionDriveProcedureORM,
    GridOptimizationProcedureORM,
)
from sqlalchemy.orm import joinedload, selectinload, load_only
from qcfractal.interface.models import (
    TaskStatusEnum,
    FailedOperation,
    RecordStatusEnum,
    InsertMetadata,
    AllProcedureSpecifications,
)
from qcfractal.storage_sockets.storage_utils import get_metadata_template
from ..sqlalchemy_common import get_query_proj_columns
from qcfractal.storage_sockets.sqlalchemy_socket import (
    format_query,
    calculate_limit,
)

from typing import TYPE_CHECKING

from .procedures import BaseProcedureHandler, FailedOperationHandler, SingleResultHandler, OptimizationHandler

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from qcfractal.interface.models import ObjectId, AllResultTypes, Molecule
    from typing import List, Dict, Union, Tuple, Optional, Sequence, Any

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
        id: Union[str, List] = None,
        procedure: str = None,
        program: str = None,
        hash_index: str = None,
        task_id: Union[str, List] = None,
        manager_id: Union[str, List] = None,
        status: str = "COMPLETE",
        include=None,
        exclude=None,
        limit: int = None,
        skip: int = 0,
        return_json=True,
        with_ids=True,
    ):
        """

        Parameters
        ----------
        id : str or List[str]
        procedure : str
        program : str
        hash_index : str
        task_id : str or List[str]
        status : bool, optional
            The status of the result: 'COMPLETE', 'INCOMPLETE', or 'ERROR'
            Default is 'COMPLETE'
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
            skip the first 'skip' resaults. Used to paginate
            Default is 0
        return_json : bool, optional
            Return the results as a list of json inseated of objects
            Default is True
        with_ids : bool, optional
            Include the ids in the returned objects/dicts
            Default is True

        Returns
        -------
        Dict[str, Any]
            Dict with keys: data and meta. Data is the objects found
        """

        limit = calculate_limit(self._limit, limit)
        meta = get_metadata_template()

        if id is not None or task_id is not None:
            status = None

        if procedure == "optimization":
            className = OptimizationProcedureORM
        elif procedure == "torsiondrive":
            className = TorsionDriveProcedureORM
        elif procedure == "gridoptimization":
            className = GridOptimizationProcedureORM
        else:
            # raise TypeError('Unsupported procedure type {}. Id: {}, task_id: {}'
            #                 .format(procedure, id, task_id))
            className = BaseResultORM  # all classes, including those with 'selectin'
            program = None  # make sure it's not used
            if id is None:
                # TODO - should be handled in pydantic model validation
                self._logger.error(f"Procedure type not specified({procedure}), and ID is not given.")
                raise KeyError("ID is required if procedure type is not specified.")

        query = format_query(
            className,
            id=id,
            procedure=procedure,
            program=program,
            hash_index=hash_index,
            task_id=task_id,
            manager_id=manager_id,
            status=status,
        )

        data = []
        try:
            # TODO: decide a way to find the right type

            data, meta["n_found"] = self._core_socket.get_query_projection(
                className, query, limit=limit, skip=skip, include=include, exclude=exclude
            )
            meta["success"] = True
        except Exception as err:
            meta["error_description"] = str(err)

        return {"data": data, "meta": meta}

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

        # The existence of the procedure should have been checked by the pydantic model
        procedure_handler = self.handler_map[specification.procedure]

        # Verify the procedure. Will raise exception  on error
        procedure_handler.verify_input(specification)

        # Add all the molecules stored in the 'data' member
        # This should apply to all procedures
        molecule_meta, molecule_ids = self._core_socket.molecule.add_mixed(molecules)

        # Only do valid molecule ids (ie, not None in the returned list)
        # These would correspond to errors

        # TODO - INT ID
        valid_molecule_ids = [int(x) for x in molecule_ids if x is not None]
        valid_molecule_idx = [idx for idx, x in enumerate(molecule_ids) if x is not None]

        with self._core_socket.session_scope() as session:
            meta, ids = procedure_handler.create(session, valid_molecule_ids, specification)

        # Place None in the ids list where molecules were None
        for idx, x in enumerate(molecule_ids):
            if x is None:
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
                        task_orm.modified_on = dt.utcnow()
                        session.commit()
                        self._core_socket.notify_completed_watch(base_result_id, RecordStatusEnum.error)

                        self._logger.error(msg)
                        task_failures += 1

                    # Manager returned a full, successful result
                    else:
                        parser = self.handler_map[task_orm.parser]
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
