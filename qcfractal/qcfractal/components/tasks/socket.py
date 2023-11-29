from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from qcelemental.models import FailedOperation
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import joinedload, aliased, Load

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcportal.all_results import AllResultTypes
from qcportal.compression import CompressionEnum, compress
from qcportal.compression import decompress
from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerStatusEnum
from qcportal.metadata_models import TaskReturnMetadata
from qcportal.record_models import RecordStatusEnum
from qcportal.utils import calculate_limit, now_at_utc
from .db_models import TaskQueueORM
from .reset_logic import should_reset

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import List, Dict, Tuple, Optional, Any


class TaskSocket:
    """
    Socket for managing tasks
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self._tasks_claim_limit = root_socket.qcf_config.api_limits.manager_tasks_claim

    def update_finished(
        self, manager_name: str, results_compressed: Dict[int, bytes], *, session: Optional[Session] = None
    ) -> TaskReturnMetadata:
        """
        Insert data from finished calculations into the database

        Parameters
        ----------
        manager_name
            The name of the manager submitting the results
        results_compressed
            Results (in QCSchema format), with the task_id as the key
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        all_task_ids = list(results_compressed.keys())

        self._logger.info("Received completed tasks from {}.".format(manager_name))
        self._logger.info("    Task ids: " + " ".join(str(x) for x in all_task_ids))

        tasks_success: List[int] = []
        tasks_failures: List[int] = []
        tasks_rejected: List[Tuple[int, str]] = []

        with self.root_socket.optional_session(session) as session:
            stmt = select(ComputeManagerORM).where(ComputeManagerORM.name == manager_name)
            stmt = stmt.with_for_update(skip_locked=False)
            manager: Optional[ComputeManagerORM] = session.execute(stmt).scalar_one_or_none()

            if manager is None:
                self._logger.warning(f"Manager {manager_name} does not exist, but is trying to return tasks. Ignoring.")
                raise ComputeManagerError(f"Manager {manager_name} does not exist")

            if manager.status != ManagerStatusEnum.active:
                self._logger.warning(f"Manager {manager_name} is not active. Ignoring...")
                raise ComputeManagerError(f"Manager {manager_name} is not active")

            all_notifications: List[Tuple[int, RecordStatusEnum]] = []

            # For automatic resetting
            to_be_reset: List[int] = []

            for task_id, result_compressed in results_compressed.items():
                result_dict = decompress(result_compressed, CompressionEnum.zstd)
                result = pydantic.parse_obj_as(AllResultTypes, result_dict)

                # We load one at a time. This works well with 'with_for_update'
                # which will do row locking. This lock is released on commit or rollback
                # We are also deferring loading of the specific record tables. These will be lazy loaded
                # when they are needed in the update functions of the various record subsockets.
                # (I tried to use with_polymorphic, but it's kind of fussy and doesn't work well with innerjoin
                #  which is needed because with_for_update doesn't work with nullable left outer joins. This should
                #  be ok, even if the second select call doesn't use with_for_update, because any loading of
                #  a derived-class orm will need to access base_record, which I believe will be locked)
                stmt = select(TaskQueueORM).filter(TaskQueueORM.id == task_id)
                stmt = stmt.options(joinedload(TaskQueueORM.record, innerjoin=True))
                stmt = stmt.with_for_update(skip_locked=False)

                task_orm: Optional[TaskQueueORM] = session.execute(stmt).scalar_one_or_none()

                # Does the task exist?
                if task_orm is None:
                    self._logger.warning(f"Task id {task_id} does not exist in the task queue")
                    tasks_rejected.append((task_id, "Task does not exist in the task queue"))
                    continue

                record_orm: BaseRecordORM = task_orm.record
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
                        self.root_socket.records.update_failed_task(session, record_orm, result, manager_name)
                        notify_status = RecordStatusEnum.error
                        tasks_failures.append(task_id)

                        # Should we automatically reset?
                        if self.root_socket.qcf_config.auto_reset.enabled:
                            if should_reset(record_orm, self.root_socket.qcf_config.auto_reset):
                                to_be_reset.append(record_id)

                    elif result.success is not True:
                        # QCEngine should always return either FailedOperation, or some result with success == True
                        msg = f"Unexpected return from manager for task {task_id}/base result {record_id}: Returned success != True, but not a FailedOperation"
                        error = {"error_type": "internal_fractal_error", "error_message": msg}
                        failed_op = FailedOperation(error=error, success=False)

                        self.root_socket.records.update_failed_task(session, record_orm, failed_op, manager_name)
                        notify_status = RecordStatusEnum.error

                        self._logger.error(msg)
                        tasks_rejected.append((task_id, "Returned success=False, but not a FailedOperation"))

                    # Manager returned a full, successful result
                    else:
                        self.root_socket.records.update_completed_task(session, record_orm, result, manager_name)

                        notify_status = RecordStatusEnum.complete
                        tasks_success.append(task_id)

                except Exception:
                    # We have no idea what was added or is pending for removal
                    # So rollback the transaction to the most recent commit
                    nested_session.rollback()

                    # Need a new nested transaction - previous one is dead
                    nested_session = session.begin_nested()

                    msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                    error = {"error_type": "internal_fractal_error", "error_message": msg}
                    failed_op = FailedOperation(error=error, success=False)

                    self.root_socket.records.update_failed_task(session, record_orm, failed_op, manager_name)
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

            session.flush()

            # Automatically reset ones that should be reset
            if self.root_socket.qcf_config.auto_reset.enabled and to_be_reset:
                self._logger.info(f"Auto resetting {len(to_be_reset)} records")
                self.root_socket.records.reset(to_be_reset, session=session)

        # Send notifications that tasks were completed
        for record_id, notify_status in all_notifications:
            self.root_socket.notify_finished_watch(record_id, notify_status)

        self._logger.info(
            "Processed {} returned tasks ({} successful, {} failed, {} rejected).".format(
                len(results_compressed), len(tasks_success), len(tasks_failures), len(tasks_rejected)
            )
        )

        return TaskReturnMetadata(rejected_info=tasks_rejected, accepted_ids=(tasks_success + tasks_failures))

    def claim_tasks(
        self,
        manager_name: str,
        programs: Dict[str, List[str]],
        tags: List[str],
        limit: Optional[int] = None,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """Claim/assign tasks for a manager

        Given tags and available programs/procedures on the manager, obtain
        waiting tasks to run.

        Parameters
        ----------
        manager_name
            Name of the manager requesting tasks
        tags
            Subset of tags to claim tasks for
        limit
            Maximum number of tasks that the manager can claim
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        # Normally, checking limits is done in the route code. However, we really do not want a manager
        # to claim absolutely everything. So double check here
        limit = calculate_limit(self._tasks_claim_limit, limit)

        # CTE for finding the created_on from services which this record is a dependency of
        # If a record is a dependency of a service, we use either the record's created_on or the service's created_on,
        # whichever is earlier. That way a service doesn't have to wait for all other services to finish their tasks
        # before it can finish.
        br_task = aliased(BaseRecordORM)  # BaseRecord for the task
        br_svc = aliased(BaseRecordORM)  # BaseRecord for services

        least_date = func.least(br_task.created_on, func.min(br_svc.created_on)).label("created_on")
        svcdate_cte = select(br_task.id.label("record_id"), least_date)
        svcdate_cte = svcdate_cte.join(ServiceDependencyORM, ServiceDependencyORM.record_id == br_task.id)
        svcdate_cte = svcdate_cte.join(ServiceQueueORM, ServiceQueueORM.id == ServiceDependencyORM.service_id)
        svcdate_cte = svcdate_cte.join(br_svc, br_svc.id == ServiceQueueORM.record_id)
        svcdate_cte = svcdate_cte.where(br_task.status == RecordStatusEnum.waiting)
        svcdate_cte = svcdate_cte.group_by(br_task.id)
        svcdate_cte = svcdate_cte.order_by(least_date.asc())
        svcdate_cte = svcdate_cte.cte()

        with self.root_socket.optional_session(session) as session:
            stmt = select(ComputeManagerORM).where(ComputeManagerORM.name == manager_name)
            stmt = stmt.with_for_update(skip_locked=False)
            manager: Optional[ComputeManagerORM] = session.execute(stmt).scalar_one_or_none()

            if manager is None:
                self._logger.warning(f"Manager {manager_name} does not exist! Will not give it tasks")
                raise ComputeManagerError("Manager does not exist!")
            elif manager.status != ManagerStatusEnum.active:
                self._logger.warning(f"Manager {manager_name} exists but is not active! Will not give it tasks")
                raise ComputeManagerError("Manager is not active!")

            manager_programs = array(programs.keys())
            found: List[Dict[str, Any]] = []

            # Remove tags that we didn't say we handled, but keep the order
            search_tags = [x for x in tags if x in manager.tags]
            for tag in search_tags:
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
                # the TaskQueueORM.record gets populated
                # See https://docs-sqlalchemy.readthedocs.io/ko/latest/orm/loading_relationships.html#routing-explicit-joins-statements-into-eagerly-loaded-collections
                stmt = select(TaskQueueORM, BaseRecordORM).join(TaskQueueORM.record)

                # Only load a few columns we need of the record
                stmt = stmt.options(
                    Load(BaseRecordORM).load_only(
                        BaseRecordORM.status, BaseRecordORM.manager_name, BaseRecordORM.modified_on
                    )
                )

                stmt = stmt.join(svcdate_cte, svcdate_cte.c.record_id == BaseRecordORM.id, isouter=True)
                stmt = stmt.filter(BaseRecordORM.status == RecordStatusEnum.waiting)
                stmt = stmt.filter(manager_programs.contains(TaskQueueORM.required_programs))

                # Order by priority, then created_on (earliest first)
                # Where the created_on may be the created_on of the parent service (see CTE above)
                stmt = stmt.order_by(
                    TaskQueueORM.priority.desc(), func.least(BaseRecordORM.created_on, svcdate_cte.c.created_on).asc()
                )

                # If tag is "*", then the manager will pull anything
                if tag != "*":
                    stmt = stmt.filter(TaskQueueORM.tag == tag)

                # Skip locked rows - They may be in the process of being claimed by someone else
                stmt = stmt.limit(new_limit).with_for_update(of=[BaseRecordORM, TaskQueueORM], skip_locked=True)

                new_items = session.execute(stmt).all()

                # Update all the task records to reflect this manager claiming them
                for _, record_orm in new_items:
                    record_orm.status = RecordStatusEnum.running
                    record_orm.manager_name = manager_name
                    record_orm.modified_on = now_at_utc()

                # Store in dict form for returning, but no need to store the info from the base record
                # Also, retrieve the actual function kwargs. Eventually we may want the managers
                # to retrieve the kwargs themselves
                for task_orm, _ in new_items:
                    task_dict = task_orm.model_dict(exclude=["record"])

                    if task_orm.function is None:
                        # Generate the task on the fly
                        task_spec = self.root_socket.records.generate_task_specification(task_orm)

                        kwargs = task_spec["function_kwargs"]
                        kwargs_compressed, _, _ = compress(kwargs, CompressionEnum.zstd)

                        # Add this to the orm for any future managers claiming this task
                        task_orm.function = task_spec["function"]
                        task_orm.function_kwargs_compressed = kwargs_compressed

                        # But just use what we created when returning to this manager
                        task_dict["function"] = task_spec["function"]
                        task_dict["function_kwargs_compressed"] = kwargs_compressed

                    found.append(task_dict)

                session.flush()

            manager.claimed += len(found)

            self._logger.info(f"Manager {manager_name} has claimed {len(found)} new tasks")

        return found
