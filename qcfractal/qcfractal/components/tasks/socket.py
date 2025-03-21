from __future__ import annotations

import logging
import traceback
from collections import defaultdict
from typing import TYPE_CHECKING

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from qcelemental.models import FailedOperation
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import joinedload, load_only, lazyload

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.record_db_models import BaseRecordORM
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
        self._strict_queue_tags = root_socket.qcf_config.strict_queue_tags

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
            manager: Optional[ComputeManagerORM] = session.execute(stmt).scalar_one_or_none()

            if manager is None:
                self._logger.warning(f"Manager {manager_name} does not exist, but is trying to return tasks. Ignoring.")
                raise ComputeManagerError(f"Manager {manager_name} does not exist")

            if manager.status != ManagerStatusEnum.active:
                self._logger.warning(f"Manager {manager_name} is not active. Ignoring...")
                raise ComputeManagerError(f"Manager {manager_name} is not active")

            # For automatic resetting
            to_be_reset: List[int] = []

            # We load basic record & task info for all returned tasks with row-level locking.
            # This lock is released on commit or rollback.
            all_task_ids = list(results_compressed.keys())
            stmt = select(
                TaskQueueORM.id,
                BaseRecordORM.id,
                BaseRecordORM.record_type,
                BaseRecordORM.status,
                BaseRecordORM.manager_name,
            )
            stmt = stmt.join(TaskQueueORM, TaskQueueORM.record_id == BaseRecordORM.id)
            stmt = stmt.where(TaskQueueORM.id.in_(all_task_ids))
            stmt = stmt.with_for_update(skip_locked=False)

            all_record_info = session.execute(stmt).all()
            all_record_info = {x[0]: x[1:] for x in all_record_info}

            for task_id, result_compressed in results_compressed.items():

                record_info = all_record_info.get(task_id, None)

                #################################################################
                # Perform some checks for consistency
                # These are simple rejections and don't modify anything
                # record-related in the database
                #################################################################
                if record_info is None:
                    self._logger.warning(f"Task id {task_id} does not exist in the task queue")
                    tasks_rejected.append((task_id, "Task does not exist in the task queue"))
                    continue

                record_id, record_type, record_status, record_manager_name = record_info

                # Is the task in the running state
                # If so, do not attempt to modify the task queue. Just move on
                if record_status != RecordStatusEnum.running:
                    self._logger.warning(f"Record {record_id} (task {task_id}) is not in a running state")
                    tasks_rejected.append((task_id, "Task is not in a running state"))
                    continue

                # Was the manager that sent the data the one that was assigned?
                # If so, do not attempt to modify the task queue. Just move on
                if record_manager_name != manager_name:
                    self._logger.warning(
                        f"Record {record_id} (task {task_id}) claimed by {record_manager_name}, not {manager_name}"
                    )
                    tasks_rejected.append((task_id, "Task is claimed by another manager"))
                    continue

                result_dict = decompress(result_compressed, CompressionEnum.zstd)
                result = pydantic.parse_obj_as(AllResultTypes, result_dict)

                notify_status = None

                ##################################################################
                # The rest of the checks are done in a try/except block because
                # they are much more complicated and can result in exceptions
                # which should be handled
                ##################################################################

                try:
                    savepoint = session.begin_nested()

                    # Failed task returning FailedOperation
                    if result.success is False and isinstance(result, FailedOperation):
                        self.root_socket.records.update_failed_task(session, record_id, result, manager_name)

                        notify_status = RecordStatusEnum.error
                        tasks_failures.append(task_id)

                        # Should we automatically reset?
                        if self.root_socket.qcf_config.auto_reset.enabled:
                            # TODO - Move to update_failed_task?
                            stmt = select(BaseRecordORM).where(BaseRecordORM.id == record_id)
                            record_orm = session.execute(stmt).scalar_one()
                            if should_reset(record_orm, self.root_socket.qcf_config.auto_reset):
                                to_be_reset.append(record_id)

                    elif result.success is not True:
                        # QCEngine should always return either FailedOperation, or some result with success == True
                        msg = f"Unexpected return from manager for task {task_id}/base result {record_id}: Returned success != True, but not a FailedOperation"
                        error = {"error_type": "internal_fractal_error", "error_message": msg}
                        failed_op = FailedOperation(error=error, success=False)

                        self.root_socket.records.update_failed_task(session, record_id, failed_op, manager_name)
                        notify_status = RecordStatusEnum.error

                        self._logger.error(msg)
                        tasks_rejected.append((task_id, "Returned success=False, but not a FailedOperation"))

                    # Manager returned a full, successful result
                    else:
                        self.root_socket.records.update_completed_task(
                            session, record_id, record_type, result, manager_name
                        )

                        notify_status = RecordStatusEnum.complete
                        tasks_success.append(task_id)

                    savepoint.commit()  # Release the savepoint (doesn't actually fully commit)

                except Exception:
                    # We have no idea what was added or is pending for removal
                    # So rollback the transaction to the most recent commit
                    savepoint.rollback()
                    savepoint = session.begin_nested()

                    msg = "Internal FractalServer Error:\n" + traceback.format_exc()
                    error = {"error_type": "internal_fractal_error", "error_message": msg}
                    failed_op = FailedOperation(error=error, success=False)

                    self.root_socket.records.update_failed_task(session, record_id, failed_op, manager_name)
                    notify_status = RecordStatusEnum.error

                    self._logger.error(msg)
                    tasks_rejected.append((task_id, "Internal server error"))

                    savepoint.commit()

                finally:
                    # Send notifications that tasks were completed
                    # Notifications are sent after the transaction is committed
                    if notify_status is not None:
                        self.root_socket.notify_finished_watch(record_id, notify_status)

            session.commit()

            # Update the stats for the manager
            manager.successes += len(tasks_success)
            manager.failures += len(tasks_failures)
            manager.rejected += len(tasks_rejected)

            # Mark that we have heard from the manager
            manager.modified_on = now_at_utc()

            # Automatically reset ones that should be reset
            if self.root_socket.qcf_config.auto_reset.enabled and to_be_reset:
                self._logger.info(f"Auto resetting {len(to_be_reset)} records")
                self.root_socket.records.reset(to_be_reset, session=session)

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
        compute_tags: List[str],
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
        compute_tags
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

        # Force given tags and programs to be lower case
        compute_tags = [tag.lower() for tag in compute_tags]
        programs = {key.lower(): [x.lower() for x in value] for key, value in programs.items()}

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

            # Remove tags & programs that we didn't say we handled
            # (order is important for tags)
            search_programs = array(p for p in programs.keys() if p in manager.programs.keys())
            search_tags = [x for x in compute_tags if x in manager.compute_tags]

            if len(search_programs) == 0:
                self._logger.warning(f"Manager {manager_name} did not send any valid programs to claim")
                raise ComputeManagerError(f"Manager {manager_name} did not send any valid programs to claim")

            if len(search_tags) == 0:
                self._logger.warning(f"Manager {manager_name} did not send any valid queue tags to claim")
                raise ComputeManagerError(f"Manager {manager_name} did not send any valid queue tags to claim")

            found: Dict[int, Dict[str, Any]] = {}
            return_order: List[int] = []  # Order of task ids

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

                # This is a very tricky query. We only want to load two columns of BaseRecord, but we need to join.
                # Sqlalchemy likes to load more columns if there is a relationship. So we join outside of the
                # relationship (ie, NOT TaskQueueORM.record) and then load the columns we want.
                stmt = select(TaskQueueORM, BaseRecordORM).join(
                    BaseRecordORM, BaseRecordORM.id == TaskQueueORM.record_id
                )

                stmt = stmt.filter(TaskQueueORM.available == True)
                stmt = stmt.filter(search_programs.contains(TaskQueueORM.required_programs))
                stmt = stmt.options(load_only(BaseRecordORM.id, BaseRecordORM.record_type))
                stmt = stmt.options(lazyload(BaseRecordORM.owner_user), lazyload(BaseRecordORM.owner_group))

                # Order by priority, then date (earliest first)
                # The sort_date usually comes from the created_on of the record, or the created_on of the record's parent service
                stmt = stmt.order_by(
                    TaskQueueORM.compute_priority.desc(), TaskQueueORM.sort_date.asc(), TaskQueueORM.id.asc()
                )

                # If tag is "*" (and strict_queue_tags is False), then the manager can pull anything
                # If tag is "*" and strict_queue_tags is enabled, only pull tasks with tag == '*'
                if tag != "*" or self._strict_queue_tags:
                    stmt = stmt.filter(TaskQueueORM.compute_tag == tag)

                # Skip locked rows - They may be in the process of being claimed by someone else
                stmt = stmt.limit(new_limit).with_for_update(of=[BaseRecordORM, TaskQueueORM], skip_locked=True)

                new_items = session.execute(stmt).all()

                # Update all the task records to reflect this manager claiming them
                task_ids = [x[0].id for x in new_items]
                record_ids = [x[1].id for x in new_items]

                return_order.extend(task_ids)  # Keep the order as returned from the db
                stmt = update(TaskQueueORM).where(TaskQueueORM.id.in_(task_ids)).values(available=False)
                session.execute(stmt)

                stmt = (
                    update(BaseRecordORM)
                    .where(BaseRecordORM.id.in_(record_ids))
                    .values(status=RecordStatusEnum.running, manager_name=manager_name, modified_on=now_at_utc())
                )
                session.execute(stmt)

                # Store in dict form for returning, but no need to store the info from the base record
                # Also, retrieve the actual function kwargs. Eventually we may want the managers
                # to retrieve the kwargs themselves
                tasks_to_generate = defaultdict(list)
                task_updates = []

                # Find what tasks need their function and kwargs generated
                # Otherwise, just add them to the returned list
                for task_orm, record_orm in new_items:
                    if task_orm.function is None:
                        tasks_to_generate[record_orm.record_type].append(task_orm)
                    else:
                        found[task_orm.id] = task_orm.model_dict(exclude=["record"])

                # Create the task data on the fly if it doesn't exist
                for record_type, tasks_orm in tasks_to_generate.items():
                    record_socket = self.root_socket.records.get_socket(record_type)

                    record_ids = [task_orm.record_id for task_orm in tasks_orm]
                    task_specs = record_socket.generate_task_specifications(session, record_ids)

                    for task_orm, task_spec in zip(tasks_orm, task_specs):
                        task_dict = task_orm.model_dict(exclude=["record"])

                        kwargs = task_spec["function_kwargs"]
                        kwargs_compressed, _, _ = compress(kwargs, CompressionEnum.zstd)

                        # Add this to the orm for any future managers claiming this task
                        task_updates.append(
                            {
                                "id": task_orm.id,
                                "function": task_spec["function"],
                                "function_kwargs_compressed": kwargs_compressed,
                            }
                        )

                        # But just use what we created when returning to this manager
                        task_dict["function"] = task_spec["function"]
                        task_dict["function_kwargs_compressed"] = kwargs_compressed
                        found[task_orm.id] = task_dict

                # Update the task records with the function and kwargs
                if task_updates:
                    session.execute(update(TaskQueueORM), task_updates)

                session.flush()

            manager.claimed += len(found)

            # Mark that we have heard from the manager
            manager.modified_on = now_at_utc()

            self._logger.info(f"Manager {manager_name} has claimed {len(found)} new tasks")

        return [found[i] for i in return_order]
