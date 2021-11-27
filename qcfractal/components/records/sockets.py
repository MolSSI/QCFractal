from __future__ import annotations

import abc
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload, with_polymorphic

from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.db_socket.helpers import (
    get_query_proj_options,
    get_count_2,
    calculate_limit,
    get_general,
    delete_general,
)
from qcfractal.portal.metadata_models import DeleteMetadata, QueryMetadata
from qcfractal.portal.outputstore import OutputStore, OutputTypeEnum, CompressionEnum
from qcfractal.portal.records import FailedOperation, AllResultTypes, PriorityEnum, RecordStatusEnum
from .db_models import RecordComputeHistoryORM, BaseResultORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.interface.models import AllResultTypes
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Iterable, Type

    ProcedureDict = Dict[str, Any]

_default_error = {"error_type": "not_supplied", "error_message": "No error message found on task."}


def create_compute_history_entry(
    result: AllResultTypes,
) -> RecordComputeHistoryORM:
    """
    Retrieves status and (possibly compressed) outputs from a result, and creates
    a record computation history entry
    """
    logger = logging.getLogger(__name__)

    history_orm = RecordComputeHistoryORM()
    history_orm.status = "complete" if result.success else "error"
    history_orm.provenance = result.provenance.dict()
    history_orm.modified_on = datetime.utcnow()

    # Get the compressed outputs if they exist
    compressed_output = result.extras.pop("_qcfractal_compressed_outputs", None)

    if compressed_output is not None:
        all_outputs = [OutputStore(**x) for x in compressed_output]

    else:
        all_outputs = []

        # This shouldn't happen, but if they aren't compressed, check for uncompressed
        if result.stdout is not None:
            logger.warning(f"Found uncompressed stdout for record id {result.id}")
            stdout = OutputStore.compress(OutputTypeEnum.stdout, result.stdout, CompressionEnum.lzma, 1)
            all_outputs.append(stdout)
        if result.stderr is not None:
            logger.warning(f"Found uncompressed stderr for record id {result.id}")
            stderr = OutputStore.compress(OutputTypeEnum.stderr, result.stderr, CompressionEnum.lzma, 1)
            all_outputs.append(stderr)
        if result.error is not None:
            logger.warning(f"Found uncompressed error for record id {result.id}")
            error = OutputStore.compress(OutputTypeEnum.error, result.error.dict(), CompressionEnum.lzma, 1)
            all_outputs.append(error)

    history_orm.outputs = [OutputStoreORM.from_model(x) for x in all_outputs]

    return history_orm


class BaseRecordSocket(abc.ABC):
    @abc.abstractmethod
    def update_completed(
        self, session: Session, record_orm: BaseResultORM, result: AllResultTypes, manager_name: str
    ) -> None:
        pass

    @abc.abstractmethod
    def recreate_task(
        self, record_orm: BaseResultORM, tag: Optional[str] = None, priority: PriorityEnum = PriorityEnum.normal
    ) -> None:
        pass


class RecordSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.record
        self._output_limit = root_socket.qcf_config.response_limits.output_store

        # All the subsockets
        from .singlepoint.sockets import SinglePointRecordSocket

        self.singlepoint = SinglePointRecordSocket(root_socket)

        self._handler_map: Dict[str, BaseRecordSocket] = {"singlepoint": self.singlepoint}

    def query_base(
        self,
        orm_type: Type[BaseResultORM],
        id: Optional[Iterable[int]] = None,
        record_type: Optional[Iterable[str]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: int = None,
        skip: int = 0,
        extra_queries: Optional[List[Any]] = None,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ProcedureDict]]:
        """
        Parameters
        ----------
        id
            Query for records based on its ID
        record_type
            Query based on record type
        manager_name
            Name of the manager that is computing this record or last computed it
        status
            The status of the procedure
        created_before
            Query for records created before this date
        created_after
            Query for records created after this date
        modified_before
            Query for records last modified before this date
        modified_after
            Query for records last modified after this date
        include
            Which fields of the molecule to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results
        extra_queries
            Extra query filters to add. Typically these involve fields on the derived class.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Metadata about the results of the query, and a list of procedure data (as dictionaries)
        """

        limit = calculate_limit(self._limit, limit)

        proj_options = get_query_proj_options(orm_type, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(orm_type.id.in_(id))
        if record_type is not None:
            and_query.append(orm_type.record_type.in_(record_type))
        if manager_name is not None:
            and_query.append(orm_type.manager_name.in_(manager_name))
        if status is not None:
            and_query.append(orm_type.status.in_(status))
        if created_before is not None:
            and_query.append(orm_type.created_on < created_before)
        if created_after is not None:
            and_query.append(orm_type.created_on > created_after)
        if modified_before is not None:
            and_query.append(orm_type.modified_on < modified_before)
        if modified_after is not None:
            and_query.append(orm_type.modified_on > modified_after)

        if extra_queries is not None:
            and_query.extend(extra_queries)

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(orm_type).where(*and_query)
            stmt = stmt.options(*proj_options)
            n_found = get_count_2(session, stmt)
            stmt = stmt.limit(limit).offset(skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def get_base(
        self,
        orm_type: Type[BaseResultORM],
        id: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ProcedureDict]]:
        """
        Obtain records of a specified type with specified IDs

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        orm_type
            The type of record to get (as an ORM class)
        id
            A list or other sequence of record IDs
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
            Records as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """
        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} records is over the limit of {self._limit}")

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, orm_type, orm_type.id, id, include, exclude, missing_ok)

    def query(
        self,
        id: Optional[Iterable[int]] = None,
        record_type: Optional[Iterable[str]] = None,
        manager_name: Optional[Iterable[str]] = None,
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

        # If include and exclude are both None, then we can load
        # the data from derived classes as well.
        if include is None and exclude is None:
            wp = with_polymorphic(BaseResultORM, "*")
        else:
            wp = BaseResultORM

        return self.query_base(
            orm_type=wp,
            id=id,
            record_type=record_type,
            manager_name=manager_name,
            status=status,
            created_before=created_before,
            created_after=created_after,
            modified_before=modified_before,
            modified_after=modified_after,
            include=include,
            exclude=exclude,
            limit=limit,
            skip=skip,
            session=session,
        )

    def get(
        self,
        id: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[ProcedureDict]]:
        """
        Obtain records of any kind of record with specified IDs

        The returned information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        id
            A list or other sequence of record IDs
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
            Records as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        # If include and exclude are both None, then we can load
        # the data from derived classes as well.
        if include is None and exclude is None:
            wp = with_polymorphic(BaseResultORM, "*")
        else:
            wp = BaseResultORM

        return self.get_base(wp, id, include, exclude, missing_ok, session=session)

    def get_history(self, record_id: int, include_outputs: bool = False, *, session: Optional[Session] = None):
        with self.root_socket.optional_session(session, True) as session:
            stmt = select(RecordComputeHistoryORM).where(RecordComputeHistoryORM.record_id == record_id)

            if include_outputs:
                stmt = stmt.options(selectinload(RecordComputeHistoryORM.outputs))

            stmt = stmt.order_by(RecordComputeHistoryORM.modified_on.asc())

            hist = session.execute(stmt).scalars().all()
            return [h.dict() for h in hist]

    def update_completed(self, session: Session, record_orm: BaseResultORM, result: AllResultTypes, manager_name: str):

        if isinstance(result, FailedOperation) or not result.success:
            raise RuntimeError("Developer error - this function only handles successful results")

        handler = self._handler_map[record_orm.record_type]

        # Update record-specific fields
        handler.update_completed(session, record_orm, result, manager_name)

        # Now do everything common to all records
        # Get the outputs & status, storing in the history orm
        history_orm = create_compute_history_entry(result)
        history_orm.manager_name = manager_name
        record_orm.compute_history.append(history_orm)

        record_orm.status = history_orm.status
        record_orm.manager_name = manager_name
        record_orm.modified_on = history_orm.modified_on

        # Delete the task from the task queue since it is completed
        session.delete(record_orm.task)

    def update_failure(
        self, session: Session, record_orm: BaseResultORM, failed_result: FailedOperation, manager_name: str
    ):
        if not isinstance(failed_result, FailedOperation):
            raise RuntimeError("Developer error - this function only handles FailedOperation results")

        # Handle outputs, which are a bit different in FailedOperation
        # Error is special in a FailedOperation
        error = failed_result.error
        if error is None:
            error = _default_error

        error_obj = OutputStore.compress(OutputTypeEnum.error, error.dict(), CompressionEnum.lzma, 1)
        all_outputs = [error_obj]

        # Get the rest of the outputs
        # This is stored in "input_data" (I know...)
        # "input_data" can be anything. So ignore if it isn't a dict
        if isinstance(failed_result.input_data, dict):
            stdout = failed_result.input_data.get("stdout", None)
            stderr = failed_result.input_data.get("stderr", None)

            if stdout is not None:
                stdout_obj = OutputStore.compress(OutputTypeEnum.stdout, stdout, CompressionEnum.lzma, 1)
                all_outputs.append(stdout_obj)
            if stderr is not None:
                stderr_obj = OutputStore.compress(OutputTypeEnum.stderr, stderr, CompressionEnum.lzma, 1)
                all_outputs.append(stderr_obj)

        # Build the history orm
        history_orm = RecordComputeHistoryORM()
        history_orm.status = RecordStatusEnum.error
        history_orm.manager_name = manager_name
        history_orm.modified_on = datetime.utcnow()
        history_orm.outputs = [OutputStoreORM.from_model(x) for x in all_outputs]

        record_orm.status = RecordStatusEnum.error
        record_orm.modified_on = history_orm.modified_on
        record_orm.manager_name = manager_name
        record_orm.compute_history.append(history_orm)

    def reset(
        self,
        record_id: Optional[Iterable[int]] = None,
        manager_name: Optional[Iterable[str]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Reset the status of records to waiting

        This will also re-create tasks as necessary

        Parameters
        ----------
        record_id
            Reset the status of these record ids
        manager_name
            Reset the status of records belonging to these managers
        status
            Reset only records with these status. Default is all status except 'complete' and 'waiting'.
            Records with complete or waiting status will always be excluded.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Number of records that had their status reset
        """

        resettable_status = {
            RecordStatusEnum.running,
            RecordStatusEnum.error,
            RecordStatusEnum.cancelled,
            RecordStatusEnum.deleted,
        }

        if status is None:
            status = set(RecordStatusEnum)

        status = set(status) & resettable_status

        query = []

        if record_id is not None:
            if len(record_id) == 0:
                return 0
            query.append(BaseResultORM.id.in_(record_id))

        if manager_name is not None:
            if len(manager_name) == 0:
                return 0
            query.append(BaseResultORM.manager_name.in_(manager_name))

        if len(query) == 0:
            raise RuntimeError("INTERNAL QCFRACTAL ERROR - id or manager name not specified for reset record")

        with self.root_socket.optional_session(session) as session:
            # Can't do inner join because task may not exist
            stmt = select(BaseResultORM).options(selectinload(BaseResultORM.task))
            stmt = stmt.filter(BaseResultORM.status.in_(status))
            stmt = stmt.with_for_update()

            stmt = stmt.filter(*query).with_for_update()
            record_orms = session.execute(stmt).scalars().all()

            for r in record_orms:
                r.status = RecordStatusEnum.waiting
                r.modified_on = datetime.utcnow()
                r.manager_name = None

                # Regenerate the task if it does not exist
                # (cancelled or deleted)
                if r.task is None:
                    handler = self._handler_map[r.record_type]
                    handler.recreate_task(r)

            return len(record_orms)

    def cancel(
        self,
        record_id: Iterable[int],
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Marks a record as cancelled

        A cancelled record will not be picked up by any manager. This can be undone
        with reset_status

        Parameters
        ----------
        record_id
            Reset the status of these record ids
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Number of records that had their status reset
        """

        cancellable_status = {RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.error}

        if len(record_id) == 0:
            return 0

        with self.root_socket.optional_session(session) as session:
            # we can innerjoin here because all cancellable status have an associated task
            stmt = select(BaseResultORM).options(joinedload(BaseResultORM.task, innerjoin=True))
            stmt = stmt.where(BaseResultORM.status.in_(cancellable_status))
            stmt = stmt.where(BaseResultORM.id.in_(record_id))
            stmt = stmt.with_for_update()
            record_orms = session.execute(stmt).scalars().all()

            for r in record_orms:
                r.status = RecordStatusEnum.cancelled
                r.modified_on = datetime.utcnow()
                r.manager_name = None

                if r.task is not None:
                    session.delete(r.task)

            return len(record_orms)

    def delete(
        self,
        record_id: Iterable[int],
        soft_delete: bool = True,
        *,
        session: Optional[Session] = None,
    ) -> DeleteMetadata:
        """
        Marks a record as deleted

        If soft_delete is True, then the record is just marked as deleted and actually deletion may
        happen later. Soft delete can be undone with reset_status

        A deleted record will not be picked up by any manager.

        Parameters
        ----------
        record_id
            Reset the status of these record ids
        soft_delete
            Don't actually delete the record, just mark it for later deletion
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Number of records that were deleted
        """

        if len(record_id) == 0:
            return 0

        with self.root_socket.optional_session(session) as session:
            if soft_delete:
                # Can't do inner join because task may not exist
                stmt = select(BaseResultORM).options(selectinload(BaseResultORM.task))
                stmt = stmt.where(BaseResultORM.status != RecordStatusEnum.deleted)
                stmt = stmt.where(BaseResultORM.id.in_(record_id))
                stmt = stmt.with_for_update()
                record_orms = session.execute(stmt).scalars().all()

                for r in record_orms:
                    r.status = RecordStatusEnum.deleted
                    r.modified_on = datetime.utcnow()
                    r.manager_name = None

                    if r.task is not None:
                        session.delete(r.task)

                return len(record_orms)
            else:
                del_id = [(x,) for x in record_id]
                return delete_general(session, BaseResultORM, (BaseResultORM.id,), del_id)

    def modify_task(
        self,
        record_id: Iterable[int],
        new_tag: Optional[str] = None,
        new_priority: Optional[RecordStatusEnum] = None,
        delete_tag: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> int:
        """
        Modifies a record's task's priority and tag

        Records without a corresponding task (completed, etc) will not be modified. Running tasks
        will also not be modified.

        An empty string for new_tag will be treated the same as if you had passed it None

        Note that to set a tag to be None, you must use delete_tag. Just setting
        new_tag to None will keep the existing tag

        Parameters
        ----------
        record_id
            Modify the tasks corresponding to these record ids
        new_tag
            New tag for the task. If None, keep the existing tag
        new_priority
            New priority for the task. If None, then keep the existing priority
        delete_tag
            Set the tag to be None
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Number of records that were modified
        """

        # empty string?
        if not new_tag:
            new_tag = None

        # Do we have anything to do?
        if new_tag is None and new_priority is None and not delete_tag:
            return 0

        with self.root_socket.optional_session(session) as session:
            # Do a manual join, not a joined load - we don't want to actually load the base record, just
            # query by status
            stmt = select(TaskQueueORM)
            stmt = stmt.join(TaskQueueORM.record)
            stmt = stmt.where(BaseResultORM.status != RecordStatusEnum.running)
            stmt = stmt.where(TaskQueueORM.record_id.in_(record_id))
            stmt = stmt.with_for_update()
            task_orms = session.execute(stmt).scalars().all()

            for t in task_orms:
                if new_tag:
                    t.tag = new_tag
                if new_priority:
                    t.priority = new_priority

                if delete_tag:
                    t.tag = None

            return len(task_orms)
