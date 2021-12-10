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
    get_general_multi,
    delete_general,
)
from qcfractal.exceptions import UserReportableError
from qcfractal.portal.metadata_models import DeleteMetadata, QueryMetadata, UpdateMetadata
from qcfractal.portal.outputstore import OutputStore, OutputTypeEnum, CompressionEnum
from qcfractal.portal.records import FailedOperation, PriorityEnum, RecordStatusEnum
from .db_models import RecordComputeHistoryORM, BaseRecordORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.portal.records import AllResultTypes, RecordQueryBody
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
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._limit = root_socket.qcf_config.response_limits.record

    @abc.abstractmethod
    def update_completed(
        self, session: Session, record_orm: BaseRecordORM, result: AllResultTypes, manager_name: str
    ) -> None:
        pass

    @abc.abstractmethod
    def recreate_task(
        self, record_orm: BaseRecordORM, tag: Optional[str] = None, priority: PriorityEnum = PriorityEnum.normal
    ) -> None:
        pass

    @abc.abstractmethod
    def insert_completed(
        self,
        session: Session,
        result: AllResultTypes,
    ) -> BaseRecordORM:
        pass

    @abc.abstractmethod
    def get_children_ids(
        self,
        session: Session,
        record_id: Iterable[int],
    ) -> List[int]:
        # NOTE - it is expected that handlers get ids
        # for computations that are not of the correct type
        pass


class RecordSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.record
        self._output_limit = root_socket.qcf_config.response_limits.output_store

        # All the subsockets
        from .singlepoint.sockets import SinglePointRecordSocket
        from .optimization.sockets import OptimizationRecordSocket

        self.singlepoint = SinglePointRecordSocket(root_socket)
        self.optimization = OptimizationRecordSocket(root_socket)

        self._handler_map: Dict[str, BaseRecordSocket] = {
            "singlepoint": self.singlepoint,
            "optimization": self.optimization,
        }
        self._handler_map_by_schema: Dict[str, BaseRecordSocket] = {
            "qcschema_output": self.singlepoint,
            "qcschema_optimization_output": self.optimization,
        }

    def get_children_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        all_ids = []

        for h in self._handler_map.values():
            ch = h.get_children_ids(session, record_id)
            all_ids.extend(ch)

        return all_ids

    def query_base(
        self,
        stmt,
        orm_type: Type[BaseRecordORM],
        query_data: RecordQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ProcedureDict]]:

        limit = calculate_limit(self._limit, query_data.limit)

        proj_options = get_query_proj_options(orm_type, query_data.include, query_data.exclude)

        and_query = []
        if query_data.record_id is not None:
            and_query.append(orm_type.id.in_(query_data.record_id))
        if query_data.record_type is not None:
            and_query.append(orm_type.record_type.in_(query_data.record_type))
        if query_data.manager_name is not None:
            and_query.append(orm_type.manager_name.in_(query_data.manager_name))
        if query_data.status is not None:
            and_query.append(orm_type.status.in_(query_data.status))
        if query_data.created_before is not None:
            and_query.append(orm_type.created_on < query_data.created_before)
        if query_data.created_after is not None:
            and_query.append(orm_type.created_on > query_data.created_after)
        if query_data.modified_before is not None:
            and_query.append(orm_type.modified_on < query_data.modified_before)
        if query_data.modified_after is not None:
            and_query.append(orm_type.modified_on > query_data.modified_after)

        with self.root_socket.optional_session(session, True) as session:
            stmt = stmt.where(*and_query)
            stmt = stmt.options(*proj_options)
            n_found = get_count_2(session, stmt)
            stmt = stmt.limit(limit).offset(query_data.skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def get_base(
        self,
        orm_type: Type[BaseRecordORM],
        record_id: Sequence[int],
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
        record_id
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
        if len(record_id) > self._limit:
            raise RuntimeError(f"Request for {len(record_id)} records is over the limit of {self._limit}")

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, orm_type, orm_type.id, record_id, include, exclude, missing_ok)

    def query(
        self,
        query_data: RecordQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ProcedureDict]]:

        # If all columns are included, then we can load
        # the data from derived classes as well.
        if (query_data.include is None or "*" in query_data.include) and not query_data.exclude:
            wp = with_polymorphic(BaseRecordORM, "*")
        else:
            wp = BaseRecordORM

        # The bare minimum when queried from the base record socket
        stmt = select(wp)

        return self.query_base(
            stmt,
            orm_type=wp,
            query_data=query_data,
            session=session,
        )

    def get(
        self,
        record_id: Sequence[int],
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
        record_id
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

        # If all columns are included, then we can load
        # the data from derived classes as well.
        if (include is None or "*" in include) and not exclude:
            wp = with_polymorphic(BaseRecordORM, "*")
        else:
            wp = BaseRecordORM

        return self.get_base(wp, record_id, include, exclude, missing_ok, session=session)

    def get_history(
        self,
        record_id: int,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ):
        with self.root_socket.optional_session(session, True) as session:
            hist = get_general_multi(
                session,
                RecordComputeHistoryORM,
                RecordComputeHistoryORM.record_id,
                [record_id],
                include,
                exclude,
                missing_ok,
            )
            return sorted(hist[0], key=lambda x: x["modified_on"])

    def update_completed(self, session: Session, record_orm: BaseRecordORM, result: AllResultTypes, manager_name: str):

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

    def insert_completed(self, results: AllResultTypes) -> List[int]:

        ids = []

        with self.root_socket.session_scope() as session:
            for result in results:
                if isinstance(result, FailedOperation) or not result.success:
                    raise UserReportableError("Cannot insert a completed, failed operation")

                handler = self._handler_map_by_schema[result.schema_name]
                record_orm = handler.insert_completed(session, result)

                # Now do everything common to all records
                # Get the outputs & status, storing in the history orm
                history_orm = create_compute_history_entry(result)
                record_orm.compute_history.append(history_orm)

                record_orm.status = history_orm.status
                record_orm.modified_on = history_orm.modified_on

                session.flush()
                ids.append(record_orm.id)

        return ids

    def update_failure(self, record_orm: BaseRecordORM, failed_result: FailedOperation, manager_name: str):
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

    def reset_assigned(
        self,
        manager_name: Optional[Iterable[str]] = None,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Reset the status of records assigned to given managers to waiting

        Parameters
        ----------
        manager_name
            Reset the status of records belonging to these managers
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Ids of the tasks that were modified
        """

        if not manager_name:
            return []

        with self.root_socket.optional_session(session) as session:
            stmt = select(BaseRecordORM).options(joinedload(BaseRecordORM.task, innerjoin=True))
            stmt = stmt.where(BaseRecordORM.manager_name.in_(manager_name))
            stmt = stmt.where(BaseRecordORM.status == RecordStatusEnum.running)
            stmt = stmt.with_for_update()

            record_orms = session.execute(stmt).scalars().all()

            for r in record_orms:
                r.status = RecordStatusEnum.waiting
                r.modified_on = datetime.utcnow()
                r.manager_name = None

            return [r.id for r in record_orms]

    def reset(
        self,
        record_id: Optional[Sequence[int]] = None,
        status: Optional[Iterable[RecordStatusEnum]] = None,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Reset the status of records to waiting

        This will also re-create tasks as necessary

        Parameters
        ----------
        record_id
            Reset the status of these record ids
        status
            Reset only records with these status. Default is all status except 'complete' and 'waiting'.
            Records with complete or waiting status will always be excluded.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about what was updated
        """

        if not record_id:
            return UpdateMetadata()

        resettable_status = {
            RecordStatusEnum.running,
            RecordStatusEnum.error,
            RecordStatusEnum.cancelled,
            RecordStatusEnum.deleted,
        }

        if status is None:
            status = set(RecordStatusEnum)

        status = set(status) & resettable_status

        with self.root_socket.optional_session(session) as session:
            # Can't do inner join because task may not exist
            stmt = select(BaseRecordORM).options(selectinload(BaseRecordORM.task))
            stmt = stmt.filter(BaseRecordORM.status.in_(status))
            stmt = stmt.filter(BaseRecordORM.id.in_(record_id))
            stmt = stmt.with_for_update()

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

            # put in order of the input parameter
            updated_ids = [r.id for r in record_orms]
            error_ids = set(record_id) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_id) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be reset") for idx in error_idx]

            return UpdateMetadata(updated_idx=updated_idx, errors=errors)

    def cancel(
        self,
        record_id: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
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
            Metadata about what was updated/cancelled
        """

        cancellable_status = {RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.error}

        if len(record_id) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            # we can innerjoin here because all cancellable status have an associated task
            stmt = select(BaseRecordORM).options(joinedload(BaseRecordORM.task, innerjoin=True))
            stmt = stmt.where(BaseRecordORM.status.in_(cancellable_status))
            stmt = stmt.where(BaseRecordORM.id.in_(record_id))
            stmt = stmt.with_for_update()
            record_orms = session.execute(stmt).scalars().all()

            for r in record_orms:
                r.status = RecordStatusEnum.cancelled
                r.modified_on = datetime.utcnow()
                r.manager_name = None

                if r.task is not None:
                    session.delete(r.task)

            # put in order of the input parameter
            updated_ids = [r.id for r in record_orms]
            error_ids = set(record_id) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_id) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be cancelled") for idx in error_idx]

            return UpdateMetadata(updated_idx=updated_idx, errors=errors)

    def delete(
        self,
        record_id: Sequence[int],
        soft_delete: bool = True,
        delete_children: bool = True,
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
            Metadata about what was deleted
        """

        if len(record_id) == 0:
            return DeleteMetadata()

        with self.root_socket.optional_session(session) as session:
            all_id = list(record_id)  # convert to a list and/or copy
            children_ids = []

            if delete_children:
                children_ids = self.get_children_ids(session, record_id)
                all_id.extend(children_ids)

            if soft_delete:
                # Can't do inner join because task may not exist
                stmt = select(BaseRecordORM).options(selectinload(BaseRecordORM.task))
                stmt = stmt.where(BaseRecordORM.id.in_(all_id))
                stmt = stmt.with_for_update()
                record_orms = session.execute(stmt).scalars().all()

                for r in record_orms:
                    if r.status != RecordStatusEnum.deleted:
                        r.status = RecordStatusEnum.deleted
                        r.modified_on = datetime.utcnow()
                        r.manager_name = None

                        if r.task is not None:
                            session.delete(r.task)

                # put in order of the input parameter
                # We only count the top level deletions, so we are looing at
                # record_id and not all_id
                deleted_ids = [r.id for r in record_orms]
                missing_ids = set(record_id) - set(deleted_ids)
                deleted_idx = [idx for idx, rid in enumerate(record_id) if rid in deleted_ids]
                missing_idx = [idx for idx, rid in enumerate(record_id) if rid in missing_ids]
                n_children_deleted = len(all_id) - len(record_id)

                return DeleteMetadata(
                    deleted_idx=deleted_idx, missing_idx=list(missing_idx), n_children_deleted=n_children_deleted
                )
            else:
                del_id_1 = [(x,) for x in record_id]
                del_id_2 = [(x,) for x in children_ids]
                meta = delete_general(session, BaseRecordORM, (BaseRecordORM.id,), del_id_1)
                ch_meta = delete_general(session, BaseRecordORM, (BaseRecordORM.id,), del_id_2)

                meta_dict = meta.dict()
                meta_dict["n_children_deleted"] = ch_meta.n_deleted
                return DeleteMetadata(**meta_dict)

    def modify(
        self,
        record_id: Sequence[int],
        new_tag: Optional[str] = None,
        new_priority: Optional[RecordStatusEnum] = None,
        delete_tag: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
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
            Metadata about what was updated
        """

        # empty string?
        if not new_tag:
            new_tag = None

        # Do we have anything to do?
        if new_tag is None and new_priority is None and not delete_tag:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            # Do a manual join, not a joined load - we don't want to actually load the base record, just
            # query by status
            stmt = select(TaskQueueORM)
            stmt = stmt.join(TaskQueueORM.record)
            stmt = stmt.where(BaseRecordORM.status != RecordStatusEnum.running)
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

            # put in order of the input parameter
            updated_ids = [t.record_id for t in task_orms]
            error_ids = set(record_id) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_id) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be modified") for idx in error_idx]

            return UpdateMetadata(updated_idx=updated_idx, errors=errors)
