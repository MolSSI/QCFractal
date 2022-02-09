from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, union, or_
from sqlalchemy.orm import joinedload, selectinload, with_polymorphic

from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependenciesORM
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.db_socket.helpers import (
    get_query_proj_options,
    get_count,
    get_general,
    delete_general,
)
from qcportal.exceptions import UserReportableError
from qcportal.metadata_models import DeleteMetadata, QueryMetadata, UpdateMetadata
from qcportal.outputstore import OutputStore, OutputTypeEnum, CompressionEnum
from qcportal.records import FailedOperation, PriorityEnum, RecordStatusEnum
from .db_models import RecordComputeHistoryORM, BaseRecordORM, RecordInfoBackupORM, RecordCommentsORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.db_socket.base_orm import BaseORM
    from qcportal.records import AllResultTypes, RecordQueryBody
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


class BaseRecordSocket:
    def __init__(
        self, root_socket: SQLAlchemySocket, record_orm: Type[BaseRecordORM], specification_orm: Type[BaseORM]
    ):
        self.root_socket = root_socket
        self.record_orm = record_orm
        self.specification_orm = specification_orm

    @staticmethod
    def get_children_select():
        raise NotImplementedError(f"get_children_select not implemented! This is a developer error")

    @staticmethod
    def create_task(record_orm: BaseRecordORM, tag: str, priority: PriorityEnum) -> None:
        """
        Recreate the entry in the task queue
        """

        record_orm.task = TaskQueueORM(tag=tag, priority=priority, required_programs=record_orm.required_programs)

    @staticmethod
    def create_service(record_orm: BaseRecordORM, tag: str, priority: PriorityEnum) -> None:
        """
        Recreate the entry in the serivce queue
        """

        record_orm.service = ServiceQueueORM(service_state={}, tag=tag, priority=priority)

    def get(
        self,
        record_id: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Obtain a record with specified IDs

        This function should be usable with all sockets. It uses the ORM type
        specified in the constructor.

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

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, self.record_orm, self.record_orm.id, record_id, include, exclude, missing_ok)

    def get_specification(
        self, spec_id: int, missing_ok: bool = False, *, session: Optional[Session] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Obtain a specification with the specified ID

        If missing_ok is False, then any ids that are missing in the database will raise an exception.
        Otherwise, the returned id will be None

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        spec_id
            An id for a single point specification
        missing_ok
           If set to True, then missing keywords will be tolerated, and the returned list of
           keywords will contain None for the corresponding IDs that were not found.
        session
            n existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Keyword information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the keywords were missing
        """

        with self.root_socket.optional_session(session, True) as session:
            return get_general(
                session, self.specification_orm, self.specification_orm.id, [spec_id], None, None, missing_ok
            )[0]

    def generate_task_specification(self, record_orm: BaseRecordORM) -> Dict[str, Any]:
        """
        Generate the specification for a task
        """
        raise NotImplementedError(
            f"generate_task_specification not implemented for {type(self)}! This is a developer error"
        )

    def update_completed_task(
        self, session: Session, record_orm: BaseRecordORM, result: AllResultTypes, manager_name: str
    ) -> None:
        raise NotImplementedError(f"updated_completed not implemented for {type(self)}! This is a developer error")

    def insert_complete_record(
        self,
        session: Session,
        result: AllResultTypes,
    ) -> BaseRecordORM:
        raise NotImplementedError(f"insert_completed not implemented for {type(self)}! This is a developer error")

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        raise NotImplementedError(f"initialize_service not implemented for {type(self)}! This is a developer error")

    def iterate_service(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        raise NotImplementedError(f"iterate_service not implemented for {type(self)}! This is a developer error")


class RecordSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        # All the subsockets
        from .singlepoint.sockets import SinglepointRecordSocket
        from .optimization.sockets import OptimizationRecordSocket
        from .torsiondrive.sockets import TorsiondriveRecordSocket
        from .gridoptimization.sockets import GridoptimizationRecordSocket

        self.singlepoint = SinglepointRecordSocket(root_socket)
        self.optimization = OptimizationRecordSocket(root_socket)
        self.torsiondrive = TorsiondriveRecordSocket(root_socket)
        self.gridoptimization = GridoptimizationRecordSocket(root_socket)

        self._handler_map: Dict[str, BaseRecordSocket] = {
            "singlepoint": self.singlepoint,
            "optimization": self.optimization,
            "torsiondrive": self.torsiondrive,
            "gridoptimization": self.gridoptimization,
        }

        self._handler_map_by_schema: Dict[str, BaseRecordSocket] = {
            "qcschema_output": self.singlepoint,
            "qcschema_optimization_output": self.optimization,
        }

        ###############################################################################
        # Build the cte that maps parent and children
        # This cte represents a single table-like object with two columns
        # - parent_id and child_id. This contains any parent-child record relationship
        # (optimization trajectory, torsiondrive optimization, etc).
        ###############################################################################

        # Get the SQL 'select' statements from the handlers
        selects = []
        for h in self._handler_map.values():
            sel = h.get_children_select()
            selects.extend(sel)

        # Union them into a CTE
        self._child_cte = union(*selects).cte()

    def get_subtask_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        # List may contain duplicates. So be tolerant of that!
        stmt = select(ServiceDependenciesORM.record_id)
        stmt = stmt.join(ServiceQueueORM, ServiceQueueORM.id == ServiceDependenciesORM.service_id)
        stmt = stmt.where(ServiceQueueORM.record_id.in_(record_id))
        return session.execute(stmt).scalars().all()

    def get_children_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        """
        Recursively obtain the record IDs of all children records
        """

        all_children_ids = []

        stmt = select(self._child_cte.c.child_id).where(self._child_cte.c.parent_id.in_(record_id))
        children_ids = session.execute(stmt).scalars().unique().all()

        while len(children_ids) > 0:
            all_children_ids.extend(children_ids)

            # find children of children, etc
            stmt = select(self._child_cte.c.child_id).where(self._child_cte.c.parent_id.in_(children_ids))
            children_ids = session.execute(stmt).scalars().unique().all()

        return all_children_ids

    def get_parent_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        """
        Recursively obtain the record IDs of all parent records
        """

        all_parent_ids = []

        stmt = select(self._child_cte.c.parent_id).where(self._child_cte.c.child_id.in_(record_id))
        parent_ids = session.execute(stmt).scalars().unique().all()

        while len(parent_ids) > 0:
            all_parent_ids.extend(parent_ids)

            # find parents of parents, etc
            stmt = select(self._child_cte.c.parent_id).where(self._child_cte.c.child_id.in_(parent_ids))
            parent_ids = session.execute(stmt).scalars().unique().all()

        return all_parent_ids

    def get_relative_ids(self, session: Session, record_id: Iterable[int]) -> List[int]:
        """
        Recursively obtain the record IDs of all parent and children records
        """

        all_relative_ids = set()

        stmt = select(self._child_cte.c.parent_id, self._child_cte.c.child_id).where(
            or_(self._child_cte.c.child_id.in_(record_id), self._child_cte.c.parent_id.in_(record_id))
        )

        relative_ids = session.execute(stmt).all()

        while len(relative_ids) > 0:
            rel_id_flat = set()

            rel_id_flat.update(x[0] for x in relative_ids)
            rel_id_flat.update(x[1] for x in relative_ids)

            rel_id_flat -= all_relative_ids
            all_relative_ids |= rel_id_flat

            # find parents of parents, children of children, children of parents, parents of children, etc
            stmt = select(self._child_cte.c.parent_id, self._child_cte.c.child_id).where(
                or_(self._child_cte.c.child_id.in_(rel_id_flat), self._child_cte.c.parent_id.in_(rel_id_flat))
            )

            relative_ids = session.execute(stmt).all()

        return list(all_relative_ids)

    def query_base(
        self,
        stmt,
        orm_type: Type[BaseRecordORM],
        query_data: RecordQueryBody,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[ProcedureDict]]:

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
            n_found = get_count(session, stmt)
            stmt = stmt.limit(query_data.limit).offset(query_data.skip)
            results = session.execute(stmt).scalars().unique().all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))
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

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, wp, wp.id, record_id, include, exclude, missing_ok)

    def generate_task_specification(self, task_orm: Sequence[TaskQueueORM]):
        """
        Generates the actual qcschema specification and related fields for a computation
        """

        for t in task_orm:
            if t.spec is None:
                record_type = t.record.record_type
                t.spec = self._handler_map[record_type].generate_task_specification(t.record)

    def update_completed_task(
        self, session: Session, record_orm: BaseRecordORM, result: AllResultTypes, manager_name: str
    ):

        if isinstance(result, FailedOperation) or not result.success:
            raise RuntimeError("Developer error - this function only handles successful results")

        if record_orm.is_service:
            raise RuntimeError("Cannot update completed task with a service")

        handler = self._handler_map[record_orm.record_type]

        # Update record-specific fields
        handler.update_completed_task(session, record_orm, result, manager_name)

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

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        if not service_orm.record.is_service:
            raise RuntimeError("Cannot initialize a record that is not a service")

        record_type = service_orm.record.record_type
        return self._handler_map[record_type].initialize_service(session, service_orm)

    def iterate_service(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        if not service_orm.record.is_service:
            raise RuntimeError("Cannot iterate a record that is not a service")

        record_type = service_orm.record.record_type
        return self._handler_map[record_type].iterate_service(session, service_orm)

    def update_failed_service(self, record_orm: BaseRecordORM, error_info: Dict[str, Any]):
        record_orm.status = RecordStatusEnum.error

        error_obj = OutputStore.compress(OutputTypeEnum.error, error_info, CompressionEnum.lzma, 1)
        error_orm = OutputStoreORM.from_model(error_obj)
        record_orm.compute_history[-1].status = RecordStatusEnum.error
        record_orm.compute_history[-1].outputs.append(error_orm)
        record_orm.compute_history[-1].modified_on = datetime.utcnow()

        record_orm.status = RecordStatusEnum.error
        record_orm.modified_on = datetime.utcnow()

    def insert_complete_record(self, session: Session, results: Sequence[AllResultTypes]) -> List[int]:

        ids = []

        for result in results:
            if isinstance(result, FailedOperation) or not result.success:
                raise UserReportableError("Cannot insert a completed, failed operation")

            # Get the outputs & status, storing in the history orm.
            # Do this before calling the individual record handlers since it modifies extras
            # (looking for compressed outputs)
            history_orm = create_compute_history_entry(result)

            # Now the record-specific stuff
            handler = self._handler_map_by_schema[result.schema_name]
            record_orm = handler.insert_complete_record(session, result)

            # Now back to the common modifications
            record_orm.compute_history.append(history_orm)

            record_orm.status = history_orm.status
            record_orm.modified_on = history_orm.modified_on

            session.flush()
            ids.append(record_orm.id)

        return ids

    def add_comment(
        self, record_id: Sequence[int], username: Optional[str], comment: str, *, session: Optional[Session] = None
    ) -> UpdateMetadata:

        with self.root_socket.optional_session(session) as session:
            # find only existing records
            stmt = select(BaseRecordORM.id).where(BaseRecordORM.id.in_(record_id))
            stmt = stmt.with_for_update()
            existing_ids = session.execute(stmt).scalars().all()

            for rid in existing_ids:
                comment_orm = RecordCommentsORM(
                    record_id=rid,
                    username=username,
                    comment=comment,
                )
                session.add(comment_orm)

            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in existing_ids]
            missing_idx = [idx for idx, rid in enumerate(record_id) if rid not in existing_ids]

            return UpdateMetadata(
                updated_idx=updated_idx,
                errors=[(idx, "Record does not exist") for idx in missing_idx],
            )

    def update_failed_task(self, record_orm: BaseRecordORM, failed_result: FailedOperation, manager_name: str):
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
            stmt = stmt.where(BaseRecordORM.is_service.is_(False))
            stmt = stmt.where(BaseRecordORM.manager_name.in_(manager_name))
            stmt = stmt.where(BaseRecordORM.status == RecordStatusEnum.running)
            stmt = stmt.with_for_update()

            record_orms = session.execute(stmt).scalars().all()

            for r in record_orms:
                r.status = RecordStatusEnum.waiting
                r.modified_on = datetime.utcnow()
                r.manager_name = None

            return [r.id for r in record_orms]

    def _revert_common(
        self,
        record_id: Sequence[int],
        applicable_status: Iterable[RecordStatusEnum],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Internal function for resetting, uncancelling, undeleting, or uninvalidation

        This will also re-create tasks & services as necessary

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
            Metadata about what was updated
        """

        if not record_id:
            return UpdateMetadata()

        all_id = set(record_id)

        with self.root_socket.optional_session(session) as session:
            # We always apply these operations to children, but never to parents
            children_ids = self.get_children_ids(session, record_id)
            all_id.update(children_ids)

            # Select records with a resettable status
            # All are resettable except complete and running
            # Can't do inner join because task may not exist
            stmt = select(BaseRecordORM).options(joinedload(BaseRecordORM.task))
            stmt = stmt.options(selectinload(BaseRecordORM.info_backup))
            stmt = stmt.where(BaseRecordORM.status.in_(applicable_status))
            stmt = stmt.where(BaseRecordORM.id.in_(all_id))
            stmt = stmt.with_for_update(of=[BaseRecordORM, BaseRecordORM])
            record_data = session.execute(stmt).scalars().all()

            for r_orm in record_data:

                # If we have the old backup info, then use that
                if (
                    r_orm.status in [RecordStatusEnum.deleted, RecordStatusEnum.cancelled, RecordStatusEnum.invalid]
                    and r_orm.info_backup
                ):
                    last_info = r_orm.info_backup.pop()  # Remove the last entry
                    r_orm.status = last_info.old_status

                    if r_orm.status in [RecordStatusEnum.waiting, RecordStatusEnum.error]:
                        if r_orm.task:
                            self._logger.warning(f"Record {r_orm.id} has a task and also an entry in the backup table!")
                            session.delete(r_orm.task)

                        # we leave service queue entries alone
                        if not r_orm.is_service:
                            BaseRecordSocket.create_task(r_orm, last_info.old_tag, last_info.old_priority)

                elif r_orm.status in [RecordStatusEnum.running, RecordStatusEnum.error] and not r_orm.info_backup:
                    if not r_orm.is_service and r_orm.task is None:
                        raise RuntimeError(f"resetting a record with status {r_orm.status} with no task")
                    if r_orm.is_service and r_orm.service is None:
                        raise RuntimeError(f"resetting a record with status {r_orm.status} with no service")

                    # Move the record back to "waiting" for a manager/service periodics to pick it up
                    r_orm.status = RecordStatusEnum.waiting
                    r_orm.manager_name = None

                else:
                    if r_orm.info_backup:
                        raise RuntimeError(f"resetting record with status {r_orm.status} with backup info present")
                    else:
                        raise RuntimeError(f"resetting record with status {r_orm.status} without backup info present")

                r_orm.modified_on = datetime.utcnow()

            # put in order of the input parameter
            updated_ids = [r.id for r in record_data]
            error_ids = set(record_id) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_id) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be reset") for idx in error_idx]
            n_children_updated = len(updated_ids) - len(updated_idx)

            return UpdateMetadata(updated_idx=updated_idx, errors=errors, n_children_updated=n_children_updated)

    def _cancel_common(
        self,
        record_id: Sequence[int],
        applicable_status: Iterable[RecordStatusEnum],
        new_status: RecordStatusEnum,
        propagate_to_children: bool,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Internal function for cancelling, deleting, or invalidation

        Cancelling, deleting, and invalidation basically the same, with the only difference
        being that they apply to different statuses.
        The connotation of these operations is of course different, but internally they behave the same.

        Parameters
        ----------
        record_id
            Reset the status of these record ids
        new_status
            What the new status of the record should be
        propagate_to_children
            Apply the operation to children as well
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about what was updated
        """

        if len(record_id) == 0:
            return UpdateMetadata()

        all_ids = set(record_id)

        with self.root_socket.optional_session(session) as session:
            if propagate_to_children:
                # We always propagate to parents. So recursively find all related records
                relative_ids = self.get_relative_ids(session, record_id)
                all_ids.update(relative_ids)
            else:
                # Always propagate these changes to parents
                parent_ids = self.get_parent_ids(session, record_id)
                all_ids.update(parent_ids)

            stmt = select(BaseRecordORM).options(joinedload(BaseRecordORM.task))
            stmt = stmt.where(BaseRecordORM.status.in_(applicable_status))
            stmt = stmt.where(BaseRecordORM.id.in_(all_ids))
            stmt = stmt.with_for_update(of=[BaseRecordORM])
            record_orms = session.execute(stmt).scalars().all()

            for r in record_orms:
                # If running, delete the manager. Resetting later will move it to waiting
                if r.status == RecordStatusEnum.running:
                    r.status = RecordStatusEnum.waiting
                    r.manager_name = None

                old_tag = None
                old_priority = None
                if r.task is not None:
                    old_tag = r.task.tag
                    old_priority = r.task.priority
                    session.delete(r.task)

                # If this is a service, we leave the
                # the entry in the service queue since it contains
                # the current service state info

                # Store the old info in the backup table
                backup_info = RecordInfoBackupORM(
                    record_id=r.id,
                    old_status=r.status,
                    old_tag=old_tag,
                    old_priority=old_priority,
                    modified_on=datetime.utcnow(),
                )
                session.add(backup_info)

                r.modified_on = datetime.utcnow()
                r.status = new_status

            # put in order of the input parameter
            updated_ids = [r.id for r in record_orms]
            error_ids = set(record_id) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_id) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be cancelled/deleted/invalidated") for idx in error_idx]
            n_children_updated = len(updated_ids) - len(updated_idx)

            return UpdateMetadata(updated_idx=updated_idx, errors=errors, n_children_updated=n_children_updated)

    def reset(self, record_id: Sequence[int], *, session: Optional[Session] = None):
        """
        Resets a running or errored record to be waiting again
        """

        return self._revert_common(
            record_id, applicable_status=[RecordStatusEnum.running, RecordStatusEnum.error], session=session
        )

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
        happen later. Soft delete can be undone with undelete

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

        if soft_delete:
            # anything can be deleted except something already deleted
            meta = self._cancel_common(
                record_id,
                set(RecordStatusEnum) - {RecordStatusEnum.deleted},
                RecordStatusEnum.deleted,
                propagate_to_children=delete_children,
                session=session,
            )

            # convert the update metadata to a deleted metadata
            return DeleteMetadata(
                error_description=meta.error_description,
                errors=meta.errors,
                deleted_idx=meta.updated_idx,
                n_children_deleted=meta.n_children_updated,
            )

        with self.root_socket.optional_session(session) as session:
            all_id = set(record_id)
            children_ids = []

            if delete_children:
                children_ids = self.get_children_ids(session, record_id)
                all_id.update(children_ids)

            del_id_1 = [(x,) for x in record_id]
            del_id_2 = [(x,) for x in children_ids]
            meta = delete_general(session, BaseRecordORM, (BaseRecordORM.id,), del_id_1)
            ch_meta = delete_general(session, BaseRecordORM, (BaseRecordORM.id,), del_id_2)

            meta_dict = meta.dict()
            meta_dict["n_children_deleted"] = ch_meta.n_deleted
            return DeleteMetadata(**meta_dict)

    def cancel(
        self,
        record_id: Sequence[int],
        cancel_children: bool = True,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Marks a record as cancelled

        Parameters
        ----------
        record_id
            Reset the status of these record ids
        cancel_children
            Cancel all children as well
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        return self._cancel_common(
            record_id,
            {RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.error},
            RecordStatusEnum.cancelled,
            propagate_to_children=cancel_children,
            session=session,
        )

    def invalidate(
        self,
        record_id: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Marks a record as invalid

        This only applies to completed records

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
            Metadata about what was deleted
        """

        return self._cancel_common(
            record_id,
            {RecordStatusEnum.complete},
            RecordStatusEnum.invalid,
            propagate_to_children=False,
            session=session,
        )

    def undelete(
        self,
        record_id: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Undeletes records that were soft deleted

        This will always undelete children whenever possible

        Parameters
        ----------
        record_id
            ID of the record to undelete
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about what was undeleted
        """
        if len(record_id) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            meta = self._revert_common(record_id, applicable_status=[RecordStatusEnum.deleted], session=session)

            return UpdateMetadata(
                updated_idx=meta.updated_idx,
                error_description=meta.error_description,
                errors=meta.errors,
                n_children_updated=meta.n_children_updated,
            )

    def uncancel(
        self,
        record_id: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Uncancels records that were previously cancelled

        This will always uncancel children whenever possible
        """
        if len(record_id) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            return self._revert_common(record_id, applicable_status=[RecordStatusEnum.cancelled], session=session)

    def uninvalidate(
        self,
        record_id: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Uninvalidates records that were previously uninvalidated

        This will always uninvalidate children whenever possible
        """
        if len(record_id) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            return self._revert_common(record_id, applicable_status=[RecordStatusEnum.invalid], session=session)

    def modify(
        self,
        record_id: Sequence[int],
        new_tag: Optional[str] = None,
        new_priority: Optional[RecordStatusEnum] = None,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Modifies a record's task's priority and tag

        Records without a corresponding task (completed, etc) will not be modified. Running tasks
        will also not be modified.

        An empty string for new_tag will be treated the same as if you had passed it None

        Parameters
        ----------
        record_id
            Modify the tasks corresponding to these record ids
        new_tag
            New tag for the task. If None, keep the existing tag
        new_priority
            New priority for the task. If None, then keep the existing priority
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about what was updated
        """

        # Do we have anything to do?
        if new_tag is None and new_priority is None:
            return UpdateMetadata()

        all_id = set(record_id)

        with self.root_socket.optional_session(session) as session:
            # Get subtasks for all the records that are services
            subtask_id = self.get_subtask_ids(session, record_id)
            all_id.update(subtask_id)

            # Do a manual join, not a joined load - we don't want to actually load the base record, just
            # query by status
            stmt = select(TaskQueueORM)
            stmt = stmt.join(TaskQueueORM.record)
            stmt = stmt.where(TaskQueueORM.record_id.in_(all_id))
            stmt = stmt.with_for_update()
            task_orms = session.execute(stmt).scalars().all()

            stmt = select(ServiceQueueORM)
            stmt = stmt.join(ServiceQueueORM.record)
            stmt = stmt.where(ServiceQueueORM.record_id.in_(all_id))
            stmt = stmt.with_for_update()
            svc_orms = session.execute(stmt).scalars().all()

            all_orm = task_orms + svc_orms
            for o in all_orm:
                if new_tag is not None:
                    o.tag = new_tag
                if new_priority is not None:
                    o.priority = new_priority

            # put in order of the input parameter
            # only pay attention to the records requested (ie, not subtasks)
            updated_ids = [t.record_id for t in all_orm]
            error_ids = set(record_id) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_id) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_id) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be modified") for idx in error_idx]
            n_children_updated = len(all_orm) - (len(updated_idx) + len(error_idx))

            return UpdateMetadata(updated_idx=updated_idx, errors=errors, n_children_updated=n_children_updated)
