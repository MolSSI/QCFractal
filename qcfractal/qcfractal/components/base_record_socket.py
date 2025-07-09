from __future__ import annotations

from typing import Optional, Type, Union, Tuple, Sequence, List, Dict, Any

from sqlalchemy import select, func
from sqlalchemy.orm import Session, lazyload, defer, joinedload, undefer, defaultload

from qcfractal.components.record_db_models import BaseRecordORM, OutputStoreORM, RecordComputeHistoryORM, NativeFileORM
from qcfractal.components.record_db_views import RecordDirectChildrenView
from qcfractal.components.services.db_models import ServiceQueueORM
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.all_results import AllResultTypes
from qcportal.compression import CompressionEnum, decompress
from qcportal.exceptions import MissingDataError
from qcportal.metadata_models import InsertCountsMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, RecordQueryFilters, OutputTypeEnum


class BaseRecordSocket:
    """
    Base class for all record sockets
    """

    # Must be overridden by derived classes
    record_orm: Optional[Type[BaseRecordORM]] = None

    # Overridden for most classes, but not all
    record_input_type = None
    record_multi_input_type = None

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket

        # Make sure these were set by the derived classes
        assert self.record_orm is not None

    @staticmethod
    def create_task(record_orm: BaseRecordORM, compute_tag: str, compute_priority: PriorityEnum) -> None:
        """
        Create an entry in the task queue, and attach it to the given record ORM
        """

        available = record_orm.status == RecordStatusEnum.waiting
        record_orm.task = TaskQueueORM(
            compute_tag=compute_tag,
            compute_priority=compute_priority,
            required_programs=record_orm.required_programs,
            available=available,
        )

    @staticmethod
    def create_service(
        record_orm: BaseRecordORM, compute_tag: str, compute_priority: PriorityEnum, find_existing: bool
    ) -> None:
        """
        Create an entry in the service queue, and attach it to the given record ORM
        """

        record_orm.service = ServiceQueueORM(
            service_state={}, compute_tag=compute_tag, compute_priority=compute_priority, find_existing=find_existing
        )

    def add_from_input(
        self,
        record_input,
        compute_tag: str,
        compute_priority: PriorityEnum,
        creator_user: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[InsertCountsMetadata, int]:
        """
        Adds a record given an object of the record's input type

        Parameters
        ----------
        record_input
            The input specifying the calculation. Dependent on the type of record
        compute_tag
            The tag for the task. This will assist in routing to appropriate compute managers.
        compute_priority
            The priority for the computation
        creator_user
            Name or ID of the user who created the record
        find_existing
            If True, search for existing records and return those. If False, always add new records
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about the insertion, and the ID of the record
        """

        raise NotImplementedError(
            f"add_from_input function not implemented for {type(self)}! This is a developer error"
        )

    def get(
        self,
        record_ids: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Obtain a record with specified IDs

        This function should be usable with all sockets - it uses the ORM type that was given
        in the constructor.

        Parameters
        ----------
        record_ids
            A list or other sequence of record IDs
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing results will be tolerated, and the returned list of
           records will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Records as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        raise NotImplementedError(f"get function not implemented for {type(self)}! This is a developer error")

    def query(
        self,
        query_data: RecordQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Query records of a particular type

        Parameters
        ----------
        query_data
            Fields/filters to query for
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of record ids that were found in the database.
        """

        raise NotImplementedError(f"query function not implemented for {type(self)}! This is a developer error")

    def generate_task_specifications(self, session: Session, record_ids: Sequence[int]) -> List[Dict[str, Any]]:
        """
        Generate the actual QCSchema input and related fields for a task
        """

        raise NotImplementedError(
            f"generate_task_specifications not implemented for {type(self)}! This is a developer error"
        )

    def update_completed_schema_v1(self, session: Session, record_id: int, result: AllResultTypes) -> None:
        """
        Update a record ORM based on the result of a successfully-completed computation
        """
        raise NotImplementedError(
            f"update_completed_schema_v1 not implemented for {type(self)}! This is a developer error"
        )

    def insert_complete_qcportal_records_v1(
        self,
        session: Session,
        records: Sequence[AllResultTypes],
    ) -> List[BaseRecordORM]:
        """
        Insert records into the database from a QCSchema result

        This will always create new ORMs from scratch, and not update any existing records.
        """
        raise NotImplementedError(
            f"insert_complete_schema_v1 not implemented for {type(self)}! This is a developer error"
        )

    def insert_complete_schema_v1(
        self,
        session: Session,
        results: Sequence[AllResultTypes],
    ) -> List[BaseRecordORM]:
        """
        Insert records into the database from a QCSchema result

        This will always create new ORMs from scratch, and not update any existing records.
        """
        raise NotImplementedError(
            f"insert_complete_schema_v1 not implemented for {type(self)}! This is a developer error"
        )

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        """
        Initialize a new service

        A service is initialized when it moves from a waiting state to a running state. After it is
        initialized, iterate_service may be called.
        """
        raise NotImplementedError(f"initialize_service not implemented for {type(self)}! This is a developer error")

    def iterate_service(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        """
        Iterate a service with successfully-completed dependencies

        The server will check to see if all dependencies are successfully completed. If they are, this function
        is called.

        Cases where the service has an errored dependency are handled elsewhere.
        """
        raise NotImplementedError(f"iterate_service not implemented for {type(self)}! This is a developer error")

    def available(self) -> bool:
        """
        Returns True if this is not a service, or if it is a service and available for iteration

        A service may not be available for iteration if the proper packages aren't installed
        on the server.
        """

        # By default, return True. Should be overridden by services
        return True

    ###########################################################################################
    # Getting various fields
    # The ones here apply to all records
    ###########################################################################################
    def get_comments(self, record_id: int, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(BaseRecordORM.comments).options(undefer("*"), defaultload("*")),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(BaseRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.model_dict() for x in rec.comments]

    def get_task(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        options = [lazyload("*"), defer("*"), joinedload(BaseRecordORM.task).options(undefer("*"), defaultload("*"))]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(BaseRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            if rec.task is None:
                return None
            return rec.task.model_dict()

    def get_service(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(BaseRecordORM.service).options(
                undefer("*"), defaultload("*"), joinedload(ServiceQueueORM.dependencies).options(undefer("*"))
            ),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(BaseRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            if rec.service is None:
                return None
            return rec.service.model_dict()

    def get_all_compute_history(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(BaseRecordORM.compute_history).options(undefer("*"), defaultload("*")),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(BaseRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return [x.model_dict() for x in rec.compute_history]

    def get_single_compute_history(
        self,
        record_id: int,
        history_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        # Slightly inefficient, but should be rarely called
        histories = self.get_all_compute_history(record_id, session=session)

        for h in histories:
            if h["id"] == history_id:
                return h
        else:
            raise MissingDataError(f"Cannot find compute history {history_id} for record {record_id}")

    def get_all_output_metadata(
        self,
        record_id: int,
        history_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Dict[str, Any]]:
        # Outer join with the history and record orm table to ensure ids, types, etc, match
        stmt = select(self.record_orm.id, OutputStoreORM)
        stmt = stmt.join(RecordComputeHistoryORM, self.record_orm.id == RecordComputeHistoryORM.record_id, isouter=True)
        stmt = stmt.join(OutputStoreORM, OutputStoreORM.history_id == RecordComputeHistoryORM.id, isouter=True)
        stmt = stmt.where(RecordComputeHistoryORM.id == history_id)
        stmt = stmt.where(self.record_orm.id == record_id)

        with self.root_socket.optional_session(session, True) as session:
            outputs = session.execute(stmt).all()

            # if empty list, not even the record was found
            if len(outputs) == 0:
                raise MissingDataError(f"Cannot find record {record_id}")

            # if the length is one, but the history is None, then record found, but no history
            elif len(outputs) == 1 and outputs[0][1] is None:
                return {}

            # Otherwise, found some outputs that match the record id/type
            else:
                return {o[1].output_type: o[1].model_dict() for o in outputs}

    def get_single_output_metadata(
        self,
        record_id: int,
        history_id: int,
        output_type: str,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        # Slightly inefficient, but should be ok
        all_outputs = self.get_all_output_metadata(record_id, history_id, session=session)

        if output_type not in all_outputs:
            raise MissingDataError(f"Cannot find output {output_type} for record {record_id}/history {history_id}")

        return all_outputs[output_type]

    def get_single_output_rawdata(
        self,
        record_id: int,
        history_id: int,
        output_type: str,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[bytes, CompressionEnum]:
        stmt = select(OutputStoreORM.data, OutputStoreORM.compression_type)
        stmt = stmt.join(RecordComputeHistoryORM, RecordComputeHistoryORM.id == OutputStoreORM.history_id)
        stmt = stmt.join(self.record_orm, RecordComputeHistoryORM.record_id == self.record_orm.id)
        stmt = stmt.where(RecordComputeHistoryORM.record_id == record_id)
        stmt = stmt.where(OutputStoreORM.history_id == history_id)
        stmt = stmt.where(OutputStoreORM.output_type == output_type)

        with self.root_socket.optional_session(session, True) as session:
            output_data = session.execute(stmt).one_or_none()
            if output_data is None:
                raise MissingDataError(
                    f"Record {record_id}/history {history_id} does not have {output_type} output (or record/history does not exist)"
                )

            return output_data[0], output_data[1]

    def get_single_output_uncompressed(
        self, record_id: int, history_id: int, output_type: OutputTypeEnum, *, session: Optional[Session] = None
    ) -> Any:
        """
        Get an uncompressed output from a record
        """

        raw_data, ctype = self.get_single_output_rawdata(record_id, history_id, output_type, session=session)
        return decompress(raw_data, ctype)

    def get_all_native_files_metadata(
        self,
        record_id: int,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Dict[str, Any]]:
        options = [
            lazyload("*"),
            defer("*"),
            joinedload(BaseRecordORM.native_files).options(undefer("*"), defaultload("*")),
        ]

        with self.root_socket.optional_session(session) as session:
            rec = session.get(BaseRecordORM, record_id, options=options)
            if rec is None:
                raise MissingDataError(f"Cannot find record {record_id}")
            return {k: v.model_dict() for k, v in rec.native_files.items()}

    def get_single_native_file_metadata(
        self,
        record_id: int,
        name: str,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        # Slightly inefficient, but should be ok
        all_nf = self.get_all_native_files_metadata(record_id, session=session)

        if not name in all_nf:
            raise MissingDataError(f"Cannot find native file {name} for record {record_id}")
        else:
            return all_nf[name]

    def get_single_native_file_rawdata(
        self,
        record_id: int,
        name: str,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[bytes, CompressionEnum]:
        stmt = select(NativeFileORM.data, NativeFileORM.compression_type)
        stmt = stmt.join(self.record_orm, NativeFileORM.record_id == self.record_orm.id)
        stmt = stmt.where(NativeFileORM.name == name)

        with self.root_socket.optional_session(session, True) as session:
            nf_data = session.execute(stmt).one_or_none()
            if nf_data is None:
                raise MissingDataError(
                    f"Record {record_id} does not have native file {name} (or record does not exist)"
                )

            return nf_data[0], nf_data[1]

    def get_single_native_file_uncompressed(
        self, record_id: int, name: str, *, session: Optional[Session] = None
    ) -> Any:
        """
        Get an uncompressed output from a record
        """

        raw_data, ctype = self.get_single_native_file_rawdata(record_id, name, session=session)
        return decompress(raw_data, ctype)

    def get_children_status(self, record_id: int, *, session: Optional[Session] = None) -> Dict[RecordStatusEnum, int]:
        stmt = select(BaseRecordORM.status, func.count())
        stmt = stmt.join(RecordDirectChildrenView, RecordDirectChildrenView.c.child_id == BaseRecordORM.id)
        stmt = stmt.where(RecordDirectChildrenView.c.parent_id == record_id)
        stmt = stmt.group_by(BaseRecordORM.status)

        with self.root_socket.optional_session(session, True) as session:
            res = session.execute(stmt).all()
            return {x: y for x, y in res}

    def get_children_errors(self, record_id: int, *, session: Optional[Session] = None) -> List[int]:
        stmt = select(RecordDirectChildrenView.c.child_id).distinct(RecordDirectChildrenView.c.child_id)
        stmt = stmt.join(BaseRecordORM, BaseRecordORM.id == RecordDirectChildrenView.c.child_id)
        stmt = stmt.where(RecordDirectChildrenView.c.parent_id == record_id)
        stmt = stmt.where(BaseRecordORM.status == RecordStatusEnum.error)

        with self.root_socket.optional_session(session, True) as session:
            return session.execute(stmt).scalars().all()
