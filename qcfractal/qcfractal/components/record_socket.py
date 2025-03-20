from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from qcelemental.models import FailedOperation
from sqlalchemy import select, delete, update, union, or_, func, text
from sqlalchemy.orm import (
    joinedload,
    selectinload,
    lazyload,
    defer,
    undefer,
    defaultload,
    with_polymorphic,
    aliased,
    load_only,
)

from qcfractal.components.auth.db_models import UserIDMapSubquery, GroupIDMapSubquery
from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.services.db_models import ServiceQueueORM, ServiceDependencyORM
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.db_socket.helpers import (
    get_general,
    delete_general,
)
from qcportal.compression import CompressionEnum, decompress
from qcportal.exceptions import UserReportableError, MissingDataError
from qcportal.managers.models import ManagerStatusEnum
from qcportal.metadata_models import DeleteMetadata, UpdateMetadata, InsertCountsMetadata
from qcportal.record_models import PriorityEnum, RecordStatusEnum, OutputTypeEnum
from qcportal.utils import chunk_iterable, now_at_utc, is_included
from .outputstore.utils import create_output_orm
from .record_db_models import (
    RecordComputeHistoryORM,
    BaseRecordORM,
    RecordInfoBackupORM,
    RecordCommentORM,
    OutputStoreORM,
    NativeFileORM,
)
from .record_utils import (
    build_extras_properties,
    upsert_output,
    compute_history_orms_from_schema_v1,
    native_files_orms_from_schema_v1,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from sqlalchemy.sql import Select
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcportal.all_results import AllResultTypes
    from qcportal.record_models import RecordQueryFilters
    from qcportal.all_inputs import AllInputTypes
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Iterable, Type, Union


_default_error = {"error_type": "not_supplied", "error_message": "No error message found on task."}


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
    def get_children_select() -> List[Any]:
        """
        Return a list of 'select' statements that contain a mapping of parent to child ids

        This is used to construct a CTE. That table consists of two columns - parent_id and child_id,
        which is used in various queries
        """
        return []

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
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
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
        owner_user
            Name or ID of the user who owns the record
        owner_group
            Group with additional permission for these records
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

    def update_completed_task(self, session: Session, record_id: int, result: AllResultTypes) -> None:
        """
        Update a record ORM based on the result of a successfully-completed computation
        """
        raise NotImplementedError(f"update_completed_task not implemented for {type(self)}! This is a developer error")

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

    def get_children_status(self, record_id: int, *, session: Optional[Session] = None) -> Dict[RecordStatusEnum, int]:
        # Get the SQL 'select' statements from the handlers
        select_stmts = self.get_children_select()

        if not select_stmts:
            return {}

        select_cte = union(*select_stmts).cte()

        stmt = select(BaseRecordORM.status, func.count())
        stmt = stmt.join(select_cte, select_cte.c.child_id == BaseRecordORM.id)
        stmt = stmt.where(select_cte.c.parent_id == record_id)
        stmt = stmt.group_by(BaseRecordORM.status)

        with self.root_socket.optional_session(session, True) as session:
            res = session.execute(stmt).all()
            return {x: y for x, y in res}

    def get_children_errors(self, record_id: int, *, session: Optional[Session] = None) -> List[int]:
        # Get the SQL 'select' statements from the handlers
        select_stmts = self.get_children_select()

        if not select_stmts:
            return []

        select_cte = union(*select_stmts).cte()

        stmt = select(select_cte.c.child_id).distinct(select_cte.c.child_id)
        stmt = stmt.join(BaseRecordORM, BaseRecordORM.id == select_cte.c.child_id)
        stmt = stmt.where(select_cte.c.parent_id == record_id)
        stmt = stmt.where(BaseRecordORM.status == RecordStatusEnum.error)

        with self.root_socket.optional_session(session, True) as session:
            return session.execute(stmt).scalars().all()


class RecordSocket:
    """
    Root socket for all record sockets

    This socket contains lots of functionality that is common for all records, as well as
    acts as a root socket for all specific record sockets.
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        # All the subsockets
        from .services.socket import ServiceSubtaskRecordSocket
        from .singlepoint.record_socket import SinglepointRecordSocket
        from .optimization.record_socket import OptimizationRecordSocket
        from .torsiondrive.record_socket import TorsiondriveRecordSocket
        from .gridoptimization.record_socket import GridoptimizationRecordSocket
        from .reaction.record_socket import ReactionRecordSocket
        from .manybody.record_socket import ManybodyRecordSocket
        from .neb.record_socket import NEBRecordSocket

        self.service_subtask = ServiceSubtaskRecordSocket(root_socket)
        self.singlepoint = SinglepointRecordSocket(root_socket)
        self.optimization = OptimizationRecordSocket(root_socket)
        self.torsiondrive = TorsiondriveRecordSocket(root_socket)
        self.gridoptimization = GridoptimizationRecordSocket(root_socket)
        self.reaction = ReactionRecordSocket(root_socket)
        self.manybody = ManybodyRecordSocket(root_socket)
        self.neb = NEBRecordSocket(root_socket)

        self._handler_map: Dict[str, BaseRecordSocket] = {
            "servicesubtask": self.service_subtask,
            "singlepoint": self.singlepoint,
            "optimization": self.optimization,
            "torsiondrive": self.torsiondrive,
            "gridoptimization": self.gridoptimization,
            "reaction": self.reaction,
            "manybody": self.manybody,
            "neb": self.neb,
        }

        self._handler_map_by_schema: Dict[str, BaseRecordSocket] = {
            "qcschema_output": self.singlepoint,
            "qcschema_optimization_output": self.optimization,
        }

        self._handler_map_by_input: Dict[Any, BaseRecordSocket] = {
            self.singlepoint.record_input_type: self.singlepoint,
            self.optimization.record_input_type: self.optimization,
            self.torsiondrive.record_input_type: self.torsiondrive,
            self.gridoptimization.record_input_type: self.gridoptimization,
            self.reaction.record_input_type: self.reaction,
            self.manybody.record_input_type: self.manybody,
            self.neb.record_input_type: self.neb,
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

        # Union them into a single CTE
        self._child_cte = union(*selects).cte()

    def get_socket(self, record_type: str) -> BaseRecordSocket:
        """
        Get the socket for a specific kind of record type
        """

        handler = self._handler_map.get(record_type, None)
        if handler is None:
            raise MissingDataError(f"Cannot find handler for type {record_type}")
        return handler

    def get_dependency_ids(self, session: Session, record_ids: Iterable[int]) -> List[int]:
        """
        Get record ids of dependencies of the given records

        This only makes sense for records that are services. These services have
        records that are dependencies of that service.

        Note that this function takes in and returns record ids, not service queue ids.
        """

        # List may contain duplicates. So be tolerant of that!
        stmt = select(ServiceDependencyORM.record_id)
        stmt = stmt.join(ServiceQueueORM, ServiceQueueORM.id == ServiceDependencyORM.service_id)
        stmt = stmt.where(ServiceQueueORM.record_id.in_(record_ids))
        return session.execute(stmt).scalars().all()

    def get_children_ids(self, session: Session, record_ids: Iterable[int]) -> List[int]:
        """
        Recursively obtain the IDs of all children records of the given records
        """

        # Do in batches to prevent really huge queries
        # First, do just direct children
        direct_children: List[int] = []
        for id_batch in chunk_iterable(record_ids, 100):
            stmt = select(self._child_cte.c.child_id).where(self._child_cte.c.parent_id.in_(id_batch))
            children_ids = session.execute(stmt).scalars().unique().all()
            direct_children.extend(children_ids)

        if len(direct_children) == 0:
            return []

        # Now, recursively find all children of children, etc
        return direct_children + self.get_children_ids(session, direct_children)

    def get_short_descriptions(
        self, record_ids: Iterable[int], *, session: Optional[Session] = None
    ) -> List[Dict[str, Any]]:
        """
        Obtain short descriptions of records
        """

        stmt = select(BaseRecordORM)
        stmt = stmt.options(defer(BaseRecordORM.properties))  # no need to load these, and they can be big
        stmt = stmt.where(BaseRecordORM.id.in_(record_ids))

        with self.root_socket.optional_session(session, True) as session:
            record_orms = session.execute(stmt).scalars().all()

            found_ids = {r.id for r in record_orms}
            if found_ids != set(record_ids):
                raise MissingDataError("Cannot find all record ids")

            desc_map = {}
            for r in record_orms:
                desc_map[r.id] = {
                    "id": r.id,
                    "record_type": r.record_type,
                    "status": r.status,
                    "created_on": r.created_on,
                    "modified_on": r.modified_on,
                    "description": r.short_description,
                }

            return [desc_map[i] for i in record_ids]

    def get_parent_ids(self, session: Session, record_ids: Iterable[int]) -> List[int]:
        """
        Recursively obtain the IDs of all parent records of the given records
        """

        # Do in batches to prevent really huge queries
        # First, do just direct parents
        direct_parents: List[int] = []

        for id_batch in chunk_iterable(record_ids, 100):
            stmt = select(self._child_cte.c.parent_id).where(self._child_cte.c.child_id.in_(id_batch))
            parent_ids = session.execute(stmt).scalars().unique().all()
            direct_parents.extend(parent_ids)

        if len(direct_parents) == 0:
            return []

        return direct_parents + self.get_parent_ids(session, direct_parents)

    def get_relative_ids(self, session: Session, record_ids: Iterable[int]) -> List[int]:
        """
        Recursively obtain the IDs of all parent and children records of the given records
        """

        all_relatives = set()

        records_to_search = set(record_ids)
        while records_to_search:
            direct_relatives = set()

            # do in batches to prevent really huge queries
            for id_batch in chunk_iterable(records_to_search, 100):
                stmt = select(self._child_cte.c.parent_id).where(self._child_cte.c.child_id.in_(id_batch))
                parent_ids = session.execute(stmt).scalars().unique().all()

                stmt = select(self._child_cte.c.child_id).where(self._child_cte.c.parent_id.in_(id_batch))
                child_ids = session.execute(stmt).scalars().unique().all()

                direct_relatives.update(parent_ids)
                direct_relatives.update(child_ids)

            # Search through the new direct relatives, but not any we have done already
            records_to_search = direct_relatives - all_relatives
            all_relatives.update(direct_relatives)

        return list(all_relatives)

    def query_base(
        self,
        stmt: Select,
        orm_type: Type[BaseRecordORM],
        query_data: RecordQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Core query functionality of all records

        Each record type will first create an SQLAlchemy Select stmt with all the record-specific
        fields and options. Then it will pass that stmt to this function, which will add queries/filters for all the
        common fields (ids, manager_names, created/modified, etc) that are passed in through
        `query_data`

        Parameters
        ----------
        stmt
            An SQLAlchemy select statement with record-specific filters
        orm_type
            The type of ORM we are querying for
        query_data
            Common record filters to add to the query
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A list of record ids that were found in the database.
        """

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
            and_query.append(orm_type.created_on <= query_data.created_before)
        if query_data.created_after is not None:
            and_query.append(orm_type.created_on >= query_data.created_after)
        if query_data.modified_before is not None:
            and_query.append(orm_type.modified_on <= query_data.modified_before)
        if query_data.modified_after is not None:
            and_query.append(orm_type.modified_on >= query_data.modified_after)

        if query_data.owner_user is not None:
            stmt = stmt.join(UserIDMapSubquery)

            int_ids = {x for x in query_data.owner_user if isinstance(x, int) or x.isnumeric()}
            str_names = set(query_data.owner_user) - int_ids

            and_query.append(or_(UserIDMapSubquery.username.in_(str_names), UserIDMapSubquery.id.in_(int_ids)))

        if query_data.owner_group is not None:
            stmt = stmt.join(GroupIDMapSubquery)

            int_ids = {x for x in query_data.owner_group if isinstance(x, int) or x.isnumeric()}
            str_names = set(query_data.owner_group) - int_ids

            and_query.append(or_(GroupIDMapSubquery.groupname.in_(str_names), GroupIDMapSubquery.id.in_(int_ids)))

        if query_data.parent_id is not None:
            # We alias the cte because we might join on it twice
            parent_cte = aliased(self._child_cte)

            stmt = stmt.join(parent_cte, parent_cte.c.child_id == orm_type.id)
            stmt = stmt.where(parent_cte.c.parent_id.in_(query_data.parent_id))

        if query_data.child_id is not None:
            # We alias the cte because we might join on it twice
            child_cte = aliased(self._child_cte)

            stmt = stmt.join(child_cte, child_cte.c.parent_id == orm_type.id)
            stmt = stmt.where(child_cte.c.child_id.in_(query_data.child_id))

        if query_data.dataset_id is not None:
            # Join with the CTE from the dataset socket
            # This contains all records and dataset ids
            cte = self.root_socket.datasets._record_cte
            stmt = stmt.join(cte, cte.c.record_id == orm_type.id)
            stmt = stmt.where(cte.c.dataset_id.in_(query_data.dataset_id))

        with self.root_socket.optional_session(session, True) as session:
            stmt = stmt.where(*and_query)

            if query_data.cursor is not None:
                stmt = stmt.where(orm_type.id < query_data.cursor)

            stmt = stmt.order_by(orm_type.id.desc())
            stmt = stmt.limit(query_data.limit)
            stmt = stmt.distinct(orm_type.id)
            record_ids = session.execute(stmt).scalars().all()

        return record_ids

    def query(
        self,
        query_data: RecordQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[int]:
        """
        Query records of all types based on common fields

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

        wp = with_polymorphic(BaseRecordORM, "*")
        stmt = select(wp.id)

        return self.query_base(
            stmt,
            orm_type=wp,
            query_data=query_data,
            session=session,
        )

    def get_base(
        self,
        orm_type: Type[BaseRecordORM],
        record_ids: Sequence[int],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        additional_options: Optional[List[Any]] = None,
        *,
        session: Optional[Session] = None,
    ):
        options = []
        if include is not None:
            include_history = is_included("compute_history", include, exclude, False)
            include_outputs = is_included("outputs", include, exclude, False)

            if include_history or include_outputs:
                options.append(selectinload(orm_type.compute_history))
            if include_outputs:
                options.append(
                    selectinload(orm_type.compute_history, RecordComputeHistoryORM.outputs).undefer(OutputStoreORM.data)
                )
            if is_included("task", include, exclude, False):
                options.append(joinedload(orm_type.task))
            if is_included("service", include, exclude, False):
                options.append(joinedload(orm_type.service))
            if is_included("comments", include, exclude, False):
                options.append(selectinload(orm_type.comments))
            if is_included("native_files", include, exclude, False):
                options.append(selectinload(orm_type.native_files).options(undefer(NativeFileORM.data)))

        if additional_options:
            options.extend(additional_options)

        with self.root_socket.optional_session(session, True) as session:
            return get_general(session, orm_type, orm_type.id, record_ids, include, exclude, missing_ok, options)

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
        Obtain records of all types with specified IDs

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
            Records (as dictionaries) in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the record was missing.
        """

        # If all columns are included, then we can load
        # the data from derived classes as well.
        if (include is None or "*" in include or "**" in include) and not exclude:
            wp = with_polymorphic(BaseRecordORM, "*")
        else:
            wp = BaseRecordORM

        return self.get_base(wp, record_ids, include, exclude, missing_ok, session=session)

    def update_completed_task(
        self, session: Session, record_id: int, record_type: str, result: AllResultTypes, manager_name: str
    ):
        """
        Update a record ORM based on the results of a successfully-completed computation

        Parameters
        ----------
        session
            An existing SQLAlchemy session
        record_id
            ID of the record to update and mark as completed
        record_type
            Type of record to update
        result
            The result of the computation. This should be a successful result
        manager_name
            The manager that produced the result
        """

        # Do these before calling the record-specific handler
        # (these may pull stuff out of extras)
        history_orm = compute_history_orms_from_schema_v1(result)
        history_orm.record_id = record_id
        history_orm.manager_name = manager_name
        session.add(history_orm)

        native_files_orms = native_files_orms_from_schema_v1(result)
        for v in native_files_orms.values():
            v.record_id = record_id
            session.add(v)

        # Now update fields specific to each record
        record_socket = self._handler_map[record_type]
        record_socket.update_completed_task(session, record_id, result)

        # Now extras and properties
        extras, properties = build_extras_properties(result)

        # What updates we have for this record
        record_updates = {
            "extras": extras,
            "properties": properties,
            "status": RecordStatusEnum.complete,
            "manager_name": manager_name,
            "modified_on": history_orm.modified_on,
        }

        # Actually update the record
        stmt = update(BaseRecordORM).where(BaseRecordORM.id == record_id).values(record_updates)
        session.execute(stmt)

        # Delete the task from the task queue since it is completed
        stmt = delete(TaskQueueORM).where(TaskQueueORM.record_id == record_id)
        session.execute(stmt)

    def update_failed_task(self, session: Session, record_id: int, failed_result: FailedOperation, manager_name: str):
        """
        Update a record ORM whose computation has failed, and mark it as errored

        Parameters
        ----------
        session
            An existing SQLAlchemy session
        record_id
            ID of the record to update and mark as completed
        failed_result
            The (failed) result of the computation
        manager_name
            The manager that produced the result
        """

        if not isinstance(failed_result, FailedOperation):
            raise RuntimeError("Developer error - this function only handles FailedOperation results")

        # Handle outputs, which are a bit different in FailedOperation
        # Error is special in a FailedOperation
        error = failed_result.error
        if error is None:
            error = _default_error

        error_out_orm = create_output_orm(OutputTypeEnum.error, error.dict())
        all_outputs = {OutputTypeEnum.error: error_out_orm}

        # Get the rest of the outputs
        # This is stored in "input_data" (I know...)
        # "input_data" can be anything. So ignore if it isn't a dict
        if isinstance(failed_result.input_data, dict):
            stdout = failed_result.input_data.get("stdout", None)
            stderr = failed_result.input_data.get("stderr", None)

            if stdout is not None:
                stdout_orm = create_output_orm(OutputTypeEnum.stdout, stdout)
                all_outputs[OutputTypeEnum.stdout] = stdout_orm
            if stderr is not None:
                stderr_orm = create_output_orm(OutputTypeEnum.stderr, stderr)
                all_outputs[OutputTypeEnum.stderr] = stderr_orm

        # Build the history orm
        history_orm = RecordComputeHistoryORM()
        history_orm.status = RecordStatusEnum.error
        history_orm.manager_name = manager_name
        history_orm.modified_on = now_at_utc()
        history_orm.outputs = all_outputs
        history_orm.record_id = record_id
        session.add(history_orm)

        # What updates we have for this record
        record_updates = {
            "status": RecordStatusEnum.error,
            "modified_on": history_orm.modified_on,
            "manager_name": manager_name,
        }

        # Actually update the record
        stmt = update(BaseRecordORM).where(BaseRecordORM.id == record_id).values(record_updates)
        session.execute(stmt)

    def initialize_service(self, session: Session, service_orm: ServiceQueueORM) -> None:
        """
        Initialize a service

        A service is initialized when it moves from a waiting state to a running state. After it is
        initialized, iterate_service may be called.
        """

        if not service_orm.record.is_service:
            raise RuntimeError("Cannot initialize a record that is not a service")

        record_type = service_orm.record.record_type
        service_socket = self._handler_map[record_type]

        if not service_socket.available():
            self._logger.warning(
                f"Cannot initialize service id={service_orm.id} - socket is not available."
                "Are the correct packages installed?"
            )
        else:
            service_socket.initialize_service(session, service_orm)

    def iterate_service(self, session: Session, service_orm: ServiceQueueORM) -> bool:
        """
        Iterate a service with successfully-completed dependencies

        The server will check to see if all dependencies are successfully completed. If they are, this function
        is called.

        Cases where the service has an errored dependency are handled in :meth:`RecordSocket.update_failed_service`.
        """

        if not service_orm.record.is_service:
            raise RuntimeError("Cannot iterate a record that is not a service")

        record_type = service_orm.record.record_type
        service_socket = self._handler_map[record_type]

        if not service_socket.available():
            self._logger.warning(
                f"Cannot iterate service id={service_orm.id} - socket is not available."
                "Are the correct packages installed?"
            )
            return False

        # Actually iterate the service
        finished = service_socket.iterate_service(session, service_orm)

        if not finished:
            # Update the sort_date on any tasks that were created
            # The sort date should almost always be the created_on of the parent service
            # This way, earlier services will have their tasks run first, rather than having to wait
            # for all other services' tasks to finish
            # But, it could be the original submission date of the record
            parent_created_on = service_orm.record.created_on

            stmt = """
                WITH tq_cte AS (
                    SELECT tq.id FROM task_queue tq
                    INNER JOIN service_dependency sd ON tq.record_id = sd.record_id
                    WHERE sd.service_id = :service_id
                    AND tq.available = true
                    AND tq.sort_date > :parent_created_on
                    FOR UPDATE OF tq SKIP LOCKED
                )
                UPDATE task_queue AS tq
                SET sort_date = :parent_created_on
                FROM tq_cte
                WHERE tq_cte.id = tq.id
            """

            session.execute(text(stmt), {"parent_created_on": parent_created_on, "service_id": service_orm.id})

        return finished

    def update_failed_service(self, session, record_orm: BaseRecordORM, error_info: Dict[str, Any]):
        """
        Handle a service with errored dependencies
        """
        record_orm.status = RecordStatusEnum.error

        error_orm = create_output_orm(OutputTypeEnum.error, error_info)
        record_orm.compute_history[-1].status = RecordStatusEnum.error

        upsert_output(session, record_orm, error_orm)
        record_orm.compute_history[-1].modified_on = now_at_utc()

        record_orm.status = RecordStatusEnum.error
        record_orm.modified_on = now_at_utc()

    def add_from_input(
        self,
        record_input: AllInputTypes,
        compute_tag: str,
        compute_priority: PriorityEnum,
        owner_user: Optional[Union[int, str]],
        owner_group: Optional[Union[int, str]],
        find_existing: bool,
        *,
        session: Optional[Session] = None,
    ):
        record_socket = self._handler_map_by_input[type(record_input)]
        return record_socket.add_from_input(
            record_input, compute_tag, compute_priority, owner_user, owner_group, find_existing, session=session
        )

    def insert_complete_schema_v1(self, session: Session, results: Sequence[AllResultTypes]) -> List[int]:
        """
        Insert records into the database from a QCSchema result

        This will always create new ORMs from scratch, and not update any existing records.
        """

        if len(results) == 0:
            return []

        # Map of schema name stored in the result to (index, result)
        type_map: Dict[str, List[Tuple[int, AllResultTypes]]] = defaultdict(list)

        for idx, result in enumerate(results):
            if isinstance(result, FailedOperation) or not result.success:
                raise UserReportableError("Cannot insert a completed, failed operation")

            type_map[result.schema_name].append((idx, result))

        # Key is the index in the original "results" dict
        to_add: List[Tuple[int, BaseRecordORM]] = []

        for schema_name, idx_results in type_map.items():
            # Get the outputs & status, storing in the history orm.
            # Do this before calling the individual record handlers since it modifies extras
            # (looking for compressed outputs and compressed native files)

            # idx_results is a list of tuple (idx, result). idx is the index in the
            # original results (function parameter)
            type_results = [r for _, r in idx_results]
            history_orms = [compute_history_orms_from_schema_v1(r) for r in type_results]
            native_files_orms = [native_files_orms_from_schema_v1(r) for r in type_results]

            # Now the record-specific stuff
            handler = self._handler_map_by_schema[schema_name]
            record_orms = handler.insert_complete_schema_v1(session, type_results)

            for record_orm, history_orm, native_files_orm, (idx, type_result) in zip(
                record_orms, history_orms, native_files_orms, idx_results
            ):
                record_orm.is_service = False

                # Now extras and properties
                extras, properties = build_extras_properties(type_result)
                record_orm.extras = extras
                record_orm.properties = properties

                # Now back to the common modifications
                record_orm.compute_history.append(history_orm)
                record_orm.native_files = native_files_orm
                record_orm.status = history_orm.status
                record_orm.modified_on = history_orm.modified_on

                to_add.append((idx, record_orm))

        to_add.sort()

        # Check that everything is in the original order
        assert tuple(idx for idx, _ in to_add) == tuple(range(len(to_add)))
        to_add_orm = [o for _, o in to_add]

        session.add_all(to_add_orm)
        session.flush()

        return [o.id for o in to_add_orm]

    def add_comment(
        self, record_ids: Sequence[int], user_id: Optional[int], comment: str, *, session: Optional[Session] = None
    ) -> UpdateMetadata:
        """
        Adds a comment to records

        Comments are free-form strings attached to a record.

        Parameters
        ----------
        record_ids
            Records to add the comment to
        user_id
            The ID of the user that is adding the comment
        comment
            The comment to be attached to the record
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        with self.root_socket.optional_session(session) as session:
            # find only existing records
            stmt = select(BaseRecordORM.id).where(BaseRecordORM.id.in_(record_ids))
            stmt = stmt.with_for_update()
            existing_ids = session.execute(stmt).scalars().all()

            for rid in existing_ids:
                comment_orm = RecordCommentORM(
                    record_id=rid,
                    user_id=user_id,
                    comment=comment,
                )
                session.add(comment_orm)

            updated_idx = [idx for idx, rid in enumerate(record_ids) if rid in existing_ids]
            missing_idx = [idx for idx, rid in enumerate(record_ids) if rid not in existing_ids]

            return UpdateMetadata(
                updated_idx=updated_idx,
                errors=[(idx, "Record does not exist") for idx in missing_idx],
            )

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
            is used, it will be flushed (but not committed) before returning from this function.

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
                r.modified_on = now_at_utc()
                r.manager_name = None
                r.task.available = True

            return [r.id for r in record_orms]

    def _revert_common(
        self,
        record_ids: Sequence[int],
        applicable_status: Iterable[RecordStatusEnum],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Internal function for resetting, uncancelling, undeleting, or uninvalidation

        This will also re-create tasks & services as necessary

        Parameters
        ----------
        record_ids
            Reset the status of these record ids
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was updated
        """

        if not record_ids:
            return UpdateMetadata()

        all_ids = set(record_ids)

        with self.root_socket.optional_session(session) as session:
            # We always apply these operations to children, but never to parents
            children_ids = self.get_children_ids(session, record_ids)
            all_ids.update(children_ids)

            updated_ids = []
            for id_batch in chunk_iterable(all_ids, 100):
                # Select records with a resettable status
                # All are resettable except complete and running
                # Can't do inner join because task may not exist
                stmt = select(BaseRecordORM).options(
                    joinedload(BaseRecordORM.task),
                    load_only(BaseRecordORM.id, BaseRecordORM.status, BaseRecordORM.manager_name),
                )
                stmt = stmt.options(selectinload(BaseRecordORM.info_backup))
                stmt = stmt.where(BaseRecordORM.status.in_(applicable_status))
                stmt = stmt.where(BaseRecordORM.id.in_(id_batch))
                stmt = stmt.with_for_update(of=[BaseRecordORM])
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
                                self._logger.warning(
                                    f"Record {r_orm.id} has a task and also an entry in the backup table!"
                                )
                                session.delete(r_orm.task)

                            # we leave service queue entries alone
                            if not r_orm.is_service:
                                BaseRecordSocket.create_task(
                                    r_orm, last_info.old_compute_tag, last_info.old_compute_priority
                                )

                    elif r_orm.status in [RecordStatusEnum.running, RecordStatusEnum.error] and not r_orm.info_backup:
                        if not r_orm.is_service and r_orm.task is None:
                            raise RuntimeError(f"resetting a record with status {r_orm.status} with no task")
                        if r_orm.is_service and r_orm.service is None:
                            raise RuntimeError(f"resetting a record with status {r_orm.status} with no service")

                        # Move the record back to "waiting" for a manager/service periodics to pick it up
                        r_orm.status = RecordStatusEnum.waiting
                        r_orm.manager_name = None

                        if not r_orm.is_service:
                            r_orm.task.available = True

                    else:
                        if r_orm.info_backup:
                            raise RuntimeError(f"resetting record with status {r_orm.status} with backup info present")
                        else:
                            raise RuntimeError(
                                f"resetting record with status {r_orm.status} without backup info present"
                            )

                    r_orm.modified_on = now_at_utc()

                    # Sanity check
                    if not r_orm.is_service:
                        if r_orm.status == RecordStatusEnum.waiting:
                            assert r_orm.task.available == True
                        else:
                            assert r_orm.task is None or r_orm.task.available == False

                updated_ids.extend([r.id for r in record_data])

            # put in order of the input parameter
            error_ids = set(record_ids) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_ids) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_ids) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be reset") for idx in error_idx]
            n_children_updated = len(updated_ids) - len(updated_idx)

            return UpdateMetadata(updated_idx=updated_idx, errors=errors, n_children_updated=n_children_updated)

    def _cancel_common(
        self,
        record_ids: Sequence[int],
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
        The connotation of these operations is different, but internally they behave the same.

        Parameters
        ----------
        record_ids
            Reset the status of these record ids
        new_status
            What the new status of the record should be
        propagate_to_children
            Apply the operation to children as well
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was updated
        """

        if len(record_ids) == 0:
            return UpdateMetadata()

        all_ids = set(record_ids)

        with self.root_socket.optional_session(session) as session:
            if propagate_to_children:
                # We always propagate to parents. So recursively find all related records
                relative_ids = self.get_relative_ids(session, record_ids)
                all_ids.update(relative_ids)
            else:
                # Always propagate these changes to parents
                parent_ids = self.get_parent_ids(session, record_ids)
                all_ids.update(parent_ids)

            updated_ids = []
            for id_batch in chunk_iterable(all_ids, 100):
                stmt = select(BaseRecordORM).options(
                    joinedload(BaseRecordORM.task),
                    load_only(BaseRecordORM.id, BaseRecordORM.status, BaseRecordORM.manager_name),
                )
                stmt = stmt.where(BaseRecordORM.status.in_(applicable_status))
                stmt = stmt.where(BaseRecordORM.id.in_(id_batch))
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
                        old_tag = r.task.compute_tag
                        old_priority = r.task.compute_priority
                        session.delete(r.task)

                    # If this is a service, we leave the
                    # the entry in the service queue since it contains
                    # the current service state info

                    # Store the old info in the backup table
                    backup_info = RecordInfoBackupORM(
                        record_id=r.id,
                        old_status=r.status,
                        old_compute_tag=old_tag,
                        old_compute_priority=old_priority,
                        modified_on=now_at_utc(),
                    )
                    session.add(backup_info)

                    r.modified_on = now_at_utc()
                    r.status = new_status

                updated_ids.extend([r.id for r in record_orms])

            # put in order of the input parameter
            error_ids = set(record_ids) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_ids) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_ids) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be cancelled/deleted/invalidated") for idx in error_idx]
            n_children_updated = len(updated_ids) - len(updated_idx)

            return UpdateMetadata(updated_idx=updated_idx, errors=errors, n_children_updated=n_children_updated)

    def reset(self, record_ids: Sequence[int], *, session: Optional[Session] = None):
        """
        Resets errored records to be waiting again
        """

        return self._revert_common(record_ids, applicable_status=[RecordStatusEnum.error], session=session)

    def reset_running(self, record_ids: Sequence[int], *, session: Optional[Session] = None):
        """
        Resets running records to be waiting again
        """

        return self._revert_common(record_ids, applicable_status=[RecordStatusEnum.running], session=session)

    def delete(
        self,
        record_ids: Sequence[int],
        soft_delete: bool = True,
        delete_children: bool = True,
        *,
        session: Optional[Session] = None,
    ) -> DeleteMetadata:
        """
        Delete records from the database

        If soft_delete is True, then the record is just marked as deleted and actually deletion may
        happen later. Soft delete can be undone with undelete

        A deleted record will not be picked up by any manager.

        Parameters
        ----------
        record_ids
            Reset the status of these record ids
        soft_delete
            Don't actually delete the record, just mark it for later deletion
        delete_children
            If True, attempt to delete child records as well
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was deleted (or marked as deleted)
        """

        if len(record_ids) == 0:
            return DeleteMetadata()

        if soft_delete:
            # anything can be deleted except something already deleted
            meta = self._cancel_common(
                record_ids,
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
            children_ids = []

            if delete_children:
                children_ids = self.get_children_ids(session, record_ids)

            del_id_1 = [(x,) for x in record_ids]
            del_id_2 = [(x,) for x in children_ids]
            meta = delete_general(session, BaseRecordORM, BaseRecordORM.id, del_id_1)
            ch_meta = delete_general(session, BaseRecordORM, BaseRecordORM.id, del_id_2)

            meta_dict = meta.dict()
            meta_dict["n_children_deleted"] = ch_meta.n_deleted
            return DeleteMetadata(**meta_dict)

    def cancel(
        self,
        record_ids: Sequence[int],
        cancel_children: bool = True,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Marks records as cancelled

        A cancelled record will not be picked up by a manager.

        Parameters
        ----------
        record_ids
            Reset the status of these record ids
        cancel_children
            Cancel all children as well
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        return self._cancel_common(
            record_ids,
            {RecordStatusEnum.waiting, RecordStatusEnum.running, RecordStatusEnum.error},
            RecordStatusEnum.cancelled,
            propagate_to_children=cancel_children,
            session=session,
        )

    def invalidate(
        self,
        record_ids: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Marks records as invalid

        An invalid record is one that supposedly successfully completed. However, after review,
        is not correct.

        Parameters
        ----------
        record_ids
            Reset the status of these record ids
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        return self._cancel_common(
            record_ids,
            {RecordStatusEnum.complete},
            RecordStatusEnum.invalid,
            propagate_to_children=False,
            session=session,
        )

    def undelete(
        self,
        record_ids: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Undeletes records that were soft deleted

        This will always undelete children whenever possible
        """
        if len(record_ids) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            meta = self._revert_common(record_ids, applicable_status=[RecordStatusEnum.deleted], session=session)

            return UpdateMetadata(
                updated_idx=meta.updated_idx,
                error_description=meta.error_description,
                errors=meta.errors,
                n_children_updated=meta.n_children_updated,
            )

    def uncancel(
        self,
        record_ids: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Uncancels records that were previously cancelled

        This will always uncancel children whenever possible
        """
        if len(record_ids) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            return self._revert_common(record_ids, applicable_status=[RecordStatusEnum.cancelled], session=session)

    def uninvalidate(
        self,
        record_ids: Sequence[int],
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Uninvalidates records that were previously uninvalidated

        This will always uninvalidate children whenever possible
        """
        if len(record_ids) == 0:
            return UpdateMetadata()

        with self.root_socket.optional_session(session) as session:
            return self._revert_common(record_ids, applicable_status=[RecordStatusEnum.invalid], session=session)

    def modify(
        self,
        record_ids: Sequence[int],
        new_compute_tag: Optional[str] = None,
        new_compute_priority: Optional[RecordStatusEnum] = None,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Modifies a record's task's priority and tag

        Records without a corresponding task (completed, etc) will not be modified. Running tasks
        will also not be modified.

        An empty string for new_compute_tag will be treated the same as if you had passed it None

        Parameters
        ----------
        record_ids
            Modify the tasks corresponding to these record ids
        new_compute_tag
            New tag for the task. If None, keep the existing tag
        new_compute_priority
            New priority for the task. If None, then keep the existing priority
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about what was updated
        """

        if new_compute_tag == "":
            new_compute_tag = None

        # Do we have anything to do?
        if new_compute_tag is None and new_compute_priority is None:
            return UpdateMetadata()

        all_id = set(record_ids)

        with self.root_socket.optional_session(session) as session:
            # Get subtasks for all the records that are services
            subtask_id = self.get_dependency_ids(session, record_ids)
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
                if new_compute_tag is not None:
                    o.compute_tag = new_compute_tag
                if new_compute_priority is not None:
                    o.compute_priority = new_compute_priority

            # put in order of the input parameter
            # only pay attention to the records requested (ie, not subtasks)
            updated_ids = [t.record_id for t in all_orm]
            error_ids = set(record_ids) - set(updated_ids)
            updated_idx = [idx for idx, rid in enumerate(record_ids) if rid in updated_ids]
            error_idx = [idx for idx, rid in enumerate(record_ids) if rid in error_ids]
            errors = [(idx, "Record is missing or cannot be modified") for idx in error_idx]
            n_children_updated = len(all_orm) - (len(updated_idx) + len(error_idx))

            return UpdateMetadata(updated_idx=updated_idx, errors=errors, n_children_updated=n_children_updated)

    def modify_generic(
        self,
        record_ids: Sequence[int],
        user_id: Optional[int],
        status: Optional[RecordStatusEnum] = None,
        compute_priority: Optional[PriorityEnum] = None,
        compute_tag: Optional[str] = None,
        comment: Optional[str] = None,
        *,
        session: Optional[Session] = None,
    ) -> UpdateMetadata:
        """
        Modify multiple things about a record at once

        This function allows for changing the status, tag, priority, and comment in a single function call

        Parameters
        ----------
        record_ids
            IDs of the records to modify
        user_id
            ID of the user modifying the records
        status
            New status for the records. Only certain status transitions will be allowed.
        compute_priority
            New priority for these records
        compute_tag
            New tag for these records
        comment
            Adds a new comment to these records
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        if len(record_ids) == 0:
            return UpdateMetadata()

        # do all in a single session
        with self.root_socket.optional_session(session) as session:
            if status is not None:
                if status == RecordStatusEnum.waiting:
                    ret = self.root_socket.records.reset(record_ids=record_ids, session=session)
                if status == RecordStatusEnum.cancelled:
                    ret = self.root_socket.records.cancel(record_ids=record_ids, session=session)
                if status == RecordStatusEnum.invalid:
                    ret = self.root_socket.records.invalidate(record_ids=record_ids, session=session)

                # ignore all other statuses

            if compute_tag is not None or compute_priority is not None:
                ret = self.root_socket.records.modify(
                    record_ids,
                    new_compute_tag=compute_tag,
                    new_compute_priority=compute_priority,
                    session=session,
                )

            if comment:
                ret = self.root_socket.records.add_comment(
                    record_ids=record_ids,
                    user_id=user_id,
                    comment=comment,
                    session=session,
                )

            return ret

    def revert_generic(self, record_id: Sequence[int], revert_status: RecordStatusEnum) -> UpdateMetadata:
        """
        Reverts the status of a record to the previous status

        This dispatches the request to the proper function based on the status. For example,
        if `revert_status` is "deleted", then this will call undelete.
        """

        if revert_status == RecordStatusEnum.cancelled:
            return self.uncancel(record_id)

        if revert_status == RecordStatusEnum.invalid:
            return self.uninvalidate(record_id)

        if revert_status == RecordStatusEnum.deleted:
            return self.undelete(record_id)

        raise RuntimeError(f"Unknown status to revert: ", revert_status)

    def get_waiting_reason(self, record_id: int, *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Determines why a record/task is waiting

        This function takes the record ID, not the task ID
        """

        # For getting the record and task info
        rec_stmt = select(
            BaseRecordORM.status, BaseRecordORM.is_service, TaskQueueORM.compute_tag, TaskQueueORM.required_programs
        )
        rec_stmt = rec_stmt.join(TaskQueueORM, TaskQueueORM.record_id == BaseRecordORM.id, isouter=True)
        rec_stmt = rec_stmt.where(BaseRecordORM.id == record_id)

        # All active managers
        manager_stmt = select(ComputeManagerORM.name, ComputeManagerORM.compute_tags, ComputeManagerORM.programs)
        manager_stmt = manager_stmt.where(ComputeManagerORM.status == ManagerStatusEnum.active)

        with self.root_socket.optional_session(session, True) as session:
            rec = session.execute(rec_stmt).one_or_none()

            if rec is None:
                return {"reason": "Record does not exist"}

            rec_status, rec_isservice, rec_tag, rec_programs = rec

            if rec_isservice is True:
                return {"reason": "Record is a service"}

            if rec_status != RecordStatusEnum.waiting:
                return {"reason": "Record is not waiting"}

            if rec_tag is None or rec_programs is None:
                return {"reason": "Missing task? This is a developer error"}

            # Record is waiting. Test each active manager
            managers = session.execute(manager_stmt).all()

            if len(managers) == 0:
                return {"reason": "No active managers"}

            rec_programs = set(rec_programs)

            ret = {"details": {}, "reason": "No manager matches programs & tags"}
            for m_name, m_tags, m_programs in managers:
                missing_programs = rec_programs - m_programs.keys()
                if missing_programs:
                    ret["details"][m_name] = f"Manager missing programs: {missing_programs}"
                elif rec_tag not in m_tags and "*" not in m_tags:
                    ret["details"][m_name] = f'Manager does not handle tag "{rec_tag}"'
                else:
                    ret["details"][m_name] = f"Manager is busy"
                    ret["reason"] = f"Waiting for a free manager"

            return ret
