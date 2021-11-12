from __future__ import annotations

from datetime import datetime
import logging
from qcfractal.components.records.db_models import BaseResultORM
from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload, load_only
from qcfractal.interface.models import RecordStatusEnum, FailedOperation
from qcfractal.portal.metadata_models import QueryMetadata
from qcfractal.db_socket.helpers import get_query_proj_columns, get_count_2, calculate_limit, get_general

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.interface.models import AllResultTypes
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Iterable

    ProcedureDict = Dict[str, Any]


class RecordSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)
        self._limit = root_socket.qcf_config.response_limits.record

        # All the subsockets
        from .singlepoint.sockets import SinglePointRecordSocket

        self.singlepoint = SinglePointRecordSocket(root_socket)

        self._handler_map = {"singlepoint": self.singlepoint}

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

        load_cols, load_rels = get_query_proj_columns(BaseResultORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(BaseResultORM.id.in_(id))
        if record_type is not None:
            and_query.append(BaseResultORM.record_type.in_(record_type))
        if manager_name is not None:
            and_query.append(BaseResultORM.manager_name.in_(manager_name))
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

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(BaseResultORM).where(*and_query)
            stmt = stmt.options(load_only(*load_cols))
            n_found = get_count_2(stmt)
            stmt = stmt.limit(limit).offset(skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

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
        Obtain results of any kind of record with specified IDs

        The returned information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
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

        # By default, exclude the full compute history and task
        default_exclude = {"compute_history", "task"}

        with self.root_socket.optional_session(session, True) as session:
            return get_general(
                session, BaseResultORM, BaseResultORM.id, id, include, exclude, default_exclude, missing_ok
            )

    def update_completed(self, session: Session, record_orm: BaseResultORM, result: AllResultTypes, manager_name: str):

        if isinstance(result, FailedOperation) or not result.success:
            raise RuntimeError("Developer error - this function only handles successful results")

        handler = self._handler_map[record_orm.record_type]
        return handler.update_completed(session, record_orm, result, manager_name)

    def update_failure(
        self, session: Session, record_orm: BaseResultORM, failed_result: FailedOperation, manager_name: str
    ):
        pass
