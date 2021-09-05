from __future__ import annotations

from datetime import datetime
import logging
from qcfractal.components.records.db_models import BaseResultORM
from sqlalchemy import and_
from sqlalchemy.orm import selectinload, load_only
from qcfractal.interface.models import RecordStatusEnum, QueryMetadata
from qcfractal.db_socket.helpers import get_query_proj_columns, get_count, calculate_limit

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from qcfractal.interface.models import ObjectId
    from typing import List, Dict, Tuple, Optional, Sequence, Any, Iterable

    ProcedureDict = Dict[str, Any]


class RecordSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.record

    def query(
        self,
        id: Optional[Iterable[ObjectId]] = None,
        procedure: Optional[Iterable[str]] = None,
        manager: Optional[Iterable[str]] = None,
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
            Query for procedures based on its ID
        procedure
            Query based on procedure type
        status
            The status of the procedure
        created_before
            Query for records created before this date
        created_after
            Query for records created after this date
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

        load_cols, _ = get_query_proj_columns(BaseResultORM, include, exclude)

        and_query = []
        if id is not None:
            and_query.append(BaseResultORM.id.in_(id))
        if procedure is not None:
            and_query.append(BaseResultORM.procedure.in_(procedure))
        if manager is not None:
            and_query.append(BaseResultORM.manager_name.in_(manager))
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

        with self._core_socket.optional_session(session, True) as session:
            query = session.query(BaseResultORM).filter(and_(*and_query))
            query = query.options(load_only(*load_cols))
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).yield_per(500)

            result_dicts = [x.dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

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
