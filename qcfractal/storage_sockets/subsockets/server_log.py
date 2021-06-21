from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime

from sqlalchemy import and_, func, text
import qcfractal
from qcfractal.interface.models import QueryMetadata
from qcfractal.storage_sockets.models import (
    AccessLogORM,
    InternalErrorLogORM,
    ServerStatsLogORM,
    CollectionORM,
    MoleculeORM,
    BaseResultORM,
    KVStoreORM,
    TaskQueueORM,
    ServiceQueueORM,
)
from qcfractal.storage_sockets.sqlalchemy_socket import calculate_limit
from qcfractal.storage_sockets.sqlalchemy_common import get_query_proj_columns, get_count

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
    from typing import Dict, Any, List, Optional, Tuple

    AccessLogDict = Dict[str, Any]
    ErrorLogDict = Dict[str, Any]
    ServerStatsDict = Dict[str, Any]
    AccessLogSummaryDict = Dict[str, Any]


class ServerLogSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._access_log_limit = core_socket.qcf_config.response_limits.access_logs
        self._server_log_limit = core_socket.qcf_config.response_limits.server_logs

    def save_access(self, log_data: AccessLogDict):
        """
        Saves information about an access to the database
        """

        with self._core_socket.session_scope() as session:
            log = AccessLogORM(**log_data)  # type: ignore
            session.add(log)

    def save_error(self, error_data: ErrorLogDict) -> int:
        """
        Saves information about an access to the database

        Returns
        -------
        :
            The id of the newly-created error
        """

        log = InternalErrorLogORM(**error_data, qcfractal_version=qcfractal.__version__)  # type: ignore
        with self._core_socket.session_scope() as session:
            session.add(log)
            session.commit()
            return log.id

    def update_stats(self):

        table_list = [CollectionORM, MoleculeORM, BaseResultORM, KVStoreORM, AccessLogORM, InternalErrorLogORM]
        db_name = self._core_socket.qcf_config.database.database_name

        table_counts = {}
        with self._core_socket.session_scope(True) as session:
            # total size of the database
            db_size = session.execute(text("SELECT pg_database_size(:dbname)"), {"dbname": db_name}).scalar()

            # Count the number of rows in each table
            for table in table_list:
                table_name = table.__tablename__
                table_counts[table_name] = session.execute(text(f"SELECT count(*) FROM {table_name}")).scalar()

            table_info_sql = f"""
                    SELECT relname                                AS table_name
                         , c.reltuples::BIGINT                    AS row_estimate
                         , pg_total_relation_size(c.oid)          AS total_bytes
                         , pg_indexes_size(c.oid)                 AS index_bytes
                         , pg_total_relation_size(reltoastrelid)  AS toast_bytes
                    FROM pg_class c
                             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE relkind = 'r' AND relname NOT LIKE 'pg_%' AND relname NOT LIKE 'sql_%';
            """

            table_info_result = session.execute(text(table_info_sql)).fetchall()

            table_info_rows = [list(r) for r in table_info_result]
            table_info = {
                "columns": ["table_name", "row_estimate", "total_bytes", "index_bytes", "toast_bytes"],
                "rows": table_info_rows,
            }

            # Task queue and Service queue status
            task_query = (
                session.query(TaskQueueORM.parser, TaskQueueORM.status, func.count(TaskQueueORM.id))
                .group_by(TaskQueueORM.parser, TaskQueueORM.status)
                .all()
            )
            task_stats = {"columns": ["result_type", "status", "count"], "rows": [list(r) for r in task_query]}

            service_query = (
                session.query(BaseResultORM.result_type, ServiceQueueORM.status, func.count(ServiceQueueORM.id))
                .join(BaseResultORM, BaseResultORM.id == ServiceQueueORM.procedure_id)
                .group_by(BaseResultORM.result_type, ServiceQueueORM.status)
                .all()
            )
            service_stats = {"columns": ["result_type", "status", "count"], "rows": [list(r) for r in service_query]}

        # Calculate combined table info
        table_size = 0
        index_size = 0
        for row in table_info_rows:
            table_size += row[2] - row[3] - (row[4] or 0)
            index_size += row[3]

        # Build out final data
        data = {
            "collection_count": table_counts[CollectionORM.__tablename__],
            "molecule_count": table_counts[MoleculeORM.__tablename__],
            "result_count": table_counts[BaseResultORM.__tablename__],
            "kvstore_count": table_counts[KVStoreORM.__tablename__],
            "access_count": table_counts[AccessLogORM.__tablename__],
            "error_count": table_counts[InternalErrorLogORM.__tablename__],
            "task_queue_status": task_stats,
            "service_queue_status": service_stats,
            "db_total_size": db_size,
            "db_table_size": table_size,
            "db_index_size": index_size,
            "db_table_information": table_info,
        }

        with self._core_socket.session_scope() as session:
            log = ServerStatsLogORM(**data)  # type: ignore
            session.add(log)
            session.commit()

    def query_stats(
        self, before: Optional[datetime] = None, after: Optional[datetime] = None, limit: int = None, skip: int = 0
    ) -> Tuple[QueryMetadata, List[ServerStatsDict]]:
        """
        General query of server statistics

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results from the total list of matches. The limit will apply after skipping,
            allowing for pagination.

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        limit = calculate_limit(self._server_log_limit, limit)

        query_cols_names, query_cols = get_query_proj_columns(ServerStatsLogORM)

        and_query = []
        if before:
            and_query.append(ServerStatsLogORM.timestamp <= before)
        if after:
            and_query.append(ServerStatsLogORM.timestamp >= after)

        with self._core_socket.session_scope(read_only=True) as session:
            query = session.query(*query_cols).filter(and_(*and_query)).order_by(ServerStatsLogORM.timestamp.desc())
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [dict(zip(query_cols_names, x)) for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def get_latest_stats(self) -> ServerStatsDict:
        """
        Obtain the latest statistics for the server

        If none are found, the server is updated and the new results returned

        Returns
        -------
        :
            A dictionary containing the latest server stats
        """

        query_cols_names, query_cols = get_query_proj_columns(ServerStatsLogORM)

        with self._core_socket.session_scope(read_only=True) as session:
            query = session.query(*query_cols).order_by(ServerStatsLogORM.timestamp.desc())
            result = query.limit(1).one_or_none()

            if result is not None:
                return dict(zip(query_cols_names, result))
            else:
                self.update_stats()
                return self.get_latest_stats()

    def query_access_logs(
        self,
        access_type: Optional[List[str]] = None,
        access_method: Optional[List[str]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: int = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[AccessLogDict]]:
        """
        General query of server access logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        access_type
            Type of access to query (typically related to the endpoint)
        access_method
            The method of access (GET, POST)
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results from the total list of matches. The limit will apply after skipping,
            allowing for pagination.

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        limit = calculate_limit(self._access_log_limit, limit)

        query_cols_names, query_cols = get_query_proj_columns(AccessLogORM)

        and_query = []
        if access_type:
            and_query.append(AccessLogORM.access_type.in_(access_type))
        if access_method:
            access_method = [x.upper() for x in access_method]
            and_query.append(AccessLogORM.access_method.in_(access_method))
        if before:
            and_query.append(AccessLogORM.access_date <= before)
        if after:
            and_query.append(AccessLogORM.access_date >= after)

        with self._core_socket.session_scope(read_only=True) as session:
            query = session.query(*query_cols).filter(and_(*and_query)).order_by(AccessLogORM.access_date.desc())
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [dict(zip(query_cols_names, x)) for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts

    def query_access_summary(
        self,
        group_by: str = "day",
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
    ) -> AccessLogSummaryDict:
        """
        General query of server access logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        group_by
            How to group the data. Valid options are "hour", "day", "country", "subdivision"
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        group_by = group_by.lower()

        and_query = []
        if before:
            and_query.append(AccessLogORM.access_date <= before)
        if after:
            and_query.append(AccessLogORM.access_date >= after)

        result_dict = defaultdict(list)
        with self._core_socket.session_scope(read_only=True) as session:
            if group_by == "user":
                group_col = AccessLogORM.user.label("group_col")
            elif group_by == "day":
                group_col = func.to_char(AccessLogORM.access_date, "YYYY-MM-DD").label("group_col")
            elif group_by == "hour":
                group_col = func.to_char(AccessLogORM.access_date, "YYYY-MM-DD HH24").label("group_col")
            elif group_by == "country":
                group_col = AccessLogORM.country.label("group_col")
            elif group_by == "subdivision":
                group_col = AccessLogORM.subdivision.label("group_col")
            else:
                raise RuntimeError(f"Unknown group_by: {group_by}")

            query = session.query(
                group_col,
                AccessLogORM.access_type,
                AccessLogORM.access_method,
                func.count(AccessLogORM.id),
                func.min(AccessLogORM.request_duration),
                func.percentile_disc(0.25).within_group(AccessLogORM.request_duration),
                func.percentile_disc(0.5).within_group(AccessLogORM.request_duration),
                func.percentile_disc(0.75).within_group(AccessLogORM.request_duration),
                func.percentile_disc(0.95).within_group(AccessLogORM.request_duration),
                func.max(AccessLogORM.request_duration),
                func.min(AccessLogORM.response_bytes),
                func.percentile_disc(0.25).within_group(AccessLogORM.response_bytes),
                func.percentile_disc(0.5).within_group(AccessLogORM.response_bytes),
                func.percentile_disc(0.75).within_group(AccessLogORM.response_bytes),
                func.percentile_disc(0.95).within_group(AccessLogORM.response_bytes),
                func.max(AccessLogORM.response_bytes),
            )
            query = query.filter(and_(*and_query)).group_by(
                AccessLogORM.access_type, AccessLogORM.access_method, "group_col"
            )

            results = query.all()

            # What comes out is a tuple in order of the specified columns
            # We group into a dictionary where the key is the date, and the value
            # is a dictionary with the rest of the information
            for row in results:
                d = {
                    "access_type": row[1],
                    "access_method": row[2],
                    "count": row[3],
                    "request_duration_info": row[4:10],
                    "response_bytes_info": row[10:16],
                }
                result_dict[row[0]].append(d)

        # replace None with "_none_"
        if None in result_dict:
            if "_none_" in result_dict:
                raise RuntimeError("Key _none_ already exists. Weird username or country?")
            result_dict["_none_"] = result_dict.pop(None)

        return dict(result_dict)

    def query_error_logs(
        self,
        id: Optional[List[int]] = None,
        user: Optional[List[str]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: int = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[AccessLogDict]]:
        """
        General query of server internal error logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        Parameters
        ----------
        id
            Query based on the error id
        user
            Query for errors from a given user
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        limit
            Limit the number of results. If None, the server limit will be used.
            This limit will not be respected if greater than the configured limit of the server.
        skip
            Skip this many results from the total list of matches. The limit will apply after skipping,
            allowing for pagination.

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        limit = calculate_limit(self._access_log_limit, limit)

        query_cols_names, query_cols = get_query_proj_columns(InternalErrorLogORM)

        and_query = []
        if id:
            and_query.append(InternalErrorLogORM.id.in_(id))
        if user:
            and_query.append(InternalErrorLogORM.user.in_(user))
        if before:
            and_query.append(InternalErrorLogORM.error_date <= before)
        if after:
            and_query.append(InternalErrorLogORM.error_date >= after)

        with self._core_socket.session_scope(read_only=True) as session:
            query = session.query(*query_cols).filter(and_(*and_query)).order_by(InternalErrorLogORM.error_date.desc())
            n_found = get_count(query)
            results = query.limit(limit).offset(skip).all()
            result_dicts = [dict(zip(query_cols_names, x)) for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))  # type: ignore
        return meta, result_dicts
