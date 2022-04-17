from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, text, select, delete

import qcfractal
from qcfractal.components.datasets.db_models import CollectionORM
from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.services.db_models import ServiceQueueORM
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.db_socket.helpers import get_query_proj_options, get_count
from qcportal.metadata_models import QueryMetadata
from .db_models import AccessLogORM, InternalErrorLogORM, ServerStatsLogORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Dict, Any, List, Optional, Tuple, Iterable


class ServerInfoSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        # Set up access logging
        self._access_log_enabled = root_socket.qcf_config.log_access

        if self._access_log_enabled:
            geo_file_path = root_socket.qcf_config.geo_file_path
            self._geoip2_reader = None

            if geo_file_path:
                try:
                    import geoip2.database

                    self._geoip2_reader = geoip2.database.Reader(geo_file_path)
                    self._logger.info(f"Successfully initialized geoip2 with {geo_file_path}.")

                except ImportError:
                    self._logger.warning(
                        f"Cannot import geoip2 module. To use API access logging, you need "
                        f"to install it manually using `pip install geoip2`"
                    )
                except FileNotFoundError:
                    self._logger.warning(
                        f"GeoIP cities file cannot be read from {geo_file_path}.\n"
                        f"Make sure to manually download the file from: \n"
                        f"https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz\n"
                        f"Then, set the geo_file_path in qcfractal_config.yaml in your base_folder."
                    )

    def _get_geoip2_data(self, ip_address: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        if not self._geoip2_reader:
            return out

        try:
            loc_data = self._geoip2_reader.city(ip_address)
            out["country_code"] = loc_data.country.iso_code
            out["subdivision"] = loc_data.subdivisions.most_specific.name
            out["city"] = loc_data.city.name
            out["ip_lat"] = loc_data.location.latitude
            out["ip_long"] = loc_data.location.longitude
        except:
            pass

        return out

    def save_access(self, log_data: Dict[str, Any], *, session: Optional[Session] = None) -> None:
        """
        Saves information about an access to the database

        Parameters
        ----------
        log_data
            Dictionary of data to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        if self._access_log_enabled is not True:
            return 0

        # Obtain all the information we can from the GeoIP database
        ip_data = self._get_geoip2_data(log_data["ip_address"])

        with self.root_socket.optional_session(session) as session:
            log = AccessLogORM(**log_data, **ip_data)
            session.add(log)

    def save_error(self, error_data: Dict[str, Any], *, session: Optional[Session] = None) -> int:
        """
        Saves information about an internal error to the database

        Parameters
        ----------
        error_data
            Dictionary of error data to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            The id of the newly-created error
        """

        log = InternalErrorLogORM(**error_data, qcfractal_version=qcfractal.__version__)
        with self.root_socket.optional_session(session) as session:
            session.add(log)
            session.flush()
            return log.id

    def update_server_stats(self, *, session: Optional[Session] = None):
        """
        Obtains some statistics about the server and stores them in the database

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
        """

        table_list = [CollectionORM, MoleculeORM, BaseRecordORM, OutputStoreORM, AccessLogORM, InternalErrorLogORM]
        db_name = self.root_socket.qcf_config.database.database_name

        table_counts = {}
        with self.root_socket.optional_session(session) as session:
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
                session.query(BaseRecordORM.record_type, BaseRecordORM.status, func.count(TaskQueueORM.id))
                .join(BaseRecordORM, BaseRecordORM.id == TaskQueueORM.record_id)
                .group_by(BaseRecordORM.record_type, BaseRecordORM.status)
                .all()
            )
            task_stats = {"columns": ["record_type", "status", "count"], "rows": [list(r) for r in task_query]}

            service_query = (
                session.query(BaseRecordORM.record_type, BaseRecordORM.status, func.count(ServiceQueueORM.id))
                .join(BaseRecordORM, BaseRecordORM.id == ServiceQueueORM.record_id)
                .group_by(BaseRecordORM.record_type, BaseRecordORM.status)
                .all()
            )
            service_stats = {"columns": ["record_type", "status", "count"], "rows": [list(r) for r in service_query]}

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
                "record_count": table_counts[BaseRecordORM.__tablename__],
                "outputstore_count": table_counts[OutputStoreORM.__tablename__],
                "access_count": table_counts[AccessLogORM.__tablename__],
                "error_count": table_counts[InternalErrorLogORM.__tablename__],
                "task_queue_status": task_stats,
                "service_queue_status": service_stats,
                "db_total_size": db_size,
                "db_table_size": table_size,
                "db_index_size": index_size,
                "db_table_information": table_info,
            }

            log = ServerStatsLogORM(**data)
            session.add(log)

    def query_access_log(
        self,
        access_type: Optional[Iterable[str]] = None,
        access_method: Optional[Iterable[str]] = None,
        username: Optional[Iterable[str]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        limit: int = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
        """
        General query of server access logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        The entries will be returned in chronological order, with the most
        recent being first.

        Parameters
        ----------
        access_type
            Type of access to query (typically related to the endpoint)
        access_method
            The method of access (GET, POST)
        username
            The name of the user that made the request
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        include
            Which fields of the access log return. Default is to return all fields.
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
            Metadata about the results of the query, and a list of access log dictionaries
        """

        proj_options = get_query_proj_options(AccessLogORM, include, exclude)

        and_query = []
        if access_type:
            and_query.append(AccessLogORM.access_type.in_(access_type))
        if access_method:
            access_method = [x.upper() for x in access_method]
            and_query.append(AccessLogORM.access_method.in_(access_method))
        if username:
            and_query.append(AccessLogORM.user.in_(username))
        if before:
            and_query.append(AccessLogORM.access_date <= before)
        if after:
            and_query.append(AccessLogORM.access_date >= after)

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(AccessLogORM).where(and_(*and_query)).order_by(AccessLogORM.access_date.desc())
            stmt = stmt.options(*proj_options)
            n_found = get_count(session, stmt)
            stmt = stmt.limit(limit).offset(skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))
        return meta, result_dicts

    def query_access_summary(
        self,
        group_by: str = "day",
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """
        General query of server access logs, returning aggregate data

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
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Metadata about the results of the query, and a list of dictionaries containing summary data
        """

        group_by = group_by.lower()

        and_query = []
        if before:
            and_query.append(AccessLogORM.access_date < before)
        if after:
            and_query.append(AccessLogORM.access_date > after)

        result_dict = defaultdict(list)
        with self.root_socket.optional_session(session, True) as session:
            if group_by == "user":
                group_col = AccessLogORM.user.label("group_col")
            elif group_by == "day":
                group_col = func.to_char(AccessLogORM.access_date, "YYYY-MM-DD").label("group_col")
            elif group_by == "hour":
                group_col = func.to_char(AccessLogORM.access_date, "YYYY-MM-DD HH24").label("group_col")
            elif group_by == "country":
                group_col = AccessLogORM.country_code.label("group_col")
            elif group_by == "subdivision":
                group_col = AccessLogORM.subdivision.label("group_col")
            else:
                raise RuntimeError(f"Unknown group_by: {group_by}")

            stmt = select(
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

            stmt = stmt.where(and_(*and_query)).group_by(
                AccessLogORM.access_type, AccessLogORM.access_method, "group_col"
            )

            results = session.execute(stmt).all()

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

    def query_error_log(
        self,
        error_id: Optional[Iterable[int]] = None,
        username: Optional[Iterable[str]] = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: int = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
        """
        General query of server internal error logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        The entries will be returned in chronological order, with the most
        recent being first.

        Parameters
        ----------
        error_id
            Query based on the error id
        username
            Query for errors from a given user name
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
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Metadata about the results of the query, and a list of error dictionaries that were found in the database.
        """

        and_query = []
        if error_id:
            and_query.append(InternalErrorLogORM.id.in_(error_id))
        if username:
            and_query.append(InternalErrorLogORM.user.in_(username))
        if before:
            and_query.append(InternalErrorLogORM.error_date < before)
        if after:
            and_query.append(InternalErrorLogORM.error_date > after)

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(InternalErrorLogORM).where(and_(*and_query)).order_by(InternalErrorLogORM.error_date.desc())
            n_found = get_count(session, stmt)
            stmt = stmt.limit(limit).offset(skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))
        return meta, result_dicts

    def query_server_stats(
        self,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: int = None,
        skip: int = 0,
        *,
        session: Optional[Session] = None,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
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
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Metadata about the results of the query, and a list of Molecule that were found in the database.
        """

        and_query = []
        if before:
            and_query.append(ServerStatsLogORM.timestamp < before)
        if after:
            and_query.append(ServerStatsLogORM.timestamp > after)

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(ServerStatsLogORM).filter(and_(*and_query)).order_by(ServerStatsLogORM.timestamp.desc())
            n_found = get_count(session, stmt)
            stmt = stmt.limit(limit).offset(skip)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in results]

        meta = QueryMetadata(n_found=n_found, n_returned=len(result_dicts))
        return meta, result_dicts

    def delete_access_logs(self, before: datetime, *, session: Optional[Session] = None) -> int:
        """
        Deletes access logs that were created before a certain date & time

        Parameters
        ----------
        before
            Delete access logs before this time

        Returns
        -------
            The number of deleted entries
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = delete(AccessLogORM).where(AccessLogORM.access_date < before)
            r = session.execute(stmt)
            return r.rowcount

    def delete_error_logs(self, before: datetime, *, session: Optional[Session] = None) -> int:
        """
        Deletes error entries that were created before a certain date & time

        Parameters
        ----------
        before
            Delete error entries before this time

        Returns
        -------
            The number of deleted entries
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = delete(InternalErrorLogORM).where(InternalErrorLogORM.error_date < before)
            r = session.execute(stmt)
            return r.rowcount

    def delete_server_stats(self, before: datetime, *, session: Optional[Session] = None) -> int:
        """
        Deletes server statistics that were created before a certain date & time

        Parameters
        ----------
        before
            Delete server stats before this time

        Returns
        -------
            The number of deleted entries
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = delete(ServerStatsLogORM).where(ServerStatsLogORM.timestamp < before)
            r = session.execute(stmt)
            return r.rowcount
