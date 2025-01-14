from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import tarfile
from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import requests
from sqlalchemy import and_, or_, func, select, delete
from sqlalchemy.orm import load_only

import qcfractal
from qcfractal.components.auth.db_models import UserIDMapSubquery
from qcfractal.db_socket.helpers import get_query_proj_options
from qcportal.serverinfo import (
    AccessLogQueryFilters,
    AccessLogSummaryFilters,
    ErrorLogQueryFilters,
)
from qcportal.utils import now_at_utc
from .db_models import AccessLogORM, InternalErrorLogORM, MessageOfTheDayORM, ServerStatsMetadataORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Dict, Any, List, Optional

# GeoIP2 package is optional
try:
    import geoip2.database

    geoip2_found = True
except ImportError:
    geoip2_found = False


class ServerInfoSocket:
    """
    Socket for managing/querying server logs and information
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self._geoip2_dir = root_socket.qcf_config.geoip2_dir
        self._geoip2_file_path = os.path.join(self._geoip2_dir, root_socket.qcf_config.geoip2_filename)
        self._maxmind_license_key = root_socket.qcf_config.maxmind_license_key

        self._geolocate_accesses_frequency = 120  # two minutes should be ok?
        self._update_geoip2_frequency = 60 * 60 * 24  # one day

        # Set up access logging
        self._access_log_enabled = root_socket.qcf_config.log_access
        self._delete_access_log_frequency = 60 * 60 * 24  # one day
        self._access_log_keep = root_socket.qcf_config.access_log_keep
        self._geoip2_enabled = geoip2_found and self._access_log_enabled

        # MOTD contents
        self._load_motd()

        if not os.path.exists(self._geoip2_dir):
            os.makedirs(self._geoip2_dir)

        if self._access_log_enabled:
            if not geoip2_found:
                self._logger.info(
                    "GeoIP2 package not found. To include locations in access logs, install the geoip2 package"
                )

        ################################
        # Add various internal jobs
        ################################
        with self.root_socket.session_scope() as session:
            # updating geoip2 file
            # Only add this if we have the maxmind license key
            if self._geoip2_enabled and self._maxmind_license_key:
                with self.root_socket.optional_session(session) as session:
                    self.root_socket.internal_jobs.add(
                        "update_geoip2_file",
                        now_at_utc(),
                        "serverinfo.update_geoip2_file",
                        {},
                        user_id=None,
                        unique_name=True,
                        repeat_delay=self._update_geoip2_frequency,
                        session=session,
                    )

            # Doing processing of access log
            if self._geoip2_enabled:
                self.root_socket.internal_jobs.add(
                    "geolocate_accesses",
                    now_at_utc(),
                    "serverinfo.geolocate_accesses",
                    {},
                    user_id=None,
                    unique_name=True,
                    repeat_delay=self._geolocate_accesses_frequency,
                    session=session,
                )

            # Deleting old access logs
            if self._access_log_keep > 0 and self._access_log_enabled:
                self.root_socket.internal_jobs.add(
                    "delete_old_access_log",
                    now_at_utc() + timedelta(seconds=2.0),
                    "serverinfo.delete_old_access_logs",
                    {},
                    user_id=None,
                    unique_name=True,
                    repeat_delay=self._delete_access_log_frequency,
                    session=session,
                )

    def update_geoip2_file(self) -> None:
        # Possible to reach this if we changed the settings, but have a job still in the queue
        if not (self._geoip2_enabled and self._maxmind_license_key):
            return

        # Session is not needed, but must be consistent with other jobs
        maxmind_license_key = self._maxmind_license_key

        if not maxmind_license_key:
            self._logger.warning("No maxmind license key provided. Cannot update geoip2 database")
            return

        base_url = f"https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key={maxmind_license_key}"
        db_url = base_url + "&suffix=tar.gz"  # Actual DB file
        sha256_url = base_url + "&suffix=tar.gz.sha256"  # File with the sha256 hash

        self._logger.info("Checking if update of geoip2 database is needed")

        # According to the docs, we can use a head request and look at the last-modified header
        r = requests.head(db_url)
        remote_last_modified = r.headers.get("content-disposition")
        m = re.match(r".*GeoLite2-City_(\d{8}).tar.gz.*", remote_last_modified)

        if not m:
            raise RuntimeError(
                "Could not get sane filename from maxmind server. Account issue? Content-disposition: "
                + remote_last_modified
            )

        remote_last_modified = m.group(1)

        date_path = os.path.join(self._geoip2_dir, "last_modified.txt")

        # What version do we have already?
        if os.path.exists(date_path):
            with open(date_path, "r") as f:
                local_last_modified = f.read()
        else:
            local_last_modified = "00000000"

        self._logger.debug(f"Maxmind GeoIP2 last modified: {remote_last_modified}")
        self._logger.debug(f"Local GeoIP2 version: {local_last_modified}")

        if remote_last_modified <= local_last_modified:
            self._logger.info("Update of GeoIP2 database not needed")
            return

        else:
            self._logger.info("Update of GeoIP2 database required")

        geoip_file_data = requests.get(db_url)
        geoip_file_sha256 = requests.get(sha256_url).text

        local_hash = hashlib.sha256(geoip_file_data.content).hexdigest()

        # Is like a file: [hash] [filename]
        # We only want the hash
        expected_hash = geoip_file_sha256.split()[0]
        if local_hash != expected_hash:
            self._logger.warning(f"Hashes for geoip2 data do not match. Expected {expected_hash}, got {local_hash}")

        fileobj = io.BytesIO(geoip_file_data.content)

        # Extract contents, but remove the subdirectory
        with tarfile.open(fileobj=fileobj, mode="r:gz") as tar:
            db_date = None
            to_extract = []
            for ti in tar.getmembers():
                if ti.isfile():
                    ti.name = os.path.basename(ti.name)  # Remove the directory
                    to_extract.append(ti)
                if ti.isdir():
                    db_date = ti.name.split("_")[1]

            tar.extractall(path=self._geoip2_dir, members=to_extract)

            # Write the date of the subdir into the last_modified file
            with open(os.path.join(self._geoip2_dir, "last_modified.txt"), "w") as f:
                f.write(db_date)

        self._logger.info(f"Geoip database (date {db_date}) downloaded and extracted to {self._geoip2_dir}")

    def geolocate_accesses(self, session: Session) -> None:
        """
        Finds and updates accesses which haven't been processed for geolocation data
        """

        # Possible to reach this if we changed the settings, but have a job still in the queue
        if not self._geoip2_enabled:
            return

        if not os.path.exists(self._geoip2_file_path):
            self._logger.warning(
                "GeoIP2 database file not found. Cannot add location data to accesses. "
                "May need to wait for the updater job to run"
            )
            return

        stmt = select(ServerStatsMetadataORM).where(ServerStatsMetadataORM.name == "last_geolocated_date")
        last_geolocated_date = session.execute(stmt).scalar_one_or_none()

        access_stmt = select(AccessLogORM)
        access_stmt = access_stmt.options(load_only(AccessLogORM.timestamp, AccessLogORM.ip_address))
        access_stmt = access_stmt.where(AccessLogORM.ip_address.is_not(None))

        if last_geolocated_date:
            access_stmt = access_stmt.where(AccessLogORM.timestamp > last_geolocated_date.date_value)

        access_stmt = access_stmt.order_by(AccessLogORM.timestamp.asc())

        to_process = session.execute(access_stmt).scalars().all()
        self._logger.info(f"Found {len(to_process)} accesses to process")

        if not to_process:
            return

        distinct_ip = set(x.ip_address for x in to_process)
        self._logger.info(f"Found {len(distinct_ip)} distinct ip addresses to process")

        geo_reader = geoip2.database.Reader(self._geoip2_file_path)

        def _lookup_ip(ip_address: str) -> Dict[str, Any]:
            out: Dict[str, Any] = {}
            try:
                loc_data = geo_reader.city(ip_address)
                out["country_code"] = loc_data.country.iso_code
                out["subdivision"] = loc_data.subdivisions.most_specific.name
                out["city"] = loc_data.city.name
                out["ip_lat"] = loc_data.location.latitude
                out["ip_long"] = loc_data.location.longitude
            except:
                pass

            return out

        geo_data = {ip: _lookup_ip(ip) for ip in distinct_ip}

        for access in to_process:
            geo = geo_data.get(access.ip_address, {})
            access.country_code = geo.get("country_code")
            access.subdivision = geo.get("subdivision")
            access.city = geo.get("city")
            access.ip_lat = geo.get("ip_lat")
            access.ip_long = geo.get("ip_long")

        # Update the last geolocated date
        if last_geolocated_date is None:
            last_geolocated_date = ServerStatsMetadataORM(name="last_geolocated_date")
            session.add(last_geolocated_date)

        last_geolocated_date.date_value = to_process[-1].timestamp

        session.commit()

    def delete_old_access_logs(self, session: Session) -> None:
        """
        Deletes old access logs (as defined by the configuration)
        """

        # we check when adding the job, but double check here
        if self._access_log_keep <= 0 or not self._access_log_enabled:
            return

        before = now_at_utc() - timedelta(seconds=self._access_log_keep)
        num_deleted = self.delete_access_logs(before, session=session)
        self._logger.info(f"Deleted {num_deleted} access logs before {before}")

    def _load_motd(self, *, session: Optional[Session] = None):
        stmt = select(MessageOfTheDayORM).order_by(MessageOfTheDayORM.id)
        with self.root_socket.optional_session(session, True) as session:
            motd_orm = session.execute(stmt).scalar_one_or_none()
            if motd_orm is None:
                self._motd = ""
            else:
                self._motd = motd_orm.motd

        self._motd_time = now_at_utc()

    def set_motd(self, new_motd: str, *, session: Optional[Session] = None):
        stmt = select(MessageOfTheDayORM).order_by(MessageOfTheDayORM.id)
        with self.root_socket.optional_session(session) as session:
            motd_orm = session.execute(stmt).scalar_one_or_none()
            if motd_orm is None:
                motd_orm = MessageOfTheDayORM(motd=new_motd)
                session.add(motd_orm)
            else:
                motd_orm.motd = new_motd

        self._motd = new_motd
        self._motd_time = now_at_utc()

    def get_motd(self, *, session: Optional[Session] = None):
        # If file is updated, reload it
        # Only load every 10 seconds though
        now = now_at_utc()
        checktime = self._motd_time + timedelta(seconds=10)

        if now > checktime:
            self._load_motd(session=session)

        return self._motd

    def save_access(self, log_data: Dict[str, Any], *, session: Optional[Session] = None) -> None:
        """
        Saves information about a request/access to the database

        Parameters
        ----------
        log_data
            Dictionary of data to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            log = AccessLogORM(**log_data)
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
            is used, it will be flushed (but not committed) before returning from this function.

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

    def query_access_log(
        self,
        query_data: AccessLogQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        General query of server access logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        The entries will be returned in chronological order, with the most
        recent being first.

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
            A list of access log dictionaries
        """

        proj_options = get_query_proj_options(AccessLogORM, query_data.include, query_data.exclude)

        stmt = select(AccessLogORM)

        and_query = []
        if query_data.module:
            and_query.append(AccessLogORM.module.in_(query_data.module))
        if query_data.method:
            method = [x.upper() for x in query_data.method]
            and_query.append(AccessLogORM.method.in_(method))
        if query_data.before:
            and_query.append(AccessLogORM.timestamp <= query_data.before)
        if query_data.after:
            and_query.append(AccessLogORM.timestamp >= query_data.after)

        if query_data.user:
            stmt = stmt.join(UserIDMapSubquery)

            int_ids = {x for x in query_data.user if isinstance(x, int) or x.isnumeric()}
            str_names = set(query_data.user) - int_ids

            and_query.append(or_(UserIDMapSubquery.username.in_(str_names), UserIDMapSubquery.id.in_(int_ids)))

        with self.root_socket.optional_session(session, True) as session:
            stmt = stmt.where(and_(True, *and_query))
            stmt = stmt.options(*proj_options)

            if query_data.cursor is not None:
                stmt = stmt.where(AccessLogORM.id < query_data.cursor)

            stmt = stmt.order_by(AccessLogORM.id.desc())
            stmt = stmt.limit(query_data.limit)
            stmt = stmt.distinct(AccessLogORM.id)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in sorted(results, key=lambda x: x.timestamp, reverse=True)]

        return result_dicts

    def query_access_summary(
        self,
        query_data: AccessLogSummaryFilters,
        *,
        session: Optional[Session] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        General query of server access logs, returning aggregate data

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

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
            A dictionary containing summary data
        """

        and_query = []
        if query_data.before:
            and_query.append(AccessLogORM.timestamp <= query_data.before)
        if query_data.after:
            and_query.append(AccessLogORM.timestamp >= query_data.after)

        result_dict = defaultdict(list)
        with self.root_socket.optional_session(session, True) as session:
            if query_data.group_by == "user":
                group_col = UserIDMapSubquery.username.label("group_col")
            elif query_data.group_by == "day":
                group_col = func.to_char(AccessLogORM.timestamp.op("AT TIME ZONE")("UTC"), "YYYY-MM-DD").label(
                    "group_col"
                )
            elif query_data.group_by == "hour":
                group_col = func.to_char(AccessLogORM.timestamp.op("AT TIME ZONE")("UTC"), "YYYY-MM-DD HH24").label(
                    "group_col"
                )
            elif query_data.group_by == "country":
                group_col = AccessLogORM.country_code.label("group_col")
            elif query_data.group_by == "subdivision":
                group_col = AccessLogORM.subdivision.label("group_col")
            else:
                raise RuntimeError(f"Unknown group_by: {query_data.group_by}")

            stmt = select(
                group_col,
                AccessLogORM.module,
                AccessLogORM.method,
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

            stmt = stmt.where(and_(True, *and_query)).group_by(AccessLogORM.module, AccessLogORM.method, "group_col")

            if query_data.group_by == "user":
                stmt = stmt.join(UserIDMapSubquery)

            results = session.execute(stmt).all()

            # What comes out is a tuple in order of the specified columns
            # We group into a dictionary where the key is the date, and the value
            # is a dictionary with the rest of the information
            for row in results:
                d = {
                    "module": row[1],
                    "method": row[2],
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
        query_data: ErrorLogQueryFilters,
        *,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        General query of server internal error logs

        All search criteria are merged via 'and'. Therefore, records will only
        be found that match all the criteria.

        The entries will be returned in chronological order, with the most
        recent being first.

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
            A list of errors (as dictionaries) that were found in the database.
        """

        and_query = []
        stmt = select(InternalErrorLogORM)

        if query_data.error_id:
            and_query.append(InternalErrorLogORM.id.in_(query_data.error_id))
        if query_data.before:
            and_query.append(InternalErrorLogORM.error_date <= query_data.before)
        if query_data.after:
            and_query.append(InternalErrorLogORM.error_date >= query_data.after)
        if query_data.user:
            stmt = stmt.join(UserIDMapSubquery)

            int_ids = {x for x in query_data.user if isinstance(x, int) or x.isnumeric()}
            str_names = set(query_data.user) - int_ids

            and_query.append(or_(UserIDMapSubquery.username.in_(str_names), UserIDMapSubquery.id.in_(int_ids)))

        with self.root_socket.optional_session(session, True) as session:
            stmt = stmt.where(and_(True, *and_query))

            if query_data.cursor is not None:
                stmt = stmt.where(InternalErrorLogORM.id < query_data.cursor)

            stmt = stmt.order_by(InternalErrorLogORM.id.desc())
            stmt = stmt.limit(query_data.limit)
            stmt = stmt.distinct(InternalErrorLogORM.id)
            results = session.execute(stmt).scalars().all()
            result_dicts = [x.model_dict() for x in sorted(results, key=lambda x: x.error_date, reverse=True)]

        return result_dicts

    def delete_access_logs(self, before: datetime, *, session: Optional[Session] = None) -> int:
        """
        Deletes access logs that were created before a certain date & time

        Parameters
        ----------
        before
            Delete access logs before this time
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
            The number of deleted entries
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = delete(AccessLogORM).where(AccessLogORM.timestamp < before)
            r = session.execute(stmt)
            return r.rowcount

    def delete_error_logs(self, before: datetime, *, session: Optional[Session] = None) -> int:
        """
        Deletes error entries that were created before a certain date & time

        Parameters
        ----------
        before
            Delete error entries before this time
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
            The number of deleted entries
        """

        with self.root_socket.optional_session(session, False) as session:
            stmt = delete(InternalErrorLogORM).where(InternalErrorLogORM.error_date < before)
            r = session.execute(stmt)
            return r.rowcount
