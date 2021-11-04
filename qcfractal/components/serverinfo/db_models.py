import datetime

from sqlalchemy import Column, Integer, DateTime, String, Float, BigInteger, JSON, Index, CHAR
from sqlalchemy.dialects.postgresql import INET

from qcfractal.db_socket import BaseORM


class AccessLogORM(BaseORM):
    __tablename__ = "access_log"

    id = Column(Integer, primary_key=True)
    access_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, index=True)
    access_method = Column(String, nullable=False)
    access_type = Column(String, nullable=False, index=True)

    request_duration = Column(Float)
    request_bytes = Column(BigInteger)
    response_bytes = Column(BigInteger)

    # Because logging happens every request, we store the user as a string
    # rather than a foreign key to the user table, which would require
    # a lookup. This also disconnects the access log from the user table,
    # allowing for logs to exist after a user is deleted
    user = Column(String)

    # Note: no performance difference between varchar and text in postgres
    # will mostly have a serialized JSON, but not stored as JSON for speed
    extra_params = Column(String)

    # user info
    ip_address = Column(INET)
    user_agent = Column(String)

    # extra computed geo data
    country_code = Column(CHAR(2))
    subdivision = Column(String)
    city = Column(String)
    ip_lat = Column(Float)
    ip_long = Column(Float)


class InternalErrorLogORM(BaseORM):
    __tablename__ = "internal_error_log"

    id = Column(Integer, primary_key=True)
    error_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    qcfractal_version = Column(String, nullable=False)
    error_text = Column(String)
    user = Column(String)

    request_path = Column(String)
    request_headers = Column(String)
    request_body = Column(String)

    __table_args__ = (Index("ix_internal_error_log_error_date", "error_date"),)


class ServerStatsLogORM(BaseORM):
    __tablename__ = "server_stats_log"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

    # Raw counts
    collection_count = Column(Integer)
    molecule_count = Column(BigInteger)
    result_count = Column(BigInteger)
    outputstore_count = Column(BigInteger)
    access_count = Column(BigInteger)
    error_count = Column(BigInteger)

    # Task & service queue status
    task_queue_status = Column(JSON)
    service_queue_status = Column(JSON)

    # Database
    db_total_size = Column(BigInteger)
    db_table_size = Column(BigInteger)
    db_index_size = Column(BigInteger)
    db_table_information = Column(JSON)

    __table_args__ = (Index("ix_server_stats_log_timestamp", "timestamp"),)


class VersionsORM(BaseORM):
    __tablename__ = "versions"

    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    elemental_version = Column(String, nullable=False)
    fractal_version = Column(String, nullable=False)
