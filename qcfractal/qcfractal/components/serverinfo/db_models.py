from __future__ import annotations

import datetime
from typing import Optional, Iterable, Dict, Any

from sqlalchemy import Column, Integer, DateTime, String, Float, BigInteger, JSON, Index, CHAR, ForeignKey
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.orm import relationship

from qcfractal.components.auth.db_models import UserORM, UserIDMapSubquery
from qcfractal.db_socket import BaseORM


class ServerStatsMetadataORM(BaseORM):
    __tablename__ = "server_stats_metadata"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    date_value = Column(DateTime, nullable=False)


class AccessLogORM(BaseORM):
    """
    Table for storing a log of accesses/requests
    """

    __tablename__ = "access_log"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    method = Column(String, nullable=False)
    module = Column(String, nullable=True)
    full_uri = Column(String, nullable=True)

    request_duration = Column(Float, nullable=False)
    request_bytes = Column(BigInteger, nullable=False)
    response_bytes = Column(BigInteger, nullable=False)

    user_id = Column(Integer, ForeignKey(UserORM.id), nullable=True)

    user = relationship(
        UserIDMapSubquery,
        foreign_keys=[user_id],
        primaryjoin="AccessLogORM.user_id == UserIDMapSubquery.id",
        lazy="selectin",
    )

    # user info
    ip_address = Column(INET)
    user_agent = Column(String)

    # extra computed geo data
    country_code = Column(CHAR(2))
    subdivision = Column(String)
    city = Column(String)
    ip_lat = Column(Float)
    ip_long = Column(Float)

    __table_args__ = (
        Index("ix_access_log_timestamp", "timestamp", postgresql_using="brin"),
        Index("ix_access_log_module", "module"),
        Index("ix_access_log_user_id", "user_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # strip user/group ids
        exclude = self.append_exclude(exclude, "user_id")

        d = BaseORM.model_dict(self, exclude)
        d["user"] = self.user.username if self.user is not None else None
        return d


class InternalErrorLogORM(BaseORM):
    """
    Table for storing internal errors

    Internal errors are usually not reported to the user, and are stored here instead
    for retrieval by an admin
    """

    __tablename__ = "internal_error_log"

    id = Column(Integer, primary_key=True)
    error_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    qcfractal_version = Column(String, nullable=False)
    error_text = Column(String)
    user_id = Column(Integer, ForeignKey(UserORM.id), nullable=True)

    user = relationship(
        UserIDMapSubquery,
        foreign_keys=[user_id],
        primaryjoin="InternalErrorLogORM.user_id == UserIDMapSubquery.id",
        lazy="selectin",
    )

    request_path = Column(String)
    request_headers = Column(String)
    request_body = Column(String)

    __table_args__ = (
        Index("ix_internal_error_log_error_date", "error_date", postgresql_using="brin"),
        Index("ix_internal_error_log_user_id", "user_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # strip user/group ids
        exclude = self.append_exclude(exclude, "user_id")

        d = BaseORM.model_dict(self, exclude)
        d["user"] = self.user.username if self.user is not None else None
        return d


class ServerStatsLogORM(BaseORM):
    """
    Table for storing server statistics

    Server statistics (storage size, row count, etc) are periodically captured and
    stored in this table
    """

    __tablename__ = "server_stats_log"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

    # Raw counts
    collection_count = Column(Integer)
    molecule_count = Column(BigInteger)
    record_count = Column(BigInteger)
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

    __table_args__ = (Index("ix_server_stats_log_timestamp", "timestamp", postgresql_using="brin"),)


class MessageOfTheDayORM(BaseORM):
    """
    Table for storing the Message-of-the-Day
    """

    __tablename__ = "motd"

    id = Column(Integer, primary_key=True)
    motd = Column(String, nullable=False)
