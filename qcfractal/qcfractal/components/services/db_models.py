import datetime

from sqlalchemy import Column, Integer, ForeignKey, String, DateTime, Index, UniqueConstraint, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM, PlainMsgpackExt


class ServiceDependencyORM(BaseORM):
    """
    Table for storing dependencies of a service

    These are other records that are required to be completed before
    a service will iterate.
    """

    __tablename__ = "service_dependency"

    id = Column(Integer, primary_key=True)

    service_id = Column(Integer, ForeignKey("service_queue.id", ondelete="cascade"), nullable=False)
    record_id = Column(Integer, ForeignKey(BaseRecordORM.id), nullable=False)
    extras = Column(JSONB, nullable=False)

    record = relationship(BaseRecordORM)

    # We make extras part of the unique constraint because rarely the same dependency will be
    # submitted but with different extras (position, etc)
    __table_args__ = (UniqueConstraint("service_id", "record_id", "extras", name="ux_service_dependency"),)


class ServiceQueueORM(BaseORM):
    """
    Table for storing service information
    """

    __tablename__ = "service_queue"

    id = Column(Integer, primary_key=True)

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), nullable=False)
    record = relationship(BaseRecordORM, back_populates="service", uselist=False)

    tag = Column(String, nullable=False)
    priority = Column(Integer, nullable=False)
    created_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    service_state = Column(PlainMsgpackExt)

    dependencies = relationship(ServiceDependencyORM, cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("record_id", name="ux_service_queue_record_id"),
        Index("ix_service_queue_tag", "tag"),
        Index("ix_service_queue_waiting_sort", priority.desc(), created_on),
        CheckConstraint("tag = LOWER(tag)", name="ck_service_queue_tag_lower"),
    )
