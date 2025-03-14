from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    JSON,
    ForeignKey,
    String,
    Boolean,
    Index,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM, PlainMsgpackExt

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


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

    _qcportal_model_excludes = ["id", "service_id"]


class ServiceQueueORM(BaseORM):
    """
    Table for storing service information
    """

    __tablename__ = "service_queue"

    id = Column(Integer, primary_key=True)

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), nullable=False)
    record = relationship(BaseRecordORM, back_populates="service", uselist=False)

    compute_tag = Column(String, nullable=False)
    compute_priority = Column(Integer, nullable=False)
    find_existing = Column(Boolean, nullable=False)

    service_state = Column(PlainMsgpackExt)

    dependencies = relationship(
        ServiceDependencyORM, lazy="selectin", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("record_id", name="ux_service_queue_record_id"),
        Index("ix_service_queue_compute_tag", "compute_tag"),
        CheckConstraint("compute_tag = LOWER(compute_tag)", name="ck_service_queue_compute_tag_lower"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        # TODO - DEPRECATED - remove
        d["tag"] = d.pop("compute_tag")
        d["priority"] = d.pop("compute_priority")

        return d


class ServiceSubtaskRecordORM(BaseRecordORM):
    """
    Table for storing records associated with service iterations
    """

    __tablename__ = "service_subtask_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    # In other records, required_programs is a property. But here, it's basically
    # specified by whoever creates the record, so it's a column
    required_programs = Column(JSON, nullable=False)

    function = Column(String, nullable=False)

    function_kwargs = Column(JSON, nullable=False)
    results = Column(JSON, nullable=True)

    # We need the inherit_condition because we have two foreign keys to the BaseRecordORM and
    # we need to disambiguate
    __mapper_args__ = {
        "polymorphic_identity": "servicesubtask",
        "inherit_condition": (id == BaseRecordORM.id),
    }

    @property
    def short_description(self) -> str:
        progs = ",".join(self.required_programs.keys())
        return f"{self.function} [{progs}]"
