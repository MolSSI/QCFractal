from __future__ import annotations

import datetime
from typing import Optional, Iterable, Dict, Any

from sqlalchemy import (
    Column,
    Integer,
    JSON,
    ForeignKey,
    String,
    DateTime,
    Index,
    UniqueConstraint,
    CheckConstraint,
    event,
    DDL,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.largebinary.db_models import LargeBinaryORM
from qcfractal.components.record_db_models import BaseRecordORM
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

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "service_id")
        return BaseORM.model_dict(self, exclude)


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

    dependencies = relationship(
        ServiceDependencyORM, lazy="selectin", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("record_id", name="ux_service_queue_record_id"),
        Index("ix_service_queue_tag", "tag"),
        Index("ix_service_queue_waiting_sort", priority.desc(), created_on),
        CheckConstraint("tag = LOWER(tag)", name="ck_service_queue_tag_lower"),
    )


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

    # Nullable to help with circular dependencies
    function_kwargs_lb_id = Column(Integer, ForeignKey(LargeBinaryORM.id), nullable=True)
    results_lb_id = Column(Integer, ForeignKey(LargeBinaryORM.id), nullable=True)

    __table_args__ = (
        Index("ix_service_subtask_record_function_kwargs_lb_id", "function_kwargs_lb_id"),
        Index("ix_service_subtask_record_results_lb_id", "results_lb_id"),
    )

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


# Trigger for deleting largebinary_store rows when rows of service_subtask_record are deleted
_del_lb_trigger = DDL(
    """
    CREATE FUNCTION qca_service_subtask_delete_lb() RETURNS TRIGGER AS
    $_$
    BEGIN
      DELETE FROM largebinary_store
      WHERE largebinary_store.id = OLD.function_kwargs_lb_id OR largebinary_store.id = OLD.results_lb_id;
      RETURN OLD;
    END
    $_$ LANGUAGE 'plpgsql';

    CREATE TRIGGER qca_service_subtask_delete_lb_tr
    AFTER DELETE ON service_subtask_record
    FOR EACH ROW EXECUTE PROCEDURE qca_service_subtask_delete_lb();
    """
)

event.listen(ServiceSubtaskRecordORM.__table__, "after_create", _del_lb_trigger.execute_if(dialect=("postgresql")))
