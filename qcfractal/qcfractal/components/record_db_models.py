from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Column, String, Integer, ForeignKey, Enum, DateTime, JSON, Index, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.nativefiles.db_models import NativeFileORM
from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.db_socket import BaseORM, MsgpackExt
from qcportal.compression import CompressionEnum
from qcportal.outputstore import OutputTypeEnum, OutputStore
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class RecordCommentORM(BaseORM):
    """
    Table for storing comments on calculations
    """

    __tablename__ = "record_comment"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    username = Column(String)  # not a foreign key - leaves username if user is deleted
    comment = Column(String, nullable=False)

    __table_args__ = (Index("ix_record_comment_record_id", "record_id"),)


class RecordInfoBackupORM(BaseORM):
    """
    Table for storing backup info about a record

    This stores previous tag, status, priority, etc, for a record. This is used when undoing
    delete, canceling, etc.
    """

    __tablename__ = "record_info_backup"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)
    old_status = Column(Enum(RecordStatusEnum), nullable=False)
    old_tag = Column(String, nullable=True)
    old_priority = Column(Integer, nullable=True)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    __table_args__ = (Index("ix_record_info_backup_record_id", "record_id"),)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "record_id")
        return BaseORM.model_dict(self, exclude)


class RecordComputeHistoryORM(BaseORM):
    """
    Table for storing the computation history of a record

    The computation history stores the result status, provenance, and manager info that
    ran a computation. This is useful for storing the history of records that have errored multiple
    times.
    """

    __tablename__ = "record_compute_history"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)

    status = Column(Enum(RecordStatusEnum), nullable=False)
    manager_name = Column(String, ForeignKey(ComputeManagerORM.name), nullable=True)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    provenance = Column(JSON)

    outputs = relationship(
        OutputStoreORM, collection_class=attribute_mapped_collection("output_type"), cascade="all, delete-orphan"
    )

    __table_args__ = (Index("ix_record_compute_history_record_id", "record_id"),)

    def upsert_output(self, session, new_output_orm: OutputStore) -> None:
        """
        Insert or replace an output in this history entry

        Given a new output orm, if it doesn't exist, add it. If an
        output of the same type already exists, then delete that one and
        insert the new one.
        """
        output_type = new_output_orm.output_type

        if output_type in self.outputs:
            old_orm = self.outputs.pop(output_type)
            session.delete(old_orm)
        session.flush()

        self.outputs[output_type] = new_output_orm

    def get_output(self, output_type: OutputTypeEnum) -> OutputStoreORM:
        """
        Get an output of a specific type

        If the output doesn't exist, then it is created.
        """

        if output_type in self.outputs:
            return self.outputs[output_type]

        new_output = OutputStore.compress(output_type, "", CompressionEnum.zstd)
        new_output_orm = OutputStoreORM.from_model(new_output)
        self.outputs[output_type] = new_output_orm
        return new_output_orm


class BaseRecordORM(BaseORM):
    """
    Base class for all the kinds of records
    """

    __tablename__ = "base_record"

    # for SQLAlchemy inheritence
    record_type = Column(String(100), nullable=False)

    # Some records can be either a service or a procedure
    is_service = Column(Boolean, nullable=False)

    # Base identification
    id = Column(Integer, primary_key=True)

    # Extra fields
    extras = Column(MsgpackExt)

    # Compute status
    # (Denormalized from compute history table for faster lookup during manager claiming/returning)
    status = Column(Enum(RecordStatusEnum), nullable=False)
    manager_name = Column(String, ForeignKey("compute_manager.name"), nullable=True)

    created_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    # Full compute history
    compute_history = relationship(
        RecordComputeHistoryORM, lazy="selectin", order_by=RecordComputeHistoryORM.modified_on.asc()
    )

    comments = relationship(RecordCommentORM, order_by=RecordCommentORM.timestamp.asc())

    # Related task. The foreign key is in the task_queue table
    task = relationship("TaskQueueORM", back_populates="record", uselist=False)

    # Related service. The foreign key is in the service_queue table
    service = relationship("ServiceQueueORM", back_populates="record", uselist=False)

    # Backed-up info (used for undelete, etc)
    info_backup = relationship(
        RecordInfoBackupORM,
        order_by=RecordInfoBackupORM.modified_on.asc(),
        cascade="all, delete-orphan",
    )

    # Native files returned from the computation
    native_files = relationship(
        NativeFileORM, collection_class=attribute_mapped_collection("name"), cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_base_record_status", "status"),
        Index("ix_base_record_record_type", "record_type"),
        Index("ix_base_record_manager_name", "manager_name"),
    )

    __mapper_args__ = {"polymorphic_on": "record_type"}

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        """
        Obtain a dictionary of required programs and versions
        """

        raise RuntimeError("Developer error - cannot create task for base record")
