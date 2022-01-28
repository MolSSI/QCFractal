import datetime
from typing import Dict, Optional

from sqlalchemy import Column, String, Integer, ForeignKey, Enum, DateTime, JSON, Index, Boolean
from sqlalchemy.orm import relationship

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.db_socket import BaseORM, MsgpackExt
from qcportal.outputstore import OutputTypeEnum, OutputStore, CompressionEnum
from qcportal.records import RecordStatusEnum


class RecordCommentsORM(BaseORM):
    __tablename__ = "record_comments"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="CASCADE"), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    username = Column(String)  # not a foreign key - leaves username if user is deleted
    comment = Column(String, nullable=False)


class RecordComputeHistoryORM(BaseORM):
    __tablename__ = "record_compute_history"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="CASCADE"), nullable=False)

    status = Column(Enum(RecordStatusEnum), nullable=False)
    manager_name = Column(String, ForeignKey(ComputeManagerORM.name), nullable=True)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    provenance = Column(JSON)

    outputs = relationship(OutputStoreORM, lazy="select")

    def get_output(self, output_type: OutputTypeEnum) -> OutputStoreORM:
        for o in self.outputs:
            if o.output_type == output_type:
                return o

        new_output = OutputStore.compress(output_type, "", CompressionEnum.lzma, 1)
        new_output_orm = OutputStoreORM.from_model(new_output)
        self.outputs.append(new_output_orm)
        return new_output_orm

    __table_args__ = (
        Index("ix_record_compute_history_record_id", "record_id"),
        Index("ix_record_compute_history_manager_name", "manager_name"),
    )


class BaseRecordORM(BaseORM):
    """
    Base class for all kinds of records
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
        RecordComputeHistoryORM,
        foreign_keys=[RecordComputeHistoryORM.record_id],
        order_by=RecordComputeHistoryORM.modified_on.asc(),
        lazy="selectin",
    )

    comments = relationship(RecordCommentsORM, order_by=RecordCommentsORM.timestamp.asc())

    # Related task. The foreign key is in the task_queue table
    task = relationship("TaskQueueORM", back_populates="record", uselist=False)

    # Related service. The foreign key is in the service_queue table
    service = relationship("ServiceQueueORM", back_populates="record", uselist=False)

    # Backed-up info (used for undelete, etc)
    info_backup = relationship(
        "RecordInfoBackupORM",
        uselist=True,
        order_by="RecordInfoBackupORM.modified_on.asc()",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_base_record_status", "status"),
        Index("ix_base_record_record_type", "record_type"),
        Index("ix_base_record_manager_name", "manager_name"),
    )

    __mapper_args__ = {"polymorphic_on": "record_type"}

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        raise RuntimeError("Developer error - cannot create task for base record")


class RecordInfoBackupORM(BaseORM):
    __tablename__ = "record_info_backup"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="CASCADE"), nullable=False)
    old_status = Column(Enum(RecordStatusEnum), nullable=False)
    old_tag = Column(String, nullable=True)
    old_priority = Column(Integer, nullable=True)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
