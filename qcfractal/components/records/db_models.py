import datetime

from sqlalchemy import Column, String, Integer, ForeignKey, Enum, DateTime, JSON, Index, Boolean
from sqlalchemy.orm import relationship

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.db_socket import BaseORM, MsgpackExt
from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.portal.records import RecordStatusEnum
from qcfractal.portal.outputstore import OutputTypeEnum, OutputStore, CompressionEnum


class RecordComputeHistoryORM(BaseORM):
    __tablename__ = "record_compute_history"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="CASCADE"), nullable=False)

    status = Column(Enum(RecordStatusEnum), nullable=False)
    manager_name = Column(String, ForeignKey(ComputeManagerORM.name), nullable=True)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    provenance = Column(JSON)

    outputs = relationship(OutputStoreORM, lazy="select")

    def get_output(self, output_type: OutputTypeEnum, create: bool = True):
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

    # Related task. The foreign key is in the task_queue table
    task = relationship("TaskQueueORM", back_populates="record", uselist=False)

    # Related service. The foreign key is in the service_queue table
    service = relationship("ServiceQueueORM", back_populates="record", uselist=False)

    __table_args__ = (
        Index("ix_base_record_status", "status"),
        Index("ix_base_record_record_type", "record_type"),
        Index("ix_base_record_manager_name", "manager_name"),
    )

    __mapper_args__ = {"polymorphic_on": "record_type"}


class RecordDeletionInfoORM(BaseORM):
    __tablename__ = "record_deletion_info"

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="CASCADE"), primary_key=True)
    old_status = Column(Enum(RecordStatusEnum), nullable=False)
    deleted_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
