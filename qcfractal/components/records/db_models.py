import datetime

from sqlalchemy import Column, String, Integer, ForeignKey, Enum, DateTime, JSON, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.interface.models import RecordStatusEnum
from qcfractal.db_socket import BaseORM, MsgpackExt


class BaseResultORM(BaseORM):
    """
    Abstract Base class for ResultORMs and ProcedureORMs
    """

    __tablename__ = "base_result"

    # for SQL
    result_type = Column(String)  # for inheritance

    # Base identification
    id = Column(Integer, primary_key=True)
    # ondelete="SET NULL": when manger is deleted, set this field to None
    manager_name = Column(String, ForeignKey("compute_manager.name", ondelete="SET NULL"), nullable=True)

    hash_index = Column(String)  # TODO
    procedure = Column(String(100), nullable=False)  # TODO: may remove
    version = Column(Integer)
    protocols = Column(JSONB, nullable=False)

    # Extra fields
    extras = Column(MsgpackExt)
    stdout = Column(Integer, ForeignKey("output_store.id"))
    stdout_obj = relationship(
        OutputStoreORM, lazy="select", foreign_keys=stdout, cascade="all, delete-orphan", single_parent=True
    )

    stderr = Column(Integer, ForeignKey("output_store.id"))
    stderr_obj = relationship(
        OutputStoreORM, lazy="select", foreign_keys=stderr, cascade="all, delete-orphan", single_parent=True
    )

    error = Column(Integer, ForeignKey("output_store.id"))
    error_obj = relationship(
        OutputStoreORM, lazy="select", foreign_keys=error, cascade="all, delete-orphan", single_parent=True
    )

    # Compute status
    status = Column(Enum(RecordStatusEnum), nullable=False, default=RecordStatusEnum.waiting)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # Carry-ons
    provenance = Column(JSON)

    # Related task. The foreign key is in the task_queue table
    task_obj = relationship("TaskQueueORM", back_populates="base_result_obj", uselist=False)

    # Related service. The foreign key is in the service_queue table
    service_obj = relationship("ServiceQueueORM", back_populates="procedure_obj", uselist=False)

    __table_args__ = (
        Index("ix_base_result_status", "status"),
        Index("ix_base_result_type", "result_type"),  # todo: needed?
        Index("ix_base_result_stdout", "stdout", unique=True),
        Index("ix_base_result_stderr", "stderr", unique=True),
        Index("ix_base_result_error", "error", unique=True),
        Index("ix_base_result_hash_index", "hash_index", unique=False),
    )

    __mapper_args__ = {"polymorphic_on": "result_type"}
