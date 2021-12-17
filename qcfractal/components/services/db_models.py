import datetime

from sqlalchemy import Column, Integer, ForeignKey, JSON, String, DateTime, Index, UniqueConstraint
from sqlalchemy.orm import relationship

from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM, PlainMsgpackExt


class ServiceQueueTasksORM(BaseORM):
    __tablename__ = "service_queue_tasks"

    service_id = Column(Integer, ForeignKey("service_queue.id", ondelete="cascade"), primary_key=True)
    record_id = Column(Integer, ForeignKey(BaseRecordORM.id), primary_key=True)

    record = relationship("BaseRecordORM")
    extras = Column(JSON)


class ServiceQueueORM(BaseORM):

    __tablename__ = "service_queue"

    id = Column(Integer, primary_key=True)

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id))
    record = relationship(BaseRecordORM, back_populates="service", uselist=False)

    tag = Column(String)
    priority = Column(Integer, nullable=False)
    created_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    service_state = Column(PlainMsgpackExt)

    tasks = relationship(ServiceQueueTasksORM, lazy="selectin")

    __table_args__ = (
        UniqueConstraint("record_id", name="ux_service_queue_record_id"),
        Index("ix_service_queue_tag", "tag"),
        Index("ix_service_queue_waiting_sort", priority.desc(), created_on),
    )
