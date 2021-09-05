"""
Basic ORM models of the QCFractal database

Note: avoid circular import here by including the name of the class
in relations and foreign keys are a string (see TaskQueueORM.base_result_obj)
"""

import datetime

# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
)

from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

from qcfractal.interface.models import PriorityEnum
from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt, PlainMsgpackExt


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ServiceQueueTasks(Base):
    __tablename__ = "service_queue_tasks"

    service_id = Column(Integer, ForeignKey("service_queue.id", ondelete="cascade"), primary_key=True)
    procedure_id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), primary_key=True)

    procedure_obj = relationship("BaseResultORM", lazy="selectin")
    extras = Column(JSON)


class ServiceQueueORM(Base):

    __tablename__ = "service_queue"

    id = Column(Integer, primary_key=True)
    tag = Column(String, default=None)

    procedure_id = Column(Integer, ForeignKey("base_result.id"), unique=True)
    procedure_obj = relationship(
        "BaseResultORM", lazy="select", innerjoin=True, back_populates="service_obj"
    )  # user inner join, since not nullable

    priority = Column(Integer, default=int(PriorityEnum.normal))
    created_on = Column(DateTime, default=datetime.datetime.utcnow)

    service_state = Column(PlainMsgpackExt)

    tasks_obj = relationship(ServiceQueueTasks, lazy="joined", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_service_queue_tag", "tag"),
        Index("ix_service_queue_waiting_sort", text("priority desc, created_on")),
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
