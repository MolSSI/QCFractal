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
    Enum,
    Float,
    ForeignKey,
    Index,
    CheckConstraint,
    Integer,
    String,
)

from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

from qcfractal.interface.models import ManagerStatusEnum, PriorityEnum, ObjectId
from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt, PlainMsgpackExt


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class TaskQueueORM(Base):
    """A queue of tasks corresponding to a procedure

    Notes: don't sort query results without having the index sorted
           will impact the performance
    """

    __tablename__ = "task_queue"

    id = Column(Integer, primary_key=True)

    spec = Column(MsgpackExt, nullable=False)

    # others
    tag = Column(String, default=None)
    required_programs = Column(JSONB, nullable=False)
    priority = Column(Integer, default=PriorityEnum.normal)
    manager = Column(String, ForeignKey("queue_manager.name", ondelete="SET NULL"), default=None)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)

    # can reference ResultORMs or any ProcedureORM
    base_result_id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), unique=True, nullable=False)
    base_result_obj = relationship(
        "BaseResultORM", lazy="select", innerjoin=True, back_populates="task_obj"
    )  # user inner join, since not nullable

    # An important special case is ORDER BY in combination with LIMIT n: an
    # explicit sort will have to process all the data to identify the first n
    # rows, but if there is an index matching the ORDER BY, the first n rows
    # can be retrieved directly, without scanning the remainder at all.
    __table_args__ = (
        Index("ix_task_queue_tag", "tag"),
        Index("ix_task_queue_manager", "manager"),
        Index("ix_task_queue_required_programs", "required_programs"),
        Index("ix_task_queue_base_result_id", "base_result_id"),
        Index("ix_task_queue_waiting_sort", text("priority desc, created_on")),
        # WARNING - these are not autodetected by alembic
        CheckConstraint(
            "required_programs::text = LOWER(required_programs::text)", name="ck_task_queue_requirements_lower"
        ),
    )


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


class QueueManagerLogORM(Base):

    __tablename__ = "queue_manager_logs"

    id = Column(Integer, primary_key=True)
    manager_id = Column(Integer, ForeignKey("queue_manager.id"), nullable=False)

    timestamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    completed = Column(Integer, nullable=True)
    submitted = Column(Integer, nullable=True)
    failures = Column(Integer, nullable=True)

    total_worker_walltime = Column(Float, nullable=True)
    total_task_walltime = Column(Float, nullable=True)
    active_tasks = Column(Integer, nullable=True)
    active_cores = Column(Integer, nullable=True)
    active_memory = Column(Float, nullable=True)

    __table_args__ = (Index("ix_queue_manager_log_timestamp", "timestamp"),)


class QueueManagerORM(Base):
    """ """

    __tablename__ = "queue_manager"

    id = Column(Integer, primary_key=True)

    name = Column(String, unique=True)
    cluster = Column(String)
    hostname = Column(String)
    username = Column(String)
    uuid = Column(String)
    tag = Column(String)

    # Count at current time
    completed = Column(Integer, default=0)
    submitted = Column(Integer, default=0)
    failures = Column(Integer, default=0)
    returned = Column(Integer, default=0)

    total_worker_walltime = Column(Float, nullable=True)
    total_task_walltime = Column(Float, nullable=True)
    active_tasks = Column(Integer, nullable=True)
    active_cores = Column(Integer, nullable=True)
    active_memory = Column(Float, nullable=True)

    # Adapter Information
    configuration = Column(JSON, nullable=True)

    status = Column(Enum(ManagerStatusEnum), default=ManagerStatusEnum.inactive)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    qcengine_version = Column(String)
    manager_version = Column(String)
    programs = Column(JSON)

    logs_obj = relationship(QueueManagerLogORM, lazy="select")

    __table_args__ = (Index("ix_queue_manager_status", "status"), Index("ix_queue_manager_modified_on", "modified_on"))

    def dict(self):
        d = Base.dict(self)
        # TODO: Are passwords stored anywhere else? Other kinds of passwords?
        if "configuration" in d and isinstance(d["configuration"], dict) and "server" in d["configuration"]:
            d["configuration"]["server"].pop("password", None)

        # TODO - int id
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
