import datetime

from sqlalchemy import Column, Integer, ForeignKey, DateTime, Float, Index, String, JSON, Enum
from sqlalchemy.orm import relationship

from qcfractal.interface.models import ManagerStatusEnum, ObjectId
from qcfractal.db_socket import BaseORM

from typing import Optional, Iterable, Dict, Any


class QueueManagerLogORM(BaseORM):

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


class QueueManagerORM(BaseORM):
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

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)
        # TODO: Are passwords stored anywhere else? Other kinds of passwords?
        if "configuration" in d and isinstance(d["configuration"], dict) and "server" in d["configuration"]:
            d["configuration"]["server"].pop("password", None)

        # TODO - int id
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
