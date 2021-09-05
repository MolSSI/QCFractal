import datetime

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index, text, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.interface.models import PriorityEnum
from qcfractal.db_socket import BaseORM, MsgpackExt


class TaskQueueORM(BaseORM):
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
        Index("ix_task_queue_waiting_sort", priority.desc(), created_on),
        # WARNING - these are not autodetected by alembic
        CheckConstraint(
            "required_programs::text = LOWER(required_programs::text)", name="ck_task_queue_requirements_lower"
        ),
    )
