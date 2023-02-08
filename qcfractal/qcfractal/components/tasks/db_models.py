import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Index,
    CheckConstraint,
    UniqueConstraint,
    DDL,
    event,
)
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from sqlalchemy.orm import relationship

from qcfractal.components.largebinary.db_models import LargeBinaryORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM


class TaskQueueORM(BaseORM):
    """
    Table for storing information about tasks
    """

    __tablename__ = "task_queue"

    id = Column(Integer, primary_key=True)

    function = Column(String, nullable=True)
    function_kwargs_lb_id = Column(Integer, ForeignKey(LargeBinaryORM.id), nullable=True)

    # For some reason, this can't be array of varchar. If it is, the comparisons
    # when claiming tasks don't work
    required_programs = Column(ARRAY(TEXT), nullable=False)

    tag = Column(String, nullable=False)
    priority = Column(Integer, nullable=False)

    created_on = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), nullable=False)
    record = relationship(BaseRecordORM, back_populates="task", uselist=False)

    # An important special case is ORDER BY in combination with LIMIT n: an
    # explicit sort will have to process all the data to identify the first n
    # rows, but if there is an index matching the ORDER BY, the first n rows
    # can be retrieved directly, without scanning the remainder at all.
    __table_args__ = (
        Index("ix_task_queue_tag", "tag"),
        Index("ix_task_queue_required_programs", "required_programs"),
        Index("ix_task_queue_waiting_sort", priority.desc(), created_on),
        Index("ix_task_queue_function_kwargs_lb_id", "function_kwargs_lb_id"),
        UniqueConstraint("record_id", name="ux_task_queue_record_id"),
        # WARNING - these are not autodetected by alembic
        CheckConstraint(
            "required_programs::text = LOWER(required_programs::text)", name="ck_task_queue_requirements_lower"
        ),
        CheckConstraint("tag = LOWER(tag)", name="ck_task_queue_tag_lower"),
    )


# Trigger for deleting largebinary_store rows when rows of task_queue are deleted
_del_lb_trigger = DDL(
    """
    CREATE FUNCTION qca_task_queue_delete_lb() RETURNS TRIGGER AS
    $_$
    BEGIN
      DELETE FROM largebinary_store WHERE largebinary_store.id = OLD.function_kwargs_lb_id;
      RETURN OLD;
    END
    $_$ LANGUAGE 'plpgsql';

    CREATE TRIGGER qca_task_queue_delete_lb_tr
    AFTER DELETE ON task_queue
    FOR EACH ROW EXECUTE PROCEDURE qca_task_queue_delete_lb();
    """
)

event.listen(TaskQueueORM.__table__, "after_create", _del_lb_trigger.execute_if(dialect=("postgresql")))
