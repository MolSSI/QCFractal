from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Index,
    LargeBinary,
    CheckConstraint,
    UniqueConstraint,
    Boolean,
)
from sqlalchemy.dialects.postgresql import ARRAY, TEXT, TIMESTAMP
from sqlalchemy.orm import relationship

from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM
from qcportal.utils import now_at_utc


class TaskQueueORM(BaseORM):
    """
    Table for storing information about tasks
    """

    __tablename__ = "task_queue"

    id = Column(Integer, primary_key=True)

    function = Column(String, nullable=True)
    function_kwargs_compressed = Column(LargeBinary, nullable=True)

    # For some reason, this can't be array of varchar. If it is, the comparisons
    # when claiming tasks don't work
    required_programs = Column(ARRAY(TEXT), nullable=False)

    sort_date = Column(TIMESTAMP(timezone=True), default=now_at_utc, nullable=False)
    tag = Column(String, nullable=False)
    priority = Column(Integer, nullable=False)
    available = Column(Boolean, nullable=False)

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), nullable=False)
    record = relationship(BaseRecordORM, back_populates="task", uselist=False)

    # An important special case is ORDER BY in combination with LIMIT n: an
    # explicit sort will have to process all the data to identify the first n
    # rows, but if there is an index matching the ORDER BY, the first n rows
    # can be retrieved directly, without scanning the remainder at all.
    __table_args__ = (
        Index("ix_task_queue_tag", "tag"),
        Index("ix_task_queue_required_programs", "required_programs", postgresql_using="gin"),
        Index(
            "ix_task_queue_sort", priority.desc(), sort_date.asc(), id.asc(), tag, postgresql_where=(available == True)
        ),
        UniqueConstraint("record_id", name="ux_task_queue_record_id"),
        # WARNING - these are not autodetected by alembic
        CheckConstraint(
            "required_programs::text = LOWER(required_programs::text)", name="ck_task_queue_requirements_lower"
        ),
        CheckConstraint("tag = LOWER(tag)", name="ck_task_queue_tag_lower"),
    )

    # Remove sort_date from the model. For backwards compatibility (and because it's only used for sorting)
    # Also remove the "available" column - is somewhat redundant with the record status
    _qcportal_model_excludes = ["sort_date", "available"]
