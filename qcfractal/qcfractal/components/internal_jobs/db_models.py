from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, TIMESTAMP, String, Index, Enum, UniqueConstraint, ForeignKey
from sqlalchemy import DDL, event
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from qcfractal.components.auth.db_models import UserIDMapSubquery, UserORM
from qcfractal.db_socket import BaseORM
from qcportal.internal_jobs.models import InternalJobStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from typing import Optional, Iterable, Dict, Any


class InternalJobORM(BaseORM):
    __tablename__ = "internal_jobs"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    status = Column(Enum(InternalJobStatusEnum), nullable=False)
    added_date = Column(TIMESTAMP(timezone=True), nullable=False, default=now_at_utc)
    scheduled_date = Column(TIMESTAMP(timezone=True), nullable=False)
    started_date = Column(TIMESTAMP(timezone=True))
    last_updated = Column(TIMESTAMP(timezone=True))
    ended_date = Column(TIMESTAMP(timezone=True))
    runner_hostname = Column(String)
    runner_uuid = Column(String)

    progress = Column(Integer, nullable=False, default=0)
    progress_description = Column(String, nullable=True)

    function = Column(String, nullable=False)
    kwargs = Column(JSON, nullable=False)

    after_function = Column(String, nullable=True)
    after_function_kwargs = Column(JSON(none_as_null=True), nullable=True)

    repeat_delay = Column(Integer, nullable=True)

    result = Column(JSON)
    user_id = Column(Integer, ForeignKey(UserORM.id, ondelete="cascade"), nullable=True)

    user = relationship(
        UserIDMapSubquery,
        foreign_keys=[user_id],
        primaryjoin="InternalJobORM.user_id == UserIDMapSubquery.id",
        lazy="selectin",
        viewonly=True,
    )

    # Nullable column with unique constraint. If a unique_name is specified,
    # it must be unique. null != null always
    unique_name = Column(String, nullable=True)

    # If this job is part of a serial group (only one may run at a time)
    serial_group = Column(String, nullable=True)

    __table_args__ = (
        Index("ix_internal_jobs_added_date", "added_date", postgresql_using="brin"),
        Index("ix_internal_jobs_scheduled_date", "scheduled_date", postgresql_using="brin"),
        Index("ix_internal_jobs_last_updated", "last_updated", postgresql_using="brin"),
        Index("ix_internal_jobs_status", "status"),
        Index("ix_internal_jobs_name", "name"),
        Index("ix_internal_jobs_user_id", "user_id"),
        UniqueConstraint("unique_name", name="ux_internal_jobs_unique_name"),
        # Enforces only one running per serial group
        Index(
            "ux_internal_jobs_status_serial_group",
            "status",
            "serial_group",
            unique=True,
            postgresql_where=(status == InternalJobStatusEnum.running),
        ),
    )

    _qcportal_model_excludes = ["unique_name", "user_id"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)
        d["user"] = self.user.username if self.user is not None else None
        return d


# Function that sends a postgres NOTIFY to internal job workers
# (always do notify, even if the job is in the future. The worker can calculate
# the time difference and sleep until the job is ready)
_insert_internal_job_triggerfunc = DDL(
    """
    CREATE OR REPLACE FUNCTION public.qca_internal_jobs_notify()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
        BEGIN
          PERFORM pg_notify('check_internal_jobs', '');
          RETURN NEW;
        END
        $_$
    ;
"""
)

# Trigger the above function whenever a new internal job is added
_insert_internal_job_trigger = DDL(
    """
    CREATE TRIGGER qca_internal_jobs_insert_tr
    AFTER INSERT ON internal_jobs
    FOR EACH ROW EXECUTE PROCEDURE qca_internal_jobs_notify();
    """
)

event.listen(
    InternalJobORM.__table__, "after_create", _insert_internal_job_triggerfunc.execute_if(dialect=("postgresql"))
)
event.listen(InternalJobORM.__table__, "after_create", _insert_internal_job_trigger.execute_if(dialect=("postgresql")))
